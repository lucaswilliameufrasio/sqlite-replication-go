package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/spool"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storages3"
)

type S3GCPlan struct {
	Prefix              string
	Plan                GCPlan
	RetainedObjectKeys  []string
	CandidateObjectKeys []string
}

type S3SyncResult struct {
	TXID             uint64
	DatabaseChecksum uint64
	ObjectsConfirmed int
	BytesConfirmed   int64
}

// SyncFilesystemToS3 validates and uploads the immutable local LTX chain, then
// persists a destination-scoped acknowledgement. It never deletes local spool
// data or remote objects.
func SyncFilesystemToS3(ctx context.Context, root string, store *storages3.Store, prefix, remoteID string) (S3SyncResult, error) {
	if store == nil {
		return S3SyncResult{}, fmt.Errorf("S3 store is required")
	}
	if err := validateS3Prefix(prefix); err != nil {
		return S3SyncResult{}, err
	}
	if strings.TrimSpace(remoteID) == "" {
		return S3SyncResult{}, fmt.Errorf("a stable, non-secret remote ID is required")
	}
	for _, character := range remoteID {
		if unicode.IsControl(character) {
			return S3SyncResult{}, fmt.Errorf("remote ID must not contain control characters")
		}
	}
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		return S3SyncResult{}, fmt.Errorf("lock local repository for S3 sync: %w", err)
	}
	defer lock.Close()
	state, err := spool.Recover(root, nil)
	if err != nil {
		return S3SyncResult{}, fmt.Errorf("validate local LTX chain: %w", err)
	}
	if _, err := BuildRestorePlanFromFilesystem(root, Target{TXID: state.Database.TXID}); err != nil {
		return S3SyncResult{}, fmt.Errorf("validate local restore chain: %w", err)
	}

	manifestPath := filepath.Join(root, "_meta", "repository.json")
	manifestObject := struct {
		path string
		key  string
	}{path: manifestPath, key: joinS3Key(prefix, "_meta/repository.json")}
	objects := make([]struct {
		path string
		key  string
	}, 0)
	ltxRoot := filepath.Join(root, "ltx")
	err = filepath.WalkDir(ltxRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("local LTX object is not a regular file")
		}
		if filepath.Ext(path) != ".ltx" {
			return nil
		}
		key, err := filesystemLTXKey(prefix, root, path)
		if err != nil {
			return err
		}
		objects = append(objects, struct {
			path string
			key  string
		}{path: path, key: key})
		return nil
	})
	if err != nil {
		return S3SyncResult{}, fmt.Errorf("enumerate local LTX objects: %w", err)
	}
	if len(objects) == 0 {
		return S3SyncResult{}, ErrNoRestorePoint
	}
	sort.Slice(objects, func(left, right int) bool {
		return objects[left].key < objects[right].key
	})
	objects = append(objects, manifestObject)

	result := S3SyncResult{TXID: state.Database.TXID, DatabaseChecksum: state.Database.Checksum}
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return S3SyncResult{}, err
		}
		file, err := os.Open(object.path)
		if err != nil {
			return S3SyncResult{}, fmt.Errorf("open local object for upload: %w", err)
		}
		receipt, putErr := store.PutImmutable(ctx, object.key, file)
		closeErr := file.Close()
		if putErr != nil {
			return S3SyncResult{}, fmt.Errorf("upload immutable repository object: %w", putErr)
		}
		if closeErr != nil {
			return S3SyncResult{}, fmt.Errorf("close uploaded repository object: %w", closeErr)
		}
		result.ObjectsConfirmed++
		result.BytesConfirmed += receipt.Size
	}
	if err := spool.WriteRemoteAck(root, spool.RemoteAck{
		Version:          1,
		RemoteID:         remoteID,
		Prefix:           prefix,
		TXID:             result.TXID,
		DatabaseChecksum: result.DatabaseChecksum,
	}); err != nil {
		return S3SyncResult{}, fmt.Errorf("persist remote acknowledgement: %w", err)
	}
	return result, nil
}

// RestoreS3ToTarget stages the remote repository into a private temporary
// filesystem repository, then uses the same verified restore path as local FS.
// It remains internal until CP-01 permits public incremental recovery.
func RestoreS3ToTarget(ctx context.Context, store *storages3.Store, prefix, destination string, target Target) error {
	return withS3Repository(ctx, store, prefix, func(root string) error {
		return RestoreToTarget(ctx, root, destination, target)
	})
}

// CompactS3ToSnapshot publishes a verified higher-level snapshot using the S3
// backend layout. Existing source objects are retained.
func CompactS3ToSnapshot(ctx context.Context, store *storages3.Store, prefix string, target Target) (string, error) {
	var result string
	err := withS3Repository(ctx, store, prefix, func(root string) error {
		localPath, err := CompactToSnapshot(root, target)
		if err != nil {
			return err
		}
		key, err := filesystemLTXKey(prefix, root, localPath)
		if err != nil {
			return err
		}
		file, err := os.Open(localPath)
		if err != nil {
			return err
		}
		defer file.Close()
		receipt, err := store.PutImmutable(ctx, key, file)
		if err != nil {
			return fmt.Errorf("publish compacted S3 snapshot: %w", err)
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if err := verifyRemoteObject(ctx, store, key, file, receipt.Size, receipt.SHA256); err != nil {
			return err
		}
		result = key
		return nil
	})
	return result, err
}

// BuildS3GCPlan creates a dry-run plan for explicit retained restore points.
func BuildS3GCPlan(ctx context.Context, store *storages3.Store, prefix string, requiredTargets []Target) (S3GCPlan, error) {
	if err := validateS3Prefix(prefix); err != nil {
		return S3GCPlan{}, err
	}
	var result S3GCPlan
	err := withS3Repository(ctx, store, prefix, func(root string) error {
		plan, err := BuildGCPlan(root, requiredTargets)
		if err != nil {
			return err
		}
		result = S3GCPlan{Prefix: prefix, Plan: plan}
		for _, key := range plan.RetainedKeys {
			remoteKey, err := localKeyToS3Key(prefix, key)
			if err != nil {
				return err
			}
			result.RetainedObjectKeys = append(result.RetainedObjectKeys, remoteKey)
		}
		for _, candidate := range plan.Candidates {
			remoteKey, err := localKeyToS3Key(prefix, candidate.Key)
			if err != nil {
				return err
			}
			result.CandidateObjectKeys = append(result.CandidateObjectKeys, remoteKey)
		}
		return nil
	})
	return result, err
}

// ApplyS3GCPlan re-downloads and revalidates the full inventory and repository
// identity before issuing explicit batched deletes.
func ApplyS3GCPlan(ctx context.Context, store *storages3.Store, prefix string, reviewed S3GCPlan) error {
	if prefix != reviewed.Prefix {
		return ErrStaleGCPlan
	}
	current, err := BuildS3GCPlan(ctx, store, prefix, reviewed.Plan.RequiredTargets)
	if err != nil {
		return err
	}
	if current.Plan.InventorySHA256 != reviewed.Plan.InventorySHA256 ||
		!sameTargets(current.Plan.RequiredTargets, reviewed.Plan.RequiredTargets) ||
		!sameGCObjects(current.Plan.Candidates, reviewed.Plan.Candidates) ||
		!sameStrings(current.Plan.RetainedKeys, reviewed.Plan.RetainedKeys) ||
		!sameStrings(current.CandidateObjectKeys, reviewed.CandidateObjectKeys) ||
		!sameStrings(current.RetainedObjectKeys, reviewed.RetainedObjectKeys) {
		return ErrStaleGCPlan
	}
	for start := 0; start < len(current.CandidateObjectKeys); start += 1000 {
		end := start + 1000
		if end > len(current.CandidateObjectKeys) {
			end = len(current.CandidateObjectKeys)
		}
		if err := store.DeleteBatch(ctx, current.CandidateObjectKeys[start:end]); err != nil {
			return fmt.Errorf("collect unreferenced S3 LTX objects: %w", err)
		}
	}
	return nil
}

func withS3Repository(ctx context.Context, store *storages3.Store, prefix string, fn func(string) error) error {
	if store == nil {
		return fmt.Errorf("S3 store is required")
	}
	if err := validateS3Prefix(prefix); err != nil {
		return err
	}
	directory, err := os.MkdirTemp("", "sqlite-replication-s3-recovery-*")
	if err != nil {
		return fmt.Errorf("create S3 recovery staging directory: %w", err)
	}
	defer os.RemoveAll(directory)
	if err := downloadS3Repository(ctx, store, prefix, directory); err != nil {
		return err
	}
	return fn(directory)
}

func downloadS3Repository(ctx context.Context, store *storages3.Store, prefix, root string) error {
	manifestKey := joinS3Key(prefix, "_meta/repository.json")
	if err := downloadS3Object(ctx, store, manifestKey, filepath.Join(root, "_meta", "repository.json")); err != nil {
		return fmt.Errorf("download S3 repository metadata: %w", err)
	}
	if _, err := storagefs.ReadRepository(root); err != nil {
		return fmt.Errorf("validate S3 repository metadata: %w", err)
	}
	prefixKey := ""
	if prefix != "" {
		prefixKey = prefix + "/"
	}
	var cursor *string
	var objectCount int
	for {
		page, err := store.ListPage(ctx, prefixKey, cursor, 1000)
		if err != nil {
			return fmt.Errorf("list S3 repository objects: %w", err)
		}
		for _, object := range page.Contents {
			if object.Key == nil {
				return fmt.Errorf("S3 list returned an object without a key")
			}
			key := *object.Key
			if key == manifestKey || !strings.HasSuffix(key, ".ltx") {
				continue
			}
			localKey, err := s3KeyToLocalKey(prefix, key)
			if err != nil {
				return err
			}
			destination := filepath.Join(root, filepath.FromSlash(localKey))
			if err := downloadS3Object(ctx, store, key, destination); err != nil {
				return fmt.Errorf("download S3 LTX object %q: %w", key, err)
			}
			objectCount++
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			break
		}
		if page.NextContinuationToken == nil || cursor != nil && *page.NextContinuationToken == *cursor {
			return fmt.Errorf("S3 repository listing returned a non-advancing cursor")
		}
		next := *page.NextContinuationToken
		cursor = &next
	}
	if objectCount == 0 {
		return ErrNoRestorePoint
	}
	return nil
}

func downloadS3Object(ctx context.Context, store *storages3.Store, key, destination string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	output, err := store.Get(ctx, key, "")
	if err != nil {
		return err
	}
	defer output.Body.Close()
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return err
	}
	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, contextReader{ctx: ctx, reader: output.Body})
	syncErr := file.Sync()
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(destination)
		return copyErr
	}
	if syncErr != nil {
		_ = os.Remove(destination)
		return syncErr
	}
	if closeErr != nil {
		_ = os.Remove(destination)
		return closeErr
	}
	return nil
}

func verifyRemoteObject(ctx context.Context, store *storages3.Store, key string, expected io.Reader, expectedSize int64, expectedSHA256 string) error {
	output, err := store.Get(ctx, key, "")
	if err != nil {
		return fmt.Errorf("read back compacted S3 snapshot: %w", err)
	}
	defer output.Body.Close()
	hasher := sha256.New()
	size, err := io.Copy(hasher, contextReader{ctx: ctx, reader: output.Body})
	if err != nil {
		return fmt.Errorf("verify compacted S3 snapshot bytes: %w", err)
	}
	gotSHA256 := hex.EncodeToString(hasher.Sum(nil))
	if size != expectedSize || gotSHA256 != expectedSHA256 {
		return fmt.Errorf("compacted S3 snapshot differs after publication")
	}
	localHash := sha256.New()
	localSize, err := io.Copy(localHash, expected)
	if err != nil {
		return err
	}
	if localSize != size || !equalHash(localHash, hasher) {
		return fmt.Errorf("local compacted snapshot differs from published S3 object")
	}
	return nil
}

func equalHash(left, right hash.Hash) bool {
	return strings.EqualFold(hex.EncodeToString(left.Sum(nil)), hex.EncodeToString(right.Sum(nil)))
}

func filesystemLTXKey(prefix, root, path string) (string, error) {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}
	return localKeyToS3Key(prefix, filepath.ToSlash(relative))
}

func localKeyToS3Key(prefix, localKey string) (string, error) {
	parts := strings.Split(localKey, "/")
	if len(parts) != 3 || parts[0] != "ltx" || !strings.HasSuffix(parts[2], ".ltx") {
		return "", fmt.Errorf("invalid local LTX object key %q", localKey)
	}
	level, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return "", fmt.Errorf("invalid local LTX level %q", parts[1])
	}
	if _, _, err := parseFilename(parts[2]); err != nil {
		return "", err
	}
	return joinS3Key(prefix, fmt.Sprintf("%04x/%s", level, parts[2])), nil
}

func s3KeyToLocalKey(prefix, key string) (string, error) {
	if prefix != "" {
		if !strings.HasPrefix(key, prefix+"/") {
			return "", fmt.Errorf("S3 LTX object %q is outside the repository prefix", key)
		}
		key = strings.TrimPrefix(key, prefix+"/")
	}
	parts := strings.Split(key, "/")
	if len(parts) != 2 || len(parts[0]) != 4 {
		return "", fmt.Errorf("invalid S3 LTX object key")
	}
	level, err := strconv.ParseUint(parts[0], 16, 16)
	if err != nil {
		return "", fmt.Errorf("invalid S3 LTX level")
	}
	if _, _, err := parseFilename(parts[1]); err != nil {
		return "", err
	}
	return filepath.ToSlash(filepath.Join("ltx", strconv.FormatUint(level, 10), parts[1])), nil
}

func joinS3Key(prefix, suffix string) string {
	if prefix == "" {
		return suffix
	}
	return prefix + "/" + suffix
}

func validateS3Prefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	if strings.HasPrefix(prefix, "/") || strings.HasSuffix(prefix, "/") || strings.ContainsRune(prefix, '\x00') {
		return fmt.Errorf("invalid S3 repository prefix")
	}
	for _, part := range strings.Split(prefix, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid S3 repository prefix")
		}
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}
