package storagefs

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const FormatVersion = 2

type Repository struct {
	SchemaVersion int    `json:"schema_version"`
	FormatVersion int    `json:"format_version"`
	RepositoryID  string `json:"repository_id"`
	DatabaseID    string `json:"database_id"`
}

func LTXPath(root string, minTXID, maxTXID uint64) string {
	name := fmt.Sprintf("%016x-%016x.ltx", minTXID, maxTXID)
	return filepath.Join(root, "ltx", "0", name)
}

func EnsureRepository(root string) error {
	metaDir := filepath.Join(root, "_meta")
	if err := os.MkdirAll(metaDir, 0o750); err != nil {
		return fmt.Errorf("create repository metadata directory: %w", err)
	}
	manifestPath := filepath.Join(metaDir, "repository.json")
	if _, err := os.Stat(manifestPath); err == nil {
		_, err := ReadRepository(root)
		if err != nil {
			return err
		}
		return SyncDirectory(metaDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect repository metadata: %w", err)
	}
	repositoryID, err := newUUID()
	if err != nil {
		return err
	}
	databaseID, err := newUUID()
	if err != nil {
		return err
	}
	manifest := Repository{SchemaVersion: 1, FormatVersion: FormatVersion, RepositoryID: repositoryID, DatabaseID: databaseID}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode repository metadata: %w", err)
	}
	data = append(data, '\n')
	temp, err := os.CreateTemp(metaDir, ".repository-*.tmp")
	if err != nil {
		return fmt.Errorf("create repository metadata staging file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write repository metadata: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync repository metadata: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close repository metadata: %w", err)
	}
	if err := Publish(tempPath, manifestPath); err != nil {
		if _, statErr := os.Stat(manifestPath); statErr == nil {
			_, readErr := ReadRepository(root)
			return readErr
		}
		return err
	}
	return SyncDirectory(metaDir)
}

func ReadRepository(root string) (Repository, error) {
	path := filepath.Join(root, "_meta", "repository.json")
	file, err := os.Open(path)
	if err != nil {
		return Repository{}, fmt.Errorf("open repository metadata: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var repository Repository
	if err := decoder.Decode(&repository); err != nil {
		return Repository{}, fmt.Errorf("decode repository metadata: %w", err)
	}
	if repository.SchemaVersion != 1 || repository.FormatVersion != FormatVersion || !validUUID(repository.RepositoryID) || !validUUID(repository.DatabaseID) {
		return Repository{}, fmt.Errorf("repository metadata is incompatible with LTX v2")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Repository{}, fmt.Errorf("repository metadata contains trailing JSON")
	}
	return repository, nil
}

// Publish atomically installs a staged file without overwriting an existing key.
// Existing content is accepted only when every byte is identical.
func Publish(stagedPath, finalPath string) error {
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o750); err != nil {
		return fmt.Errorf("create storage directory: %w", err)
	}
	if err := os.Link(stagedPath, finalPath); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("publish immutable file: %w", err)
		}
		equal, compareErr := sameFiles(stagedPath, finalPath)
		if compareErr != nil {
			return compareErr
		}
		if !equal {
			return fmt.Errorf("immutable key already exists with different content: %s", finalPath)
		}
	}
	if err := SyncDirectory(filepath.Dir(finalPath)); err != nil {
		return err
	}
	if err := os.Remove(stagedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove published staging file: %w", err)
	}
	return SyncDirectory(filepath.Dir(finalPath))
}

func SyncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}

func newUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate repository identity: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(raw[:])
	return strings.Join([]string{encoded[:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:]}, "-"), nil
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	compact := strings.ReplaceAll(value, "-", "")
	decoded, err := hex.DecodeString(compact)
	return err == nil && len(decoded) == 16 && decoded[6]>>4 == 4 && decoded[8]&0xc0 == 0x80
}

func sameFiles(leftPath, rightPath string) (bool, error) {
	left, err := os.Open(leftPath)
	if err != nil {
		return false, fmt.Errorf("open staged immutable file: %w", err)
	}
	defer left.Close()
	right, err := os.Open(rightPath)
	if err != nil {
		return false, fmt.Errorf("open existing immutable file: %w", err)
	}
	defer right.Close()
	leftInfo, err := left.Stat()
	if err != nil {
		return false, err
	}
	rightInfo, err := right.Stat()
	if err != nil {
		return false, err
	}
	if leftInfo.Size() != rightInfo.Size() {
		return false, nil
	}
	leftBuf := make([]byte, 64*1024)
	rightBuf := make([]byte, len(leftBuf))
	for {
		leftN, leftErr := io.ReadFull(left, leftBuf)
		rightN, rightErr := io.ReadFull(right, rightBuf)
		if leftN != rightN || !bytes.Equal(leftBuf[:leftN], rightBuf[:rightN]) {
			return false, nil
		}
		if errors.Is(leftErr, io.EOF) || errors.Is(leftErr, io.ErrUnexpectedEOF) {
			return errors.Is(rightErr, io.EOF) || errors.Is(rightErr, io.ErrUnexpectedEOF), nil
		}
		if leftErr != nil {
			return false, leftErr
		}
		if rightErr != nil {
			return false, rightErr
		}
	}
}
