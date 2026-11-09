package recovery

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/repositorylock"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
)

var ErrStaleGCPlan = errors.New("garbage-collection plan is stale")

type GCObject struct {
	Key  string
	Size int64
}

// GCPlan is a read-only proposal. BuildGCPlan never deletes objects.
type GCPlan struct {
	RequiredTargets []Target
	RetainedKeys    []string
	Candidates      []GCObject
	InventorySHA256 [32]byte
}

// BuildGCPlan plans collection against explicit promised restore points.
// An empty target set is rejected rather than interpreted as a default policy.
func BuildGCPlan(root string, requiredTargets []Target) (GCPlan, error) {
	if len(requiredTargets) == 0 {
		return GCPlan{}, fmt.Errorf("at least one explicit retained restore point is required")
	}
	repository, err := storagefs.ReadRepository(root)
	if err != nil {
		return GCPlan{}, fmt.Errorf("read repository identity for GC: %w", err)
	}
	retained := make(map[string]struct{})
	for _, target := range requiredTargets {
		plan, err := BuildRestorePlanFromFilesystem(root, target)
		if err != nil {
			return GCPlan{}, fmt.Errorf("plan retained restore point: %w", err)
		}
		for _, segment := range plan.Segments {
			retained[segment.Key] = struct{}{}
		}
	}
	objects, inventoryFingerprint, err := listGCObjects(root)
	if err != nil {
		return GCPlan{}, err
	}
	// Capture reconstructs from the level-zero base and every level-zero delta.
	// A restore-only plan must not collect that still-required writer chain.
	for _, object := range objects {
		if strings.HasPrefix(object.Key, "ltx/0/") {
			retained[object.Key] = struct{}{}
		}
	}
	fingerprint := sha256.Sum256([]byte(repository.RepositoryID + "\x00" + repository.DatabaseID + "\x00" + fmt.Sprintf("%x", inventoryFingerprint)))
	plan := GCPlan{
		RequiredTargets: cloneTargets(requiredTargets),
		InventorySHA256: fingerprint,
	}
	for key := range retained {
		plan.RetainedKeys = append(plan.RetainedKeys, key)
	}
	sort.Strings(plan.RetainedKeys)
	for _, object := range objects {
		if _, keep := retained[object.Key]; !keep {
			plan.Candidates = append(plan.Candidates, object)
		}
	}
	return plan, nil
}

func cloneTargets(targets []Target) []Target {
	cloned := make([]Target, len(targets))
	copy(cloned, targets)
	for index := range cloned {
		if targets[index].TimestampMillis != nil {
			timestamp := *targets[index].TimestampMillis
			cloned[index].TimestampMillis = &timestamp
		}
	}
	return cloned
}

// ApplyGCPlan explicitly applies a previously reviewed plan only after
// rebuilding and comparing the complete immutable-object inventory.
func ApplyGCPlan(root string, reviewed GCPlan) error {
	return applyGCPlanLocked(root, reviewed, nil)
}

func applyGCPlanLocked(root string, reviewed GCPlan, afterValidation func() error) error {
	lock, err := repositorylock.AcquireExclusive(root)
	if err != nil {
		return fmt.Errorf("lock repository for GC: %w", err)
	}
	defer lock.Close()
	return applyGCPlan(root, reviewed, afterValidation)
}

// afterValidation is an internal test seam for exercising concurrent mutations.
func applyGCPlan(root string, reviewed GCPlan, afterValidation func() error) error {
	if len(reviewed.RequiredTargets) == 0 {
		return fmt.Errorf("GC plan has no explicit retained restore points")
	}
	current, err := BuildGCPlan(root, reviewed.RequiredTargets)
	if err != nil {
		return err
	}
	if current.InventorySHA256 != reviewed.InventorySHA256 || !sameTargets(current.RequiredTargets, reviewed.RequiredTargets) || !sameGCObjects(current.Candidates, reviewed.Candidates) || !sameStrings(current.RetainedKeys, reviewed.RetainedKeys) {
		return ErrStaleGCPlan
	}
	if afterValidation != nil {
		if err := afterValidation(); err != nil {
			return err
		}
	}
	for _, candidate := range current.Candidates {
		path := filepath.Join(root, filepath.FromSlash(candidate.Key))
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove unreferenced LTX object %q: %w", candidate.Key, err)
		}
		if err := storagefs.SyncDirectory(filepath.Dir(path)); err != nil {
			return fmt.Errorf("sync LTX level after collecting %q: %w", candidate.Key, err)
		}
	}
	return nil
}

func sameTargets(left, right []Target) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].TXID != right[index].TXID {
			return false
		}
		if (left[index].TimestampMillis == nil) != (right[index].TimestampMillis == nil) {
			return false
		}
		if left[index].TimestampMillis != nil && *left[index].TimestampMillis != *right[index].TimestampMillis {
			return false
		}
	}
	return true
}

func listGCObjects(root string) ([]GCObject, [32]byte, error) {
	levels, err := os.ReadDir(filepath.Join(root, "ltx"))
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("list LTX levels for GC: %w", err)
	}
	var inventory strings.Builder
	var objects []GCObject
	for _, level := range levels {
		if !level.IsDir() {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(root, "ltx", level.Name()))
		if err != nil {
			return nil, [32]byte{}, fmt.Errorf("list LTX level %q for GC: %w", level.Name(), err)
		}
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".ltx" {
				continue
			}
			if !entry.Type().IsRegular() {
				return nil, [32]byte{}, fmt.Errorf("LTX object %q is not a regular file", entry.Name())
			}
			key := filepath.ToSlash(filepath.Join("ltx", level.Name(), entry.Name()))
			path := filepath.Join(root, filepath.FromSlash(key))
			file, err := os.Open(path)
			if err != nil {
				return nil, [32]byte{}, fmt.Errorf("open LTX object %q for GC fingerprint: %w", key, err)
			}
			hash := sha256.New()
			size, copyErr := io.Copy(hash, file)
			closeErr := file.Close()
			if copyErr != nil {
				return nil, [32]byte{}, copyErr
			}
			if closeErr != nil {
				return nil, [32]byte{}, closeErr
			}
			objects = append(objects, GCObject{Key: key, Size: size})
			fmt.Fprintf(&inventory, "%s\x00%d\x00%x\n", key, size, hash.Sum(nil))
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, sha256.Sum256([]byte(inventory.String())), nil
}

func sameGCObjects(left, right []GCObject) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
