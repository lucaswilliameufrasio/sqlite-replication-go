package recovery

import (
	"fmt"
)

// SnapshotPolicy requires an explicit cadence; no periodic-snapshot default is
// implied. EveryTXIDs counts TXID distance from the last full snapshot.
type SnapshotPolicy struct {
	EveryTXIDs uint64
}

func ShouldCompact(lastSnapshotTXID, currentTXID uint64, policy SnapshotPolicy) (bool, error) {
	if policy.EveryTXIDs == 0 {
		return false, fmt.Errorf("snapshot cadence must be positive")
	}
	if currentTXID < lastSnapshotTXID {
		return false, fmt.Errorf("current TXID precedes last snapshot TXID")
	}
	return currentTXID-lastSnapshotTXID >= policy.EveryTXIDs, nil
}

// CompactIfDue evaluates an explicit cadence and publishes a full snapshot
// when due. It does not create a scheduler or alter the retention policy.
func CompactIfDue(root string, lastSnapshotTXID, currentTXID uint64, policy SnapshotPolicy) (string, bool, error) {
	due, err := ShouldCompact(lastSnapshotTXID, currentTXID, policy)
	if err != nil || !due {
		return "", false, err
	}
	path, err := CompactToSnapshot(root, Target{TXID: currentTXID})
	if err != nil {
		return "", false, err
	}
	return path, true, nil
}
