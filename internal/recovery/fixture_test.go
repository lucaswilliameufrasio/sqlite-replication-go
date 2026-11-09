package recovery

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/storagefs"
	_ "modernc.org/sqlite"
)

func TestPinnedReferenceRecoveryChainFixtures(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve recovery fixture test source path")
	}
	fixtureRoot := filepath.Join(filepath.Dir(sourceFile), "../../conformance/fixtures/ltx-v2/recovery-chain-4096")
	chainRepository := filepath.Join(t.TempDir(), "chain")
	if err := storagefs.EnsureRepository(chainRepository); err != nil {
		t.Fatalf("initialize fixture chain repository: %v", err)
	}
	for _, fixture := range []struct {
		name    string
		minTXID uint64
		maxTXID uint64
	}{
		{name: "snapshot-1.ltx", minTXID: 1, maxTXID: 1},
		{name: "delta-2.ltx", minTXID: 2, maxTXID: 2},
		{name: "delta-3.ltx", minTXID: 3, maxTXID: 3},
	} {
		destination := storagefs.LTXPath(chainRepository, fixture.minTXID, fixture.maxTXID)
		copyRecoveryFixture(t, filepath.Join(fixtureRoot, fixture.name), destination)
	}
	chainRestore := filepath.Join(t.TempDir(), "chain.sqlite")
	if err := RestoreToTarget(context.Background(), chainRepository, chainRestore, Target{TXID: 3}); err != nil {
		t.Fatalf("restore pinned reference snapshot + delta chain: %v", err)
	}
	assertReferenceChainState(t, chainRestore, "updated-final", 32, false)

	compactedRepository := filepath.Join(t.TempDir(), "compacted")
	if err := storagefs.EnsureRepository(compactedRepository); err != nil {
		t.Fatalf("initialize compacted fixture repository: %v", err)
	}
	compactedPath := filepath.Join(compactedRepository, "ltx", "1", fmt.Sprintf("%016x-%016x.ltx", 1, 2))
	copyRecoveryFixture(t, filepath.Join(fixtureRoot, "compacted-2.ltx"), compactedPath)
	compactedRestore := filepath.Join(t.TempDir(), "compacted.sqlite")
	if err := RestoreToTarget(context.Background(), compactedRepository, compactedRestore, Target{TXID: 2}); err != nil {
		t.Fatalf("restore pinned reference compacted snapshot: %v", err)
	}
	assertReferenceChainState(t, compactedRestore, "updated-middle", 33, true)

	largeFixtureRoot := filepath.Join(filepath.Dir(sourceFile), "../../conformance/fixtures/ltx-v2/large-transaction-4096")
	largeRepository := filepath.Join(t.TempDir(), "large-transaction")
	if err := storagefs.EnsureRepository(largeRepository); err != nil {
		t.Fatalf("initialize large transaction fixture repository: %v", err)
	}
	for _, fixture := range []struct {
		name    string
		minTXID uint64
		maxTXID uint64
	}{
		{name: "snapshot-1.ltx", minTXID: 1, maxTXID: 1},
		{name: "large-delta-2.ltx", minTXID: 2, maxTXID: 2},
	} {
		copyRecoveryFixture(t, filepath.Join(largeFixtureRoot, fixture.name), storagefs.LTXPath(largeRepository, fixture.minTXID, fixture.maxTXID))
	}
	largeRestore := filepath.Join(t.TempDir(), "large-transaction.sqlite")
	if err := RestoreToTarget(context.Background(), largeRepository, largeRestore, Target{TXID: 2}); err != nil {
		t.Fatalf("restore pinned reference large transaction: %v", err)
	}
	assertLargeReferenceTransactionState(t, largeRestore)

	longFixtureRoot := filepath.Join(filepath.Dir(sourceFile), "../../conformance/fixtures/ltx-v2/long-chain-4096")
	longRepository := filepath.Join(t.TempDir(), "long-chain")
	if err := storagefs.EnsureRepository(longRepository); err != nil {
		t.Fatalf("initialize long-chain fixture repository: %v", err)
	}
	copyRecoveryFixture(t, filepath.Join(longFixtureRoot, "snapshot-1.ltx"), storagefs.LTXPath(longRepository, 1, 1))
	for txid := uint64(2); txid <= 33; txid++ {
		fixtureName := fmt.Sprintf("delta-%03d.ltx", txid)
		copyRecoveryFixture(t, filepath.Join(longFixtureRoot, fixtureName), storagefs.LTXPath(longRepository, txid, txid))
	}
	longRestore := filepath.Join(t.TempDir(), "long-chain.sqlite")
	if err := RestoreToTarget(context.Background(), longRepository, longRestore, Target{TXID: 33}); err != nil {
		t.Fatalf("restore pinned reference 32-delta chain: %v", err)
	}
	assertLongReferenceChainState(t, longRestore)
}

func assertLargeReferenceTransactionState(t *testing.T, databasePath string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open restored large transaction fixture: %v", err)
	}
	defer database.Close()
	var rowCount, payloadBytes, wrongLengthCount int
	if err := database.QueryRow("SELECT count(*), sum(length(data)), sum(length(data) != 8192) FROM payloads").Scan(&rowCount, &payloadBytes, &wrongLengthCount); err != nil {
		t.Fatalf("read restored large transaction fixture: %v", err)
	}
	if rowCount != 512 || payloadBytes != 512*8192 || wrongLengthCount != 0 {
		t.Fatalf("large transaction fixture count/bytes/wrong_lengths = %d/%d/%d", rowCount, payloadBytes, wrongLengthCount)
	}
}

func assertLongReferenceChainState(t *testing.T, databasePath string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open restored long-chain fixture: %v", err)
	}
	defer database.Close()
	var value string
	var rowCount int
	if err := database.QueryRow("SELECT value FROM chain_values WHERE id=1").Scan(&value); err != nil {
		t.Fatalf("read restored long-chain value: %v", err)
	}
	if err := database.QueryRow("SELECT count(*) FROM chain_values").Scan(&rowCount); err != nil {
		t.Fatalf("count restored long-chain rows: %v", err)
	}
	if value != "step-033" || rowCount != 1 {
		t.Fatalf("long-chain state = %q/%d rows, want step-033/1", value, rowCount)
	}
}

func copyRecoveryFixture(t *testing.T, sourcePath, destinationPath string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o750); err != nil {
		t.Fatalf("create fixture destination directory: %v", err)
	}
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("read pinned reference fixture %q: %v", sourcePath, err)
	}
	if err := os.WriteFile(destinationPath, data, 0o600); err != nil {
		t.Fatalf("install pinned reference fixture %q: %v", sourcePath, err)
	}
}

func assertReferenceChainState(t *testing.T, databasePath, expectedValue string, expectedRows int, insertedMiddle bool) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open restored fixture database: %v", err)
	}
	defer database.Close()
	var value string
	if err := database.QueryRow("SELECT value FROM records WHERE id=7").Scan(&value); err != nil {
		t.Fatalf("read updated fixture row: %v", err)
	}
	if value != expectedValue {
		t.Fatalf("fixture row value = %q, want %q", value, expectedValue)
	}
	var rowCount int
	if err := database.QueryRow("SELECT count(*) FROM records").Scan(&rowCount); err != nil {
		t.Fatalf("count restored fixture rows: %v", err)
	}
	if rowCount != expectedRows {
		t.Fatalf("fixture row count = %d, want %d", rowCount, expectedRows)
	}
	var rowTwoCount, rowThirtyThreeCount int
	if err := database.QueryRow("SELECT count(*) FROM records WHERE id=2").Scan(&rowTwoCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow("SELECT count(*) FROM records WHERE id=33").Scan(&rowThirtyThreeCount); err != nil {
		t.Fatal(err)
	}
	wantRowTwo, wantRowThirtyThree := 0, 1
	if insertedMiddle {
		wantRowTwo = 1
	}
	if rowTwoCount != wantRowTwo || rowThirtyThreeCount != wantRowThirtyThree {
		t.Fatalf("fixture row 2/33 counts = %d/%d, want %d/%d", rowTwoCount, rowThirtyThreeCount, wantRowTwo, wantRowThirtyThree)
	}
}
