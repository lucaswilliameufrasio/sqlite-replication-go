package ltx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"testing"
)

func TestSnapshotRoundTrip(t *testing.T) {
	page := make([]byte, 512)
	copy(page[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(page[16:18], 512)
	binary.BigEndian.PutUint32(page[28:32], 1)
	for i := 100; i < len(page); i++ {
		page[i] = byte(i * 17)
	}

	var encoded bytes.Buffer
	header, err := EncodeSnapshot(bytes.NewReader(page), int64(len(page)), &encoded, 1234)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	if header.PageSize != 512 || header.Commit != 1 {
		t.Fatalf("unexpected LTX header: %+v", header)
	}

	var restored bytes.Buffer
	decodedHeader, err := DecodeSnapshot(bytes.NewReader(encoded.Bytes()), &restored)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if decodedHeader != header {
		t.Fatalf("decoded header differs: got %+v, want %+v", decodedHeader, header)
	}
	if !bytes.Equal(restored.Bytes(), page) {
		t.Fatal("restored database bytes differ from source")
	}
}

func TestCompactedSnapshotPreservesInclusiveTXID(t *testing.T) {
	database := make([]byte, 512)
	copy(database[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(database[16:18], 512)
	binary.BigEndian.PutUint32(database[28:32], 1)
	var encoded bytes.Buffer
	header, err := EncodeSnapshotAt(bytes.NewReader(database), int64(len(database)), &encoded, 42, 1234)
	if err != nil {
		t.Fatalf("encode compacted snapshot: %v", err)
	}
	if header.MinTXID != 1 || header.MaxTXID != 42 || header.Timestamp != 1234 {
		t.Fatalf("compacted snapshot header = %+v", header)
	}
	var restored bytes.Buffer
	decodedHeader, err := DecodeSnapshot(bytes.NewReader(encoded.Bytes()), &restored)
	if err != nil {
		t.Fatalf("decode compacted snapshot: %v", err)
	}
	if decodedHeader != header || !bytes.Equal(restored.Bytes(), database) {
		t.Fatalf("compacted snapshot round trip differs: header=%+v size=%d", decodedHeader, restored.Len())
	}
	if _, err := EncodeSnapshotAt(bytes.NewReader(database), int64(len(database)), &bytes.Buffer{}, 0, 1234); err == nil {
		t.Fatal("snapshot with TXID zero was accepted")
	}
}

func TestDeltaRoundTrip(t *testing.T) {
	page1 := bytes.Repeat([]byte{0x11}, 512)
	page3 := bytes.Repeat([]byte{0x33}, 512)
	header := Header{
		PageSize:         512,
		Commit:           3,
		MinTXID:          2,
		MaxTXID:          2,
		Timestamp:        1234,
		PreApplyChecksum: ChecksumFlag | 0x1234,
		WALOffset:        32,
		WALSize:          32 + 2*(24+512),
		WALSalt1:         0x11223344,
		WALSalt2:         0xaabbccdd,
	}
	pages := []Page{
		{PageNumber: 1, Data: page1},
		{PageNumber: 3, Data: page3},
	}
	postApplyChecksum := ChecksumFlag | 0x9876
	var encoded bytes.Buffer
	if err := EncodeDelta(header, pages, postApplyChecksum, &encoded); err != nil {
		t.Fatalf("encode LTX delta: %v", err)
	}

	decodedHeader, decodedPages, decodedChecksum, err := DecodeDelta(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("decode LTX delta: %v", err)
	}
	if decodedHeader != header {
		t.Fatalf("decoded LTX header differs: got %+v, want %+v", decodedHeader, header)
	}
	if decodedChecksum != postApplyChecksum {
		t.Fatalf("post-apply checksum = %#x, want %#x", decodedChecksum, postApplyChecksum)
	}
	if len(decodedPages) != len(pages) {
		t.Fatalf("decoded %d pages, want %d", len(decodedPages), len(pages))
	}
	for i := range pages {
		if decodedPages[i].PageNumber != pages[i].PageNumber || !bytes.Equal(decodedPages[i].Data, pages[i].Data) {
			t.Fatalf("decoded page %d differs from input", i)
		}
	}
}

func TestDatabaseStateValidatesAndAppliesDeltaChecksums(t *testing.T) {
	image := make([]byte, 3*512)
	copy(image[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(image[16:18], 512)
	binary.BigEndian.PutUint32(image[28:32], 3)
	for index := 512; index < len(image); index++ {
		image[index] = byte(index * 13)
	}
	state, err := LoadDatabaseState(bytes.NewReader(image), int64(len(image)), 1)
	if err != nil {
		t.Fatalf("load initial database state: %v", err)
	}

	pageTwo := bytes.Repeat([]byte{0x72}, 512)
	firstHeader := Header{
		PageSize:         512,
		Commit:           3,
		MinTXID:          2,
		MaxTXID:          2,
		PreApplyChecksum: state.Checksum,
		WALOffset:        32,
		WALSize:          32 + 24 + 512,
		WALSalt1:         1,
		WALSalt2:         2,
	}
	firstPages := []Page{{PageNumber: 2, Data: pageTwo}}
	firstState, err := state.ApplyPages(firstHeader, firstPages)
	if err != nil {
		t.Fatalf("apply first delta pages: %v", err)
	}
	var encoded bytes.Buffer
	if err := EncodeDelta(firstHeader, firstPages, firstState.Checksum, &encoded); err != nil {
		t.Fatalf("encode first delta: %v", err)
	}
	decodedHeader, decodedPages, decodedChecksum, err := DecodeDelta(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("decode first delta: %v", err)
	}
	verifiedState, err := state.ApplyDelta(decodedHeader, decodedPages, decodedChecksum)
	if err != nil {
		t.Fatalf("verify first delta state: %v", err)
	}
	if verifiedState.Checksum != firstState.Checksum || verifiedState.TXID != 2 {
		t.Fatalf("unexpected state after first delta: %+v", verifiedState)
	}

	pageFour := bytes.Repeat([]byte{0x44}, 512)
	growHeader := Header{
		PageSize:         512,
		Commit:           5,
		MinTXID:          3,
		MaxTXID:          3,
		PreApplyChecksum: verifiedState.Checksum,
		WALOffset:        32 + 24 + 512,
		WALSize:          32 + 2*(24+512),
		WALSalt1:         1,
		WALSalt2:         2,
	}
	grownState, err := verifiedState.ApplyPages(growHeader, []Page{{PageNumber: 4, Data: pageFour}})
	if err != nil {
		t.Fatalf("apply growing delta: %v", err)
	}
	if len(grownState.pageChecksums) != 5 {
		t.Fatalf("grown page checksum count = %d, want 5", len(grownState.pageChecksums))
	}

	shrinkHeader := Header{
		PageSize:         512,
		Commit:           2,
		MinTXID:          4,
		MaxTXID:          4,
		PreApplyChecksum: grownState.Checksum,
		WALOffset:        32 + 2*(24+512),
		WALSize:          32 + 3*(24+512),
		WALSalt1:         1,
		WALSalt2:         2,
	}
	shrunkState, err := grownState.ApplyPages(shrinkHeader, nil)
	if err != nil {
		t.Fatalf("apply shrinking delta: %v", err)
	}
	if shrunkState.Commit != 2 || len(shrunkState.pageChecksums) != 2 {
		t.Fatalf("unexpected state after shrink: %+v", shrunkState)
	}
}

func TestDatabaseStateBuildsReconciliationDeltas(t *testing.T) {
	base := make([]byte, 2*512)
	copy(base[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(base[16:18], 512)
	binary.BigEndian.PutUint32(base[28:32], 2)
	for index := 512; index < len(base); index++ {
		base[index] = byte(index * 9)
	}
	state, err := LoadDatabaseState(bytes.NewReader(base), int64(len(base)), 1)
	if err != nil {
		t.Fatalf("load base state: %v", err)
	}

	target := append(append([]byte(nil), base...), bytes.Repeat([]byte{0x73}, 512)...)
	binary.BigEndian.PutUint32(target[28:32], 3)
	pageTwoUpdated := bytes.Repeat([]byte{0x42}, 512)
	copy(target[512:1024], pageTwoUpdated)
	header, pages, updated, err := state.DeltaToDatabaseImage(bytes.NewReader(target), int64(len(target)), 200)
	if err != nil {
		t.Fatalf("build reconciliation delta: %v", err)
	}
	if header == nil || header.Commit != 3 || header.MinTXID != 2 || header.WALOffset != 0 || len(pages) != 3 {
		t.Fatalf("unexpected reconciliation delta: header=%+v pages=%+v", header, pages)
	}
	if pages[0].PageNumber != 1 || pages[1].PageNumber != 2 || pages[2].PageNumber != 3 {
		t.Fatalf("reconciliation pages are not sorted/complete: %+v", pages)
	}
	expected, err := LoadDatabaseState(bytes.NewReader(target), int64(len(target)), 2)
	if err != nil {
		t.Fatalf("load target state: %v", err)
	}
	if updated.Checksum != expected.Checksum {
		t.Fatalf("reconciled checksum = %#x, want %#x", updated.Checksum, expected.Checksum)
	}
	if noHeader, noPages, unchanged, err := updated.DeltaToDatabaseImage(bytes.NewReader(target), int64(len(target)), 201); err != nil {
		t.Fatalf("check already reconciled image: %v", err)
	} else if noHeader != nil || len(noPages) != 0 || unchanged.TXID != updated.TXID ||
		unchanged.Commit != updated.Commit || unchanged.Checksum != updated.Checksum {
		t.Fatal("identical image produced another reconciliation delta")
	}

	shrunk := target[:512]
	binary.BigEndian.PutUint32(shrunk[28:32], 1)
	shrinkHeader, shrinkPages, shrunkState, err := updated.DeltaToDatabaseImage(bytes.NewReader(shrunk), int64(len(shrunk)), 202)
	if err != nil {
		t.Fatalf("build shrink reconciliation delta: %v", err)
	}
	if shrinkHeader == nil || shrinkHeader.Commit != 1 || len(shrinkPages) != 1 || shrinkPages[0].PageNumber != 1 {
		t.Fatalf("unexpected shrink reconciliation delta: header=%+v pages=%+v", shrinkHeader, shrinkPages)
	}
	expectedShrink, err := LoadDatabaseState(bytes.NewReader(shrunk), int64(len(shrunk)), 3)
	if err != nil {
		t.Fatalf("load shrink target state: %v", err)
	}
	if shrunkState.Checksum != expectedShrink.Checksum {
		t.Fatalf("shrink checksum = %#x, want %#x", shrunkState.Checksum, expectedShrink.Checksum)
	}
}

func TestEncodeDeltaRejectsInvalidPages(t *testing.T) {
	header := Header{
		PageSize:         512,
		Commit:           3,
		MinTXID:          2,
		MaxTXID:          2,
		PreApplyChecksum: ChecksumFlag | 1,
		WALOffset:        32,
		WALSize:          1024,
		WALSalt1:         1,
		WALSalt2:         2,
	}
	validPage := bytes.Repeat([]byte{0x22}, 512)
	tests := []struct {
		name  string
		pages []Page
	}{
		{name: "zero page number", pages: []Page{{PageNumber: 0, Data: validPage}}},
		{name: "page beyond commit", pages: []Page{{PageNumber: 4, Data: validPage}}},
		{name: "unsorted pages", pages: []Page{{PageNumber: 2, Data: validPage}, {PageNumber: 1, Data: validPage}}},
		{name: "duplicate page", pages: []Page{{PageNumber: 1, Data: validPage}, {PageNumber: 1, Data: validPage}}},
		{name: "wrong page size", pages: []Page{{PageNumber: 1, Data: validPage[:256]}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := EncodeDelta(header, test.pages, ChecksumFlag|3, &bytes.Buffer{}); err == nil {
				t.Fatal("expected invalid delta pages to fail")
			}
		})
	}
}

func TestDecodeRejectsCorruptionAndTruncation(t *testing.T) {
	page := make([]byte, 512)
	copy(page[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(page[16:18], 512)
	binary.BigEndian.PutUint32(page[28:32], 1)
	var encoded bytes.Buffer
	if _, err := EncodeSnapshot(bytes.NewReader(page), int64(len(page)), &encoded, 1); err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}

	corrupt := append([]byte(nil), encoded.Bytes()...)
	corrupt[len(corrupt)-1] ^= 0x01
	if _, err := DecodeSnapshot(bytes.NewReader(corrupt), &bytes.Buffer{}); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}
	truncated := encoded.Bytes()[:encoded.Len()-1]
	if _, err := DecodeSnapshot(bytes.NewReader(truncated), &bytes.Buffer{}); err == nil {
		t.Fatal("expected truncated snapshot to fail")
	}
}

func TestLockPageNumber(t *testing.T) {
	tests := []struct {
		pageSize uint32
		want     uint32
	}{
		{512, 2097153},
		{4096, 262145},
		{65536, 16385},
	}
	for _, test := range tests {
		if got := lockPageNo(test.pageSize); got != test.want {
			t.Errorf("lockPageNo(%d) = %d, want %d", test.pageSize, got, test.want)
		}
	}
}

func TestReferenceSnapshotFixture(t *testing.T) {
	content, err := os.ReadFile("../conformance/fixtures/ltx-v2/snapshot-4096.ltx")
	if err != nil {
		t.Fatalf("read reference fixture: %v", err)
	}
	var restored bytes.Buffer
	header, err := DecodeSnapshot(bytes.NewReader(content), &restored)
	if err != nil {
		t.Fatalf("decode reference fixture: %v", err)
	}
	if header.PageSize != 4096 || header.Commit < 2 {
		t.Fatalf("unexpected reference snapshot header: %+v", header)
	}
	if !bytes.HasPrefix(restored.Bytes(), []byte("SQLite format 3\x00")) {
		t.Fatal("reference fixture did not decode to a SQLite image")
	}
	if got, want := restored.Len(), int(header.PageSize*header.Commit); got != want {
		t.Fatalf("restored image size = %d, want %d", got, want)
	}
}
