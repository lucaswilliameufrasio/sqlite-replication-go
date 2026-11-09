package walcapture

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/lucaswilliameufrasio/sqlite-replication-go/internal/wal"
	"github.com/lucaswilliameufrasio/sqlite-replication-go/ltx"
)

type walFrame struct {
	pageNumber   uint32
	databaseSize uint32
	page         []byte
}

func TestTranslateWALCommitBoundariesAndResume(t *testing.T) {
	base := sqliteImage(2)
	baseState, err := ltx.LoadDatabaseState(bytes.NewReader(base), int64(len(base)), 1)
	if err != nil {
		t.Fatalf("load snapshot state: %v", err)
	}

	pageOneGrown := append([]byte(nil), base[:512]...)
	binary.BigEndian.PutUint32(pageOneGrown[28:32], 3)
	pageOneShrunk := append([]byte(nil), pageOneGrown...)
	binary.BigEndian.PutUint32(pageOneShrunk[28:32], 1)
	pageTwoA := bytes.Repeat([]byte{0xa1}, 512)
	pageTwoB := bytes.Repeat([]byte{0xb2}, 512)
	pageTwoC := bytes.Repeat([]byte{0xc3}, 512)
	pageThreeA := bytes.Repeat([]byte{0x3a}, 512)
	pageThreeB := bytes.Repeat([]byte{0x3b}, 512)
	frames := []walFrame{
		{pageNumber: 2, page: pageTwoA},
		{pageNumber: 2, databaseSize: 2, page: pageTwoB},
		{pageNumber: 1, page: pageOneGrown},
		{pageNumber: 3, databaseSize: 3, page: pageThreeA},
		{pageNumber: 2, page: pageTwoC},
		{pageNumber: 3, page: pageThreeB},
		{pageNumber: 1, databaseSize: 1, page: pageOneShrunk},
	}
	walBytes := encodeWAL(frames, 7, 0x11223344, 0xaabbccdd)

	transactions, current, err := TranslateWAL(walBytes, State{Database: baseState}, 100)
	if err != nil {
		t.Fatalf("translate WAL: %v", err)
	}
	if len(transactions) != 3 {
		t.Fatalf("translated %d transactions, want 3", len(transactions))
	}
	if transactions[0].StartFrame != 1 || transactions[0].EndFrame != 2 ||
		len(transactions[0].Pages) != 1 || transactions[0].Pages[0].PageNumber != 2 ||
		!bytes.Equal(transactions[0].Pages[0].Data, pageTwoB) {
		t.Fatalf("first translated transaction has wrong commit boundary or last-page value: %+v", transactions[0])
	}
	if transactions[0].Header.WALSize != 32+2*(24+512) {
		t.Fatalf("first delta WAL size = %d, want end of its commit frame", transactions[0].Header.WALSize)
	}
	if transactions[1].StartFrame != 3 || transactions[1].EndFrame != 4 ||
		len(transactions[1].Pages) != 2 || transactions[1].Pages[0].PageNumber != 1 ||
		transactions[1].Pages[1].PageNumber != 3 {
		t.Fatalf("second translated transaction has wrong commit boundary or pages: %+v", transactions[1])
	}
	if transactions[1].Header.WALSize != 32+4*(24+512) {
		t.Fatalf("second delta WAL size = %d, want end of its commit frame", transactions[1].Header.WALSize)
	}
	if transactions[2].StartFrame != 5 || transactions[2].EndFrame != 7 ||
		len(transactions[2].Pages) != 1 || transactions[2].Pages[0].PageNumber != 1 {
		t.Fatalf("shrinking transaction included truncated pages: %+v", transactions[2])
	}
	if current.Database.TXID != 4 || current.Database.Commit != 1 || current.WAL.FrameCount != 7 {
		t.Fatalf("unexpected resulting capture state: %+v", current)
	}

	wantFinal := append([]byte(nil), pageOneShrunk...)
	wantState, err := ltx.LoadDatabaseState(bytes.NewReader(wantFinal), int64(len(wantFinal)), 4)
	if err != nil {
		t.Fatalf("load expected final image state: %v", err)
	}
	if current.Database.Checksum != wantState.Checksum {
		t.Fatalf("translated checksum = %#x, want %#x", current.Database.Checksum, wantState.Checksum)
	}
	verifiedState := baseState
	for _, transaction := range transactions {
		header, pages, postChecksum, err := ltx.DecodeDelta(bytes.NewReader(transaction.Encoded))
		if err != nil {
			t.Fatalf("decode translated transaction: %v", err)
		}
		verifiedState, err = verifiedState.ApplyDelta(header, pages, postChecksum)
		if err != nil {
			t.Fatalf("verify translated delta %d: %v", transaction.Header.MinTXID, err)
		}
	}
	if verifiedState.Checksum != current.Database.Checksum {
		t.Fatal("decoded LTX chain does not reproduce the translated checksum state")
	}

	repeated, repeatedState, err := TranslateWAL(walBytes, current, 101)
	if err != nil {
		t.Fatalf("resume unchanged WAL: %v", err)
	}
	if len(repeated) != 0 || !reflect.DeepEqual(repeatedState, current) {
		t.Fatalf("unchanged WAL was not idempotent: transactions=%d", len(repeated))
	}

	pageOneNext := append([]byte(nil), pageOneShrunk...)
	pageOneNext[100] ^= 0x7f
	frames = append(frames, walFrame{pageNumber: 1, databaseSize: 1, page: pageOneNext})
	walBytes = encodeWAL(frames, 7, 0x11223344, 0xaabbccdd)
	appended, appendedState, err := TranslateWAL(walBytes, current, 102)
	if err != nil {
		t.Fatalf("translate appended WAL commit: %v", err)
	}
	if len(appended) != 1 || appended[0].StartFrame != 8 || appended[0].EndFrame != 8 || appendedState.Database.TXID != 5 {
		t.Fatalf("appended WAL commit was not isolated: transactions=%+v", appended)
	}

	changedPrefix := append([]walFrame(nil), frames...)
	changedPrefix[0].page = bytes.Repeat([]byte{0xee}, 512)
	changedWAL := encodeWAL(changedPrefix, 7, 0x11223344, 0xaabbccdd)
	if _, _, err := TranslateWAL(changedWAL, current, 103); err == nil {
		t.Fatal("accepted a modified captured prefix with the same WAL generation")
	}
	truncatedWAL := walBytes[:len(walBytes)-2*(24+512)]
	if _, _, err := TranslateWAL(truncatedWAL, current, 103); err == nil {
		t.Fatal("accepted a WAL truncated before the persisted commit boundary")
	}
	generationPage := append([]byte(nil), pageOneShrunk...)
	generationPage[101] ^= 0x5c
	newGenerationWAL := encodeWAL([]walFrame{{pageNumber: 1, databaseSize: 1, page: generationPage}}, 8, 0x55667788, 0x99aabbcc)
	newGenerationTransactions, newGenerationState, err := TranslateWAL(newGenerationWAL, current, 103)
	if err != nil {
		t.Fatalf("translate WAL after salt/checkpoint generation change: %v", err)
	}
	if len(newGenerationTransactions) != 1 || newGenerationState.WAL.Salt1 != 0x55667788 ||
		newGenerationState.WAL.CheckpointSequence != 8 {
		t.Fatalf("new WAL generation was not established: %+v", newGenerationState.WAL)
	}

	partialWAL := append(append([]byte(nil), walBytes...), 0)
	partialTransactions, partialState, err := TranslateWAL(partialWAL, current, 104)
	if err != nil {
		t.Fatalf("rejecting incomplete uncommitted tail was not expected: %v", err)
	}
	if len(partialTransactions) != 1 || partialState.WAL.FrameCount != 8 || partialState.Database.TXID != 5 {
		t.Fatal("incomplete tail changed the last committed boundary")
	}

	if _, err := PositionFromWAL(walBytes); err != nil {
		t.Fatalf("initialize position from exact WAL boundary: %v", err)
	}
	if position, err := PositionFromWAL(partialWAL); err != nil || position.FrameCount != 8 {
		t.Fatalf("incomplete tail changed the last committed boundary: position=%+v err=%v", position, err)
	}
	if _, err := PositionFromWALPrefix(partialWAL, int64(len(partialWAL))); err == nil {
		t.Fatal("accepted a recovery prefix that does not end at a commit boundary")
	}
	if _, err := wal.Scan(walBytes); err != nil {
		t.Fatalf("test WAL was not valid: %v", err)
	}
}

func sqliteImage(pageCount uint32) []byte {
	image := make([]byte, int(pageCount)*512)
	copy(image[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(image[16:18], 512)
	binary.BigEndian.PutUint32(image[28:32], pageCount)
	for index := 512; index < len(image); index++ {
		image[index] = byte(index * 17)
	}
	return image
}

func encodeWAL(frames []walFrame, checkpointSequence, salt1, salt2 uint32) []byte {
	pageSize := uint32(len(frames[0].page))
	content := make([]byte, 32)
	binary.BigEndian.PutUint32(content[:4], 0x377f0682)
	binary.BigEndian.PutUint32(content[4:8], 3007000)
	binary.BigEndian.PutUint32(content[8:12], pageSize)
	binary.BigEndian.PutUint32(content[12:16], checkpointSequence)
	binary.BigEndian.PutUint32(content[16:20], salt1)
	binary.BigEndian.PutUint32(content[20:24], salt2)
	checksum1, checksum2 := walChecksum(content[:24], 0, 0)
	binary.BigEndian.PutUint32(content[24:28], checksum1)
	binary.BigEndian.PutUint32(content[28:32], checksum2)

	for _, frame := range frames {
		frameHeader := make([]byte, 24)
		binary.BigEndian.PutUint32(frameHeader[:4], frame.pageNumber)
		binary.BigEndian.PutUint32(frameHeader[4:8], frame.databaseSize)
		binary.BigEndian.PutUint32(frameHeader[8:12], salt1)
		binary.BigEndian.PutUint32(frameHeader[12:16], salt2)
		checksum1, checksum2 = walChecksum(frameHeader[:8], checksum1, checksum2)
		checksum1, checksum2 = walChecksum(frame.page, checksum1, checksum2)
		binary.BigEndian.PutUint32(frameHeader[16:20], checksum1)
		binary.BigEndian.PutUint32(frameHeader[20:24], checksum2)
		content = append(content, frameHeader...)
		content = append(content, frame.page...)
	}
	return content
}

func walChecksum(data []byte, checksum1, checksum2 uint32) (uint32, uint32) {
	for offset := 0; offset < len(data); offset += 8 {
		checksum1 += binary.LittleEndian.Uint32(data[offset:offset+4]) + checksum2
		checksum2 += binary.LittleEndian.Uint32(data[offset+4:offset+8]) + checksum1
	}
	return checksum1, checksum2
}
