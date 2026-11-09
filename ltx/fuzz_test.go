package ltx

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

func FuzzDecodeSnapshot(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("LTX1"))

	database := make([]byte, 512)
	copy(database[:16], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(database[16:18], 512)
	binary.BigEndian.PutUint32(database[28:32], 1)
	var validSnapshot bytes.Buffer
	if _, err := EncodeSnapshot(bytes.NewReader(database), int64(len(database)), &validSnapshot, 1234); err != nil {
		f.Fatalf("encode snapshot fuzz seed: %v", err)
	}
	f.Add(validSnapshot.Bytes())
	for _, fixturePath := range []string{
		"../conformance/fixtures/ltx-v2/snapshot-4096.ltx",
	} {
		fixture, err := os.ReadFile(fixturePath)
		if err != nil {
			f.Fatalf("read independent snapshot fuzz seed %q: %v", fixturePath, err)
		}
		f.Add(fixture)
	}

	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > 4*1024*1024 {
			return
		}
		var restored bytes.Buffer
		header, err := DecodeSnapshot(bytes.NewReader(encoded), &restored)
		if err != nil {
			return
		}
		if header.MinTXID != 1 || !validPageSize(header.PageSize) {
			t.Fatalf("decoder accepted invalid snapshot header: %+v", header)
		}
		if got, want := restored.Len(), int(header.PageSize)*int(header.Commit); got != want {
			t.Fatalf("restored size = %d, want %d", got, want)
		}
		if !bytes.HasPrefix(restored.Bytes(), []byte("SQLite format 3\x00")) {
			t.Fatal("successful snapshot decode did not produce a SQLite image")
		}
	})
}

func FuzzDecodeDelta(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("LTX1"))

	header := Header{
		PageSize:         512,
		Commit:           1,
		MinTXID:          2,
		MaxTXID:          2,
		PreApplyChecksum: ChecksumFlag | 1,
	}
	page := Page{PageNumber: 1, Data: bytes.Repeat([]byte{0x5a}, 512)}
	var validDelta bytes.Buffer
	if err := EncodeDelta(header, []Page{page}, ChecksumFlag|2, &validDelta); err != nil {
		f.Fatalf("encode delta fuzz seed: %v", err)
	}
	f.Add(validDelta.Bytes())
	for _, fixturePath := range []string{
		"../conformance/fixtures/ltx-v2/recovery-chain-4096/delta-2.ltx",
		"../conformance/fixtures/ltx-v2/long-chain-4096/delta-033.ltx",
	} {
		fixture, err := os.ReadFile(fixturePath)
		if err != nil {
			f.Fatalf("read independent delta fuzz seed %q: %v", fixturePath, err)
		}
		f.Add(fixture)
	}

	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > 4*1024*1024 {
			return
		}
		header, pages, postApplyChecksum, err := DecodeDelta(bytes.NewReader(encoded))
		if err != nil {
			return
		}
		if header.MinTXID <= 1 || header.MaxTXID < header.MinTXID || !validPageSize(header.PageSize) {
			t.Fatalf("decoder accepted invalid delta header: %+v", header)
		}
		if postApplyChecksum&ChecksumFlag == 0 {
			t.Fatalf("decoder accepted unflagged post-apply checksum: %#x", postApplyChecksum)
		}
		var previousPage uint32
		for _, page := range pages {
			if page.PageNumber <= previousPage || page.PageNumber > header.Commit {
				t.Fatalf("decoder accepted invalid delta page ordering: %+v", pages)
			}
			if len(page.Data) != int(header.PageSize) {
				t.Fatalf("decoded page length = %d, want %d", len(page.Data), header.PageSize)
			}
			previousPage = page.PageNumber
		}
	})
}
