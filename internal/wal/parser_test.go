package wal

import (
	"errors"
	"os"
	"testing"
)

func TestScanReferenceFixtures(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		pageSize uint32
	}{
		{
			name:     "little-endian checksum",
			path:     "../../conformance/fixtures/wal/sqlite-le.wal",
			pageSize: 512,
		},
		{
			name:     "big-endian checksum",
			path:     "../../conformance/fixtures/wal/sqlite-be.wal",
			pageSize: 512,
		},
		{
			name:     "maximum page size",
			path:     "../../conformance/fixtures/wal/sqlite-le-65536.wal",
			pageSize: 65536,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content, err := os.ReadFile(test.path)
			if err != nil {
				t.Fatalf("read WAL fixture: %v", err)
			}
			result, err := Scan(content)
			if err != nil {
				t.Fatalf("scan WAL fixture: %v", err)
			}
			if result.Header.PageSize != test.pageSize || result.Header.CheckpointSequence != 7 {
				t.Fatalf("unexpected WAL header: %+v", result.Header)
			}
			if result.Header.Salt1 != 0x11223344 || result.Header.Salt2 != 0xaabbccdd {
				t.Fatalf("unexpected WAL salts: %+v", result.Header)
			}
			if len(result.Frames) != 3 || result.CommittedFrameCount != 2 || result.DatabaseSize != 2 {
				t.Fatalf("unexpected scan result: %+v", result)
			}
			if result.IncompleteTailBytes != 0 || result.Frames[0].PageOffset != headerSize+frameHeaderSize {
				t.Fatalf("unexpected WAL tail or page offset: %+v", result)
			}
		})
	}
}

func TestScanReportsIncompleteTrailingFrame(t *testing.T) {
	content, err := os.ReadFile("../../conformance/fixtures/wal/sqlite-le.wal")
	if err != nil {
		t.Fatalf("read WAL fixture: %v", err)
	}
	content = append(content, make([]byte, 17)...)
	result, err := Scan(content)
	if err != nil {
		t.Fatalf("scan WAL with partial tail: %v", err)
	}
	if len(result.Frames) != 3 || result.CommittedFrameCount != 2 || result.IncompleteTailBytes != 17 {
		t.Fatalf("unexpected scan result for partial tail: %+v", result)
	}
}

func TestScanRejectsInvalidHeaderAndFrames(t *testing.T) {
	content, err := os.ReadFile("../../conformance/fixtures/wal/sqlite-le.wal")
	if err != nil {
		t.Fatalf("read WAL fixture: %v", err)
	}
	tests := []struct {
		name      string
		mutate    func([]byte)
		wantError error
	}{
		{
			name:      "header checksum",
			mutate:    func(data []byte) { data[24] ^= 1 },
			wantError: ErrChecksumMismatch,
		},
		{
			name: "frame salt",
			mutate: func(data []byte) {
				data[headerSize+8] ^= 1
			},
			wantError: ErrInvalidFrame,
		},
		{
			name: "frame page checksum",
			mutate: func(data []byte) {
				data[headerSize+frameHeaderSize+20] ^= 1
			},
			wantError: ErrChecksumMismatch,
		},
		{
			name:      "unsupported format version",
			mutate:    func(data []byte) { data[4] ^= 1 },
			wantError: ErrInvalidHeader,
		},
		{
			name: "database header page-size sentinel",
			mutate: func(data []byte) {
				data[8] = 0
				data[9] = 0
				data[10] = 0
				data[11] = 1
			},
			wantError: ErrInvalidHeader,
		},
		{
			name: "reserved maximum page number",
			mutate: func(data []byte) {
				data[headerSize] = 0xff
				data[headerSize+1] = 0xff
				data[headerSize+2] = 0xff
				data[headerSize+3] = 0xff
			},
			wantError: ErrInvalidFrame,
		},
		{
			name: "reserved maximum database size",
			mutate: func(data []byte) {
				data[headerSize+4] = 0xff
				data[headerSize+5] = 0xff
				data[headerSize+6] = 0xff
				data[headerSize+7] = 0xff
			},
			wantError: ErrInvalidFrame,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := append([]byte(nil), content...)
			test.mutate(mutated)
			_, err := Scan(mutated)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("Scan() error = %v, want %v", err, test.wantError)
			}
		})
	}
}
