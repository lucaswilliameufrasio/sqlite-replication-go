package wal

import (
	"os"
	"testing"
)

func FuzzScan(f *testing.F) {
	f.Add([]byte{})
	for _, fixturePath := range []string{
		"../../conformance/fixtures/wal/sqlite-le.wal",
		"../../conformance/fixtures/wal/sqlite-be.wal",
		"../../conformance/fixtures/wal/sqlite-le-65536.wal",
	} {
		fixture, err := os.ReadFile(fixturePath)
		if err != nil {
			f.Fatalf("read independent WAL fuzz seed %q: %v", fixturePath, err)
		}
		f.Add(fixture)
	}

	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > 1024*1024 {
			return
		}
		result, err := Scan(encoded)
		if err != nil {
			return
		}
		if !validPageSize(result.Header.PageSize) {
			t.Fatalf("Scan accepted invalid page size %d", result.Header.PageSize)
		}
		if result.CommittedFrameCount < 0 || result.CommittedFrameCount > len(result.Frames) {
			t.Fatalf("Scan returned invalid committed frame count %d for %d frames", result.CommittedFrameCount, len(result.Frames))
		}
		frameSize := int64(frameHeaderSize + int(result.Header.PageSize))
		previousPageOffset := int64(0)
		for index, frame := range result.Frames {
			if frame.PageOffset < headerSize+frameHeaderSize || frame.PageOffset+int64(result.Header.PageSize) > int64(len(encoded)) {
				t.Fatalf("Scan returned page offset outside input: %+v", frame)
			}
			if index > 0 && frame.PageOffset-previousPageOffset != frameSize {
				t.Fatalf("Scan returned non-contiguous frame offsets: previous=%d current=%d", previousPageOffset, frame.PageOffset)
			}
			previousPageOffset = frame.PageOffset
		}
		if result.CommittedFrameCount == 0 && result.DatabaseSize != 0 {
			t.Fatalf("Scan returned database size %d without a committed frame", result.DatabaseSize)
		}
		if result.IncompleteTailBytes < 0 || result.IncompleteTailBytes >= int(frameSize) {
			t.Fatalf("Scan returned invalid incomplete tail length %d", result.IncompleteTailBytes)
		}
	})
}
