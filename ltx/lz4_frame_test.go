package ltx

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/pierrec/lz4/v4"
)

func lz4Frame(t *testing.T, content []byte, options ...lz4.Option) []byte {
	t.Helper()
	var frame bytes.Buffer
	writer := lz4.NewWriter(&frame)
	if err := writer.Apply(options...); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return frame.Bytes()
}

// LTX places its own bytes immediately after every page frame. Decoding a
// page must consume the frame and nothing else, whatever the LZ4 library does
// at the end of a frame.
func TestReadLZ4PageStopsAtTheFrameBoundary(t *testing.T) {
	compressible := bytes.Repeat([]byte{7}, 4096)
	incompressible := make([]byte, 4096)
	for index := range incompressible {
		incompressible[index] = byte(index*131 + index>>3)
	}
	trailers := map[string][]byte{
		"next LTX page header":    {0, 0, 0, 2},
		"another LZ4 frame":       lz4Frame(t, compressible),
		"bytes resembling a size": {0xff, 0xff, 0xff, 0x7f, 1, 2, 3, 4},
		"end of input":            nil,
	}
	for name, content := range map[string][]byte{"compressible": compressible, "incompressible": incompressible} {
		for _, options := range [][]lz4.Option{nil, {lz4.BlockChecksumOption(true)}, {lz4.ChecksumOption(false)}, {lz4.SizeOption(uint64(len(content)))}} {
			frame := lz4Frame(t, content, options...)
			for trailerName, trailer := range trailers {
				reader := bytes.NewReader(append(append([]byte(nil), frame...), trailer...))
				page := make([]byte, len(content))
				if err := readLZ4Page(reader, page); err != nil {
					t.Fatalf("%s page followed by %s: %v", name, trailerName, err)
				}
				rest, err := io.ReadAll(reader)
				if err != nil || !bytes.Equal(page, content) || !bytes.Equal(rest, trailer) {
					t.Fatalf("%s page followed by %s: decoded=%v left %d bytes, want %d", name, trailerName, bytes.Equal(page, content), len(rest), len(trailer))
				}
			}
		}
	}
}

func TestReadLZ4PageRejectsMalformedFrames(t *testing.T) {
	content := bytes.Repeat([]byte{9}, 4096)
	frame := lz4Frame(t, content)
	page := make([]byte, len(content))
	for name, input := range map[string][]byte{
		"empty input":       nil,
		"wrong magic":       append([]byte{1, 2, 3, 4}, frame[4:]...),
		"truncated header":  frame[:5],
		"truncated block":   frame[:len(frame)-10],
		"missing checksum":  frame[:len(frame)-2],
		"two pages":         lz4Frame(t, append(append([]byte(nil), content...), content...)),
		"short page":        lz4Frame(t, content[:100]),
		"unsupported flags": append(append([]byte(nil), frame[:4]...), append([]byte{frame[4] | 0x02}, frame[5:]...)...),
	} {
		if err := readLZ4Page(bytes.NewReader(input), page); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// A block size field cannot make the decoder allocate without bound.
	huge := append(append([]byte(nil), frame[:7]...), 0xff, 0xff, 0xff, 0x7f)
	if err := readLZ4Page(bytes.NewReader(huge), page); !errors.Is(err, ErrInvalidFile) {
		t.Fatalf("oversized block declaration: %v", err)
	}
	if err := readLZ4Page(bytes.NewReader(append(append([]byte(nil), frame...), frame...)), make([]byte, 2*len(content))); err == nil {
		t.Error("a page spanning two frames was accepted")
	}
}
