package ltx

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func BenchmarkDecodeReferenceSnapshot(b *testing.B) {
	encoded, err := os.ReadFile("../conformance/fixtures/ltx-v2/snapshot-4096.ltx")
	if err != nil {
		b.Fatalf("read independent snapshot benchmark fixture: %v", err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := DecodeSnapshot(bytes.NewReader(encoded), io.Discard); err != nil {
			b.Fatalf("decode independent snapshot benchmark fixture: %v", err)
		}
	}
}

func BenchmarkDecodeReferenceLargeDelta(b *testing.B) {
	encoded, err := os.ReadFile("../conformance/fixtures/ltx-v2/large-transaction-4096/large-delta-2.ltx")
	if err != nil {
		b.Fatalf("read independent large-delta benchmark fixture: %v", err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, _, err := DecodeDelta(bytes.NewReader(encoded)); err != nil {
			b.Fatalf("decode independent large-delta benchmark fixture: %v", err)
		}
	}
}
