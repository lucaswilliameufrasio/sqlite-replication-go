//go:build linux || darwin

package sqliteadapter

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadStableWALFileRejectsTruncationDuringRead(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "source.sqlite-wal")
	original := bytes.Repeat([]byte("wal-frame"), 16)
	if err := os.WriteFile(walPath, original, 0o600); err != nil {
		t.Fatalf("create WAL sidecar: %v", err)
	}

	_, _, _, err := readStableWALFile(walPath, func() error {
		return os.Truncate(walPath, int64(len(original)/2))
	})
	if err == nil {
		t.Fatal("WAL truncation during the read was accepted")
	}
}

func TestReadStableWALFileRejectsSameMetadataPathReplacement(t *testing.T) {
	directory := t.TempDir()
	walPath := filepath.Join(directory, "source.sqlite-wal")
	originalTarget := filepath.Join(directory, "original-wal-data")
	replacementTarget := filepath.Join(directory, "replacement-wal-data")
	replacementLink := filepath.Join(directory, "replacement-wal-link")
	original := bytes.Repeat([]byte("original-frame"), 8)
	replacement := bytes.Repeat([]byte("replaced-frame"), 8)
	if len(original) != len(replacement) {
		t.Fatalf("test fixture lengths differ: original=%d replacement=%d", len(original), len(replacement))
	}
	if err := os.WriteFile(originalTarget, original, 0o600); err != nil {
		t.Fatalf("create original WAL target: %v", err)
	}
	if err := os.WriteFile(replacementTarget, replacement, 0o600); err != nil {
		t.Fatalf("create replacement WAL target: %v", err)
	}
	if err := os.Symlink(originalTarget, walPath); err != nil {
		t.Fatalf("create original WAL sidecar link: %v", err)
	}
	originalInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat original WAL sidecar: %v", err)
	}

	_, _, _, err = readStableWALFile(walPath, func() error {
		if err := os.Chtimes(replacementTarget, originalInfo.ModTime(), originalInfo.ModTime()); err != nil {
			return err
		}
		if err := os.Symlink(replacementTarget, replacementLink); err != nil {
			return err
		}
		if err := os.Rename(replacementLink, walPath); err != nil {
			return err
		}
		replacementInfo, err := os.Stat(walPath)
		if err != nil {
			return err
		}
		if replacementInfo.Size() != originalInfo.Size() || !replacementInfo.ModTime().Equal(originalInfo.ModTime()) {
			return fmt.Errorf("replacement fixture did not preserve size and modification time")
		}
		return nil
	})
	if err == nil {
		t.Fatal("same-size, same-time WAL path replacement was accepted")
	}
}

func TestReadStableWALFileRejectsInPlaceRewriteWithRestoredModTime(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "source.sqlite-wal")
	original := bytes.Repeat([]byte("original-frame"), 8)
	replacement := bytes.Repeat([]byte("replaced-frame"), 8)
	if len(original) != len(replacement) {
		t.Fatalf("test fixture lengths differ: original=%d replacement=%d", len(original), len(replacement))
	}
	if err := os.WriteFile(walPath, original, 0o600); err != nil {
		t.Fatalf("create original WAL sidecar: %v", err)
	}
	knownModTime := time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(walPath, knownModTime, knownModTime); err != nil {
		t.Fatalf("set original WAL timestamps: %v", err)
	}
	originalInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat original WAL sidecar: %v", err)
	}
	originalChangeTime, err := fileChangeTime(originalInfo)
	if err != nil {
		t.Fatalf("read original WAL change time: %v", err)
	}

	_, _, _, err = readStableWALFile(walPath, func() error {
		file, err := os.OpenFile(walPath, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		written, writeErr := file.WriteAt(replacement, 0)
		closeErr := file.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
		if written != len(replacement) {
			return fmt.Errorf("rewrote %d bytes, want %d", written, len(replacement))
		}
		if err := os.Chtimes(walPath, knownModTime, knownModTime); err != nil {
			return err
		}
		replacementInfo, err := os.Stat(walPath)
		if err != nil {
			return err
		}
		if replacementInfo.Size() != originalInfo.Size() || !replacementInfo.ModTime().Equal(originalInfo.ModTime()) {
			return fmt.Errorf("in-place rewrite fixture did not preserve size and modification time")
		}
		replacementChangeTime, err := fileChangeTime(replacementInfo)
		if err != nil {
			return err
		}
		if replacementChangeTime == originalChangeTime {
			return fmt.Errorf("in-place rewrite fixture did not advance change time")
		}
		return nil
	})
	if err == nil {
		t.Fatal("same-size in-place WAL rewrite with restored mtime was accepted")
	}
}

func TestVerifyStableWALPathRejectsAppearanceAfterMissingSidecar(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "source.sqlite-wal")
	if err := verifyStableWALPath(walPath, nil, false, false); err != nil {
		t.Fatalf("verify initially absent WAL sidecar: %v", err)
	}
	if err := os.WriteFile(walPath, []byte("new WAL generation"), 0o600); err != nil {
		t.Fatalf("create WAL sidecar after absence check: %v", err)
	}
	if err := verifyStableWALPath(walPath, nil, false, false); err == nil {
		t.Fatal("WAL sidecar appearance after the absent read was accepted")
	}
}

func TestVerifyStableWALPathRejectsDisappearanceAfterOpen(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "source.sqlite-wal")
	if err := os.WriteFile(walPath, []byte("captured WAL generation"), 0o600); err != nil {
		t.Fatalf("create WAL sidecar: %v", err)
	}
	walInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL sidecar: %v", err)
	}
	if err := os.Remove(walPath); err != nil {
		t.Fatalf("remove WAL sidecar: %v", err)
	}
	if err := verifyStableWALPath(walPath, walInfo, true, false); err == nil {
		t.Fatal("WAL sidecar disappearance after open was accepted")
	}
}
