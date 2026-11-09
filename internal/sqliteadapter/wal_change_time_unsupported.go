//go:build !linux && !darwin

package sqliteadapter

import (
	"errors"
	"os"
)

func fileChangeTime(os.FileInfo) (walChangeTimestamp, error) {
	return walChangeTimestamp{}, errors.New("SQLite WAL change-time validation is unsupported on this platform")
}
