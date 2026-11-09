//go:build darwin

package sqliteadapter

import (
	"fmt"
	"os"
	"syscall"
)

func fileChangeTime(info os.FileInfo) (walChangeTimestamp, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return walChangeTimestamp{}, fmt.Errorf("SQLite WAL metadata is not a Darwin stat record")
	}
	return walChangeTimestamp{seconds: stat.Ctimespec.Sec, nanoseconds: stat.Ctimespec.Nsec}, nil
}
