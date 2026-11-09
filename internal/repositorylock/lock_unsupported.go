//go:build !linux && !darwin

package repositorylock

import (
	"errors"
	"os"
)

// AcquireExclusive fails closed on platforms without the experimental lock.
func AcquireExclusive(_ string) (*os.File, error) {
	return nil, errors.New("repository coordination lock is unsupported on this platform")
}
