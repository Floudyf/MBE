//go:build !windows

package v5

import "os"

// MBE_TXALLO_POSIX_ATOMIC_V227
func txalloAtomicReplace(src, dst string) error {
	return os.Rename(src, dst)
}
