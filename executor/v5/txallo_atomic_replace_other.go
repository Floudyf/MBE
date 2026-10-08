//go:build !windows

package v5

import "os"

// MBE_TXALLO_POSIX_ATOMIC_V2297
func txalloControlFileTransient(error) bool              { return false }
func txalloReadFileStable(path string) ([]byte, error)   { return os.ReadFile(path) }
func txalloOpenFileStable(path string) (*os.File, error) { return os.Open(path) }
func txalloOpenAppendFileStable(path string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, perm)
}
func txalloAtomicReplace(src, dst string) error { return os.Rename(src, dst) }

// POSIX hard-link publication is atomic and refuses an existing destination.
// The temp file is created in the same directory, so link/unlink remains on one
// filesystem and readers can never observe partial bytes.
func txalloAtomicPublishNew(src, dst string) error {
	if err := os.Link(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}
