//go:build windows

package v5

import (
	"fmt"
	"syscall"
	"time"
	"unsafe"
)

// MBE_TXALLO_WIN_ATOMIC_V227
// Windows os.Rename does not replace an existing destination. TxAllo mapping
// snapshots are intentionally rewritten at every mapping epoch, so use the
// native replace-existing primitive while preserving same-directory atomic
// publication and requesting write-through before the call returns.
//
// Mapping watchers can briefly have the old snapshot open. Windows may reject
// replacement during that tiny sharing window, so retry only the two transient
// sharing/access errors. The old snapshot stays present throughout; there is no
// delete-before-rename gap.
var txalloKernel32V227 = syscall.NewLazyDLL("kernel32.dll")
var txalloMoveFileExWV227 = txalloKernel32V227.NewProc("MoveFileExW")

const (
	txalloMoveFileReplaceExistingV227 = 0x1
	txalloMoveFileWriteThroughV227    = 0x8
	txalloReplaceRetryCountV227       = 200
	txalloReplaceRetryDelayV227       = 5 * time.Millisecond
	txalloErrorAccessDeniedV227       = syscall.Errno(5)
	txalloErrorSharingViolationV227   = syscall.Errno(32)
)

func txalloAtomicReplace(src, dst string) error {
	srcp, err := syscall.UTF16PtrFromString(src)
	if err != nil {
		return fmt.Errorf("TxAllo atomic replace source path: %w", err)
	}
	dstp, err := syscall.UTF16PtrFromString(dst)
	if err != nil {
		return fmt.Errorf("TxAllo atomic replace destination path: %w", err)
	}
	for attempt := 0; attempt < txalloReplaceRetryCountV227; attempt++ {
		r1, _, callErr := txalloMoveFileExWV227.Call(
			uintptr(unsafe.Pointer(srcp)),
			uintptr(unsafe.Pointer(dstp)),
			uintptr(txalloMoveFileReplaceExistingV227|txalloMoveFileWriteThroughV227),
		)
		if r1 != 0 {
			return nil
		}
		if callErr == syscall.Errno(0) {
			callErr = syscall.EINVAL
		}
		if callErr != txalloErrorAccessDeniedV227 && callErr != txalloErrorSharingViolationV227 {
			return fmt.Errorf("MoveFileExW replace-existing: %w", callErr)
		}
		if attempt+1 < txalloReplaceRetryCountV227 {
			time.Sleep(txalloReplaceRetryDelayV227)
			continue
		}
		return fmt.Errorf("MoveFileExW replace-existing after %d retries: %w", txalloReplaceRetryCountV227, callErr)
	}
	return fmt.Errorf("MoveFileExW replace-existing exhausted retries")
}
