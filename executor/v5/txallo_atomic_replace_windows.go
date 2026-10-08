//go:build windows

package v5

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// MBE_TXALLO_WIN_ATOMIC_V2297
// TxAllo dynamic control-plane files are shared across client/node processes.
// Windows may transiently reject reads, appends or atomic publication while a
// peer has a short-lived handle open. Retry only the documented sharing/access
// class; every other I/O error remains fail-closed.
var txalloKernel32V2297 = syscall.NewLazyDLL("kernel32.dll")
var txalloMoveFileExWV2297 = txalloKernel32V2297.NewProc("MoveFileExW")

const (
	txalloMoveFileReplaceExistingV2297 = 0x1
	txalloMoveFileWriteThroughV2297    = 0x8
	txalloControlRetryCountV2297       = 200
	txalloControlRetryDelayV2297       = 5 * time.Millisecond
	txalloErrorAccessDeniedV2297       = syscall.Errno(5)
	txalloErrorSharingViolationV2297   = syscall.Errno(32)
	txalloErrorLockViolationV2297      = syscall.Errno(33)
	txalloErrorFileExistsV2297         = syscall.Errno(80)
	txalloErrorAlreadyExistsV2297      = syscall.Errno(183)
)

func txalloControlFileTransient(err error) bool {
	return errors.Is(err, txalloErrorAccessDeniedV2297) ||
		errors.Is(err, txalloErrorSharingViolationV2297) ||
		errors.Is(err, txalloErrorLockViolationV2297)
}

func txalloReadFileStable(path string) ([]byte, error) {
	var last error
	for attempt := 0; attempt < txalloControlRetryCountV2297; attempt++ {
		raw, err := os.ReadFile(path)
		if err == nil {
			return raw, nil
		}
		if !txalloControlFileTransient(err) {
			return nil, err
		}
		last = err
		if attempt+1 < txalloControlRetryCountV2297 {
			time.Sleep(txalloControlRetryDelayV2297)
		}
	}
	return nil, last
}

func txalloOpenFileStable(path string) (*os.File, error) {
	var last error
	for attempt := 0; attempt < txalloControlRetryCountV2297; attempt++ {
		f, err := os.Open(path)
		if err == nil {
			return f, nil
		}
		if !txalloControlFileTransient(err) {
			return nil, err
		}
		last = err
		if attempt+1 < txalloControlRetryCountV2297 {
			time.Sleep(txalloControlRetryDelayV2297)
		}
	}
	return nil, last
}

func txalloOpenAppendFileStable(path string, perm os.FileMode) (*os.File, error) {
	var last error
	for attempt := 0; attempt < txalloControlRetryCountV2297; attempt++ {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, perm)
		if err == nil {
			return f, nil
		}
		if !txalloControlFileTransient(err) {
			return nil, err
		}
		last = err
		if attempt+1 < txalloControlRetryCountV2297 {
			time.Sleep(txalloControlRetryDelayV2297)
		}
	}
	return nil, last
}

func txalloAtomicReplace(src, dst string) error {
	return txalloMoveFileExV2297(src, dst, txalloMoveFileReplaceExistingV2297|txalloMoveFileWriteThroughV2297, false)
}

// txalloAtomicPublishNew atomically publishes an immutable epoch file. The
// destination must not already exist; callers resolve an existence race by
// validating the already-published bytes for exact equality.
func txalloAtomicPublishNew(src, dst string) error {
	return txalloMoveFileExV2297(src, dst, txalloMoveFileWriteThroughV2297, true)
}

func txalloMoveFileExV2297(src, dst string, flags uintptr, noReplace bool) error {
	srcp, err := syscall.UTF16PtrFromString(src)
	if err != nil {
		return fmt.Errorf("TxAllo atomic source path: %w", err)
	}
	dstp, err := syscall.UTF16PtrFromString(dst)
	if err != nil {
		return fmt.Errorf("TxAllo atomic destination path: %w", err)
	}
	for attempt := 0; attempt < txalloControlRetryCountV2297; attempt++ {
		r1, _, callErr := txalloMoveFileExWV2297.Call(
			uintptr(unsafe.Pointer(srcp)),
			uintptr(unsafe.Pointer(dstp)),
			flags,
		)
		if r1 != 0 {
			return nil
		}
		if callErr == syscall.Errno(0) {
			callErr = syscall.EINVAL
		}
		if noReplace && (callErr == txalloErrorFileExistsV2297 || callErr == txalloErrorAlreadyExistsV2297) {
			return fmt.Errorf("TxAllo immutable destination exists: %w", os.ErrExist)
		}
		if !txalloControlFileTransient(callErr) {
			return fmt.Errorf("MoveFileExW: %w", callErr)
		}
		if attempt+1 < txalloControlRetryCountV2297 {
			time.Sleep(txalloControlRetryDelayV2297)
			continue
		}
		return fmt.Errorf("MoveFileExW after %d retries: %w", txalloControlRetryCountV2297, callErr)
	}
	return fmt.Errorf("MoveFileExW exhausted retries")
}
