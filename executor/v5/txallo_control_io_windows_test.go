//go:build windows

package v5

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func txalloExclusiveReadHandleV2297(t *testing.T, path string) syscall.Handle {
	t.Helper()
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := syscall.CreateFile(
		ptr,
		syscall.GENERIC_READ,
		0, // no sharing: force transient read/replace failures in the peer
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestTxAlloControlIOV2297StableReadRecoversFromSharingViolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.json")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := txalloExclusiveReadHandleV2297(t, path)
	done := make(chan error, 1)
	go func() {
		_, err := txalloReadFileStable(path)
		done <- err
	}()
	time.Sleep(40 * time.Millisecond)
	if err := syscall.CloseHandle(h); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stable read did not recover: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stable read did not complete")
	}
}

func TestTxAlloControlIOV2297AtomicReplaceRecoversFromSharingViolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.json")
	if err := txalloWriteAtomicJSON(path, map[string]any{"epoch": 0}); err != nil {
		t.Fatal(err)
	}
	h := txalloExclusiveReadHandleV2297(t, path)
	done := make(chan error, 1)
	go func() { done <- txalloWriteAtomicJSON(path, map[string]any{"epoch": 1}) }()
	time.Sleep(40 * time.Millisecond)
	if err := syscall.CloseHandle(h); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("atomic replace did not recover: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("atomic replace did not complete")
	}
}
