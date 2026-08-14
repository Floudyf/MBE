//go:build windows

package main

import (
	"fmt"
	"syscall"
	"unsafe"
)

const (
	processQueryLimitedInformation = 0x1000
	processVMRead                  = 0x0010
)

type filetime struct {
	LowDateTime  uint32
	HighDateTime uint32
}

type processMemoryCounters struct {
	CB                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
}

var (
	kernel32DLL          = syscall.NewLazyDLL("kernel32.dll")
	psapiDLL             = syscall.NewLazyDLL("psapi.dll")
	openProcessProc      = kernel32DLL.NewProc("OpenProcess")
	closeHandleProc      = kernel32DLL.NewProc("CloseHandle")
	getProcessTimesProc  = kernel32DLL.NewProc("GetProcessTimes")
	getProcessMemoryProc = psapiDLL.NewProc("GetProcessMemoryInfo")
)

func sampleProcessResource(pid int) (processResourceSample, error) {
	handle, _, openErr := openProcessProc.Call(processQueryLimitedInformation|processVMRead, 0, uintptr(uint32(pid)))
	if handle == 0 {
		return processResourceSample{}, fmt.Errorf("OpenProcess: %v", openErr)
	}
	defer closeHandleProc.Call(handle)

	var creation, exit, kernel, user filetime
	ok, _, timesErr := getProcessTimesProc.Call(
		handle,
		uintptr(unsafe.Pointer(&creation)),
		uintptr(unsafe.Pointer(&exit)),
		uintptr(unsafe.Pointer(&kernel)),
		uintptr(unsafe.Pointer(&user)),
	)
	if ok == 0 {
		return processResourceSample{}, fmt.Errorf("GetProcessTimes: %v", timesErr)
	}
	var counters processMemoryCounters
	counters.CB = uint32(unsafe.Sizeof(counters))
	ok, _, memoryErr := getProcessMemoryProc.Call(handle, uintptr(unsafe.Pointer(&counters)), uintptr(counters.CB))
	if ok == 0 {
		return processResourceSample{}, fmt.Errorf("GetProcessMemoryInfo: %v", memoryErr)
	}
	kernel100ns := (uint64(kernel.HighDateTime) << 32) | uint64(kernel.LowDateTime)
	user100ns := (uint64(user.HighDateTime) << 32) | uint64(user.LowDateTime)
	return processResourceSample{
		CPUTimeMS: float64(kernel100ns+user100ns) / 10_000.0,
		RSSBytes:  uint64(counters.WorkingSetSize),
	}, nil
}
