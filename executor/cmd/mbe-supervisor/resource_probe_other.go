//go:build !windows && !linux

package main

import "fmt"

func sampleProcessResource(pid int) (processResourceSample, error) {
	return processResourceSample{}, fmt.Errorf("resource sampling unsupported on this platform for pid %d", pid)
}
