//go:build linux

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func sampleProcessResource(pid int) (processResourceSample, error) {
	schedstat, err := os.ReadFile(fmt.Sprintf("/proc/%d/schedstat", pid))
	if err != nil {
		return processResourceSample{}, err
	}
	fields := strings.Fields(string(schedstat))
	if len(fields) < 1 {
		return processResourceSample{}, fmt.Errorf("invalid schedstat")
	}
	runtimeNS, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return processResourceSample{}, fmt.Errorf("parse schedstat runtime: %w", err)
	}
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return processResourceSample{}, err
	}
	var rssBytes uint64
	for _, line := range strings.Split(string(status), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			kb, parseErr := strconv.ParseUint(parts[1], 10, 64)
			if parseErr == nil {
				rssBytes = kb * 1024
			}
		}
		break
	}
	return processResourceSample{CPUTimeMS: float64(runtimeNS) / 1_000_000.0, RSSBytes: rssBytes}, nil
}
