package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const resourceSamplerSchemaVersion = "mbe_v5_resource_sampler_v1"

type processResourceSample struct {
	CPUTimeMS float64
	RSSBytes  uint64
}

type resourceProbeFunc func(pid int) (processResourceSample, error)

type clusterResourceSample struct {
	TimestampMS          int64
	ClusterCPUTimeMS     float64
	ClusterRSSBytes      uint64
	SampledProcessCount  int
	ExpectedProcessCount int
	FailedProcessCount   int
}

type resourceSampler struct {
	processes []v5NodeProcess
	interval  time.Duration
	probe     resourceProbeFunc
	mu        sync.Mutex
	samples   []clusterResourceSample
	errors    []string
	stop      chan struct{}
	done      chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	writeOnce sync.Once
}

func startResourceSampler(processes []v5NodeProcess, interval time.Duration) *resourceSampler {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("MBE_V5_RESOURCE_SAMPLER")))
	if mode == "0" || mode == "off" || mode == "false" || mode == "disabled" {
		return nil
	}
	s := newResourceSampler(processes, interval, sampleProcessResource)
	s.Start()
	return s
}

func newResourceSampler(processes []v5NodeProcess, interval time.Duration, probe resourceProbeFunc) *resourceSampler {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if probe == nil {
		probe = sampleProcessResource
	}
	return &resourceSampler{
		processes: append([]v5NodeProcess(nil), processes...),
		interval:  interval,
		probe:     probe,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (s *resourceSampler) Start() {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		go func() {
			defer close(s.done)
			s.capture()
			ticker := time.NewTicker(s.interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					s.capture()
				case <-s.stop:
					s.capture()
					return
				}
			}
		}()
	})
}

func (s *resourceSampler) capture() clusterResourceSample {
	sample := clusterResourceSample{
		TimestampMS:          time.Now().UnixMilli(),
		ExpectedProcessCount: len(s.processes),
	}
	for _, process := range s.processes {
		value, err := s.probe(process.PID)
		if err != nil {
			sample.FailedProcessCount++
			s.mu.Lock()
			if len(s.errors) < 32 {
				s.errors = append(s.errors, fmt.Sprintf("%s(pid=%d): %v", process.NodeID, process.PID, err))
			}
			s.mu.Unlock()
			continue
		}
		sample.SampledProcessCount++
		sample.ClusterCPUTimeMS += value.CPUTimeMS
		sample.ClusterRSSBytes += value.RSSBytes
	}
	s.mu.Lock()
	s.samples = append(s.samples, sample)
	s.mu.Unlock()
	return sample
}

func (s *resourceSampler) StopAndWrite(dataDir string) {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() { close(s.stop) })
	<-s.done
	s.writeOnce.Do(func() { s.writeArtifacts(dataDir) })
}

func (s *resourceSampler) writeArtifacts(dataDir string) {
	s.mu.Lock()
	samples := append([]clusterResourceSample(nil), s.samples...)
	errorsCopy := append([]string(nil), s.errors...)
	s.mu.Unlock()

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return
	}
	csvPath := filepath.Join(dataDir, "resource_usage_timeseries.csv")
	if handle, err := os.Create(csvPath); err == nil {
		writer := csv.NewWriter(handle)
		_ = writer.Write([]string{"timestamp_ms", "cluster_cpu_time_ms", "cluster_rss_bytes", "sampled_process_count", "expected_process_count", "failed_process_count"})
		for _, item := range samples {
			_ = writer.Write([]string{
				strconv.FormatInt(item.TimestampMS, 10),
				strconv.FormatFloat(item.ClusterCPUTimeMS, 'f', 3, 64),
				strconv.FormatUint(item.ClusterRSSBytes, 10),
				strconv.Itoa(item.SampledProcessCount),
				strconv.Itoa(item.ExpectedProcessCount),
				strconv.Itoa(item.FailedProcessCount),
			})
		}
		writer.Flush()
		_ = handle.Close()
	}

	samplingAvailable := false
	for _, item := range samples {
		if item.SampledProcessCount > 0 {
			samplingAvailable = true
			break
		}
	}
	payload := map[string]any{
		"schema_version":         resourceSamplerSchemaVersion,
		"sampling_available":     samplingAvailable,
		"sample_interval_ms":     s.interval.Milliseconds(),
		"sample_count":           len(samples),
		"expected_process_count": len(s.processes),
		"process_scope":          "validator_node_processes_only",
		"write_policy":           "memory_buffer_then_post_window_write",
		"failure_policy":         "fail_open_observer_only",
		"errors":                 errorsCopy,
	}
	if len(errorsCopy) > 0 {
		payload["sampling_error"] = strings.Join(errorsCopy, " | ")
	}
	if encoded, err := json.MarshalIndent(payload, "", "  "); err == nil {
		_ = os.WriteFile(filepath.Join(dataDir, "resource_sampler_summary.json"), append(encoded, '\n'), 0o644)
	}
}
