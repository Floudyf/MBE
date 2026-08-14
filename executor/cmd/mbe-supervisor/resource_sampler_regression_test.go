package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestResourceSamplerAggregatesNodeProcessesAndWritesBufferedArtifacts(t *testing.T) {
	calls := 0
	probe := func(pid int) (processResourceSample, error) {
		calls++
		return processResourceSample{CPUTimeMS: float64(pid * 10), RSSBytes: uint64(pid * 1000)}, nil
	}
	s := newResourceSampler([]v5NodeProcess{{NodeID: "n1", PID: 1}, {NodeID: "n2", PID: 2}}, time.Hour, probe)
	sample := s.capture()
	if sample.SampledProcessCount != 2 || sample.FailedProcessCount != 0 {
		t.Fatalf("unexpected process counts: %+v", sample)
	}
	if sample.ClusterCPUTimeMS != 30 || sample.ClusterRSSBytes != 3000 {
		t.Fatalf("unexpected aggregate: %+v", sample)
	}
	if calls != 2 {
		t.Fatalf("probe calls=%d want 2", calls)
	}

	dir := t.TempDir()
	s.writeArtifacts(dir)
	path := filepath.Join(dir, "resource_usage_timeseries.csv")
	handle, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	rows, err := csv.NewReader(handle).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%d want 2", len(rows))
	}
	if got, _ := strconv.Atoi(rows[1][4]); got != 2 {
		t.Fatalf("expected_process_count=%d want 2", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "resource_sampler_summary.json")); err != nil {
		t.Fatal(err)
	}
}

func TestResourceSamplerCanBeDisabledForObserverImpactDiagnostics(t *testing.T) {
	t.Setenv("MBE_V5_RESOURCE_SAMPLER", "off")
	if sampler := startResourceSampler([]v5NodeProcess{{NodeID: "n1", PID: 1}}, time.Millisecond); sampler != nil {
		t.Fatal("expected disabled sampler to return nil")
	}
}
