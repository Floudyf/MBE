package v5

// Go runtime CPU metric is process-wide, NOT a per-thread clock.
// When the sender is active, it includes the sender's CPU use as well.
import (
    "runtime"
    "runtime/metrics"
)

func mtOverlapCPUSeconds() float64 {
    samples := []metrics.Sample{{Name: "/cpu/classes/user:cpu-seconds"}}
    metrics.Read(samples)
    if samples[0].Value.Kind() == metrics.KindFloat64 {
        return samples[0].Value.Float64()
    }
    return -1
}
func mtOverlapMaxProcs() int { return runtime.GOMAXPROCS(0) }
func mtOverlapNumCPU() int { return runtime.NumCPU() }
