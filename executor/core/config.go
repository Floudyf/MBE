package core

import (
	"os"
	"regexp"
	"strconv"
	"strings"
)

const defaultShardCount = 4

// ReplayConfig is the executor-owned, documented subset of experiment config.
// It deliberately keeps lightweight parsing while centralising all config reads.
type ReplayConfig struct {
	StateShardCount      int
	ExecutionShardCount  int
	BlockSize            int
	BlockIntervalMS      float64
	FinalityDelayMS      float64
	RemoteFetchLatencyMS float64
}

// LoadReplayConfig reads fields required by the compatible V0/V1.2 replay path.
func LoadReplayConfig(path string) (ReplayConfig, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return ReplayConfig{}, err
	}
	text := string(contents)
	return ReplayConfig{
		StateShardCount:      configSectionPositiveInt(text, "state_sharding", "shard_count", defaultShardCount),
		ExecutionShardCount:  configSectionPositiveInt(text, "execution_sharding", "shard_count", defaultShardCount),
		BlockSize:            configPositiveInt(text, "block_size", 1),
		BlockIntervalMS:      configNonNegativeFloat(text, "block_interval_ms", 0),
		FinalityDelayMS:      configNonNegativeFloat(text, "finality_delay_ms", 1),
		RemoteFetchLatencyMS: configNonNegativeFloat(text, "remote_fetch_latency_ms", 0),
	}, nil
}

func configSectionPositiveInt(contents, section, field string, fallback int) int {
	value := configSectionNonNegativeFloat(contents, section, field, float64(fallback))
	if value <= 0 {
		return fallback
	}
	return int(value)
}

func configSectionNonNegativeFloat(contents, section, field string, fallback float64) float64 {
	lines := strings.Split(contents, "\n")
	sectionPrefix := section + ":"
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, sectionPrefix) {
			continue
		}
		if value, found := configLineFloat(trimmed, field); found {
			return value
		}
		for _, candidate := range lines[index+1:] {
			if len(candidate) > 0 && candidate[0] != ' ' && candidate[0] != '\t' {
				break
			}
			if value, found := configLineFloat(strings.TrimSpace(candidate), field); found {
				return value
			}
		}
		break
	}
	return fallback
}

func configLineFloat(line, field string) (float64, bool) {
	matches := regexp.MustCompile(regexp.QuoteMeta(field) + `:\s*([0-9]+(?:\.[0-9]+)?)`).FindStringSubmatch(line)
	if len(matches) != 2 {
		return 0, false
	}
	value, err := strconv.ParseFloat(matches[1], 64)
	return value, err == nil && value >= 0
}

func configPositiveInt(contents, field string, fallback int) int {
	value := configNonNegativeFloat(contents, field, float64(fallback))
	if value <= 0 {
		return fallback
	}
	return int(value)
}

func configNonNegativeFloat(contents, field string, fallback float64) float64 {
	matches := regexp.MustCompile(regexp.QuoteMeta(field) + `:\s*([0-9]+(?:\.[0-9]+)?)`).FindStringSubmatch(contents)
	if len(matches) != 2 {
		return fallback
	}
	value, err := strconv.ParseFloat(matches[1], 64)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}
