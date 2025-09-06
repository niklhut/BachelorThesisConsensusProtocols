package util

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrNotInContainer = errors.New("not running inside a container")

type MetricsSample struct {
	Timestamp time.Time
	CPU       float64 // CPU percentage
	MemoryMB  float64 // Memory usage in MB
}

type MetricsCollector struct {
	interval      time.Duration
	buffer        [](*MetricsSample)
	index         int
	lastCPUUsage  uint64
	lastTimestamp time.Time
	mu            sync.RWMutex
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// NewMetricsCollector initializes a new collector and starts collection.
func NewMetricsCollector(interval time.Duration, maxSamples int) (*MetricsCollector, error) {
	if !isRunningInContainer() {
		return nil, ErrNotInContainer
	}

	mc := &MetricsCollector{
		interval: interval,
		buffer:   make([]*MetricsSample, maxSamples),
		stopCh:   make(chan struct{}),
	}

	mc.wg.Add(1)
	go mc.startCollection()

	return mc, nil
}

// isRunningInContainer checks if the process is running inside Docker/Kubernetes.
func isRunningInContainer() bool {
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}

	data, err := os.ReadFile("/proc/1/cgroup")
	if err == nil {
		content := string(data)
		return strings.Contains(content, "docker") || strings.Contains(content, "kubepods")
	}
	return false
}

// startCollection runs in a goroutine and collects metrics periodically.
func (mc *MetricsCollector) startCollection() {
	defer mc.wg.Done()

	mc.lastCPUUsage = mc.readCPUUsage()
	mc.lastTimestamp = time.Now()

	ticker := time.NewTicker(mc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-mc.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			currentCPUUsage := mc.readCPUUsage()
			memory := mc.readMemory()

			var cpuPercentage float64
			if mc.lastCPUUsage > 0 {
				cpuDelta := currentCPUUsage - mc.lastCPUUsage
				timeDelta := now.Sub(mc.lastTimestamp).Microseconds()

				if timeDelta > 0 {
					cpuPercentage = float64(cpuDelta) / float64(timeDelta) * 100
				}
			}

			sample := &MetricsSample{
				Timestamp: now,
				CPU:       cpuPercentage,
				MemoryMB:  float64(memory) / (1024.0 * 1024.0),
			}

			mc.mu.Lock()
			mc.buffer[mc.index] = sample
			mc.index = (mc.index + 1) % len(mc.buffer)
			mc.mu.Unlock()

			mc.lastCPUUsage = currentCPUUsage
			mc.lastTimestamp = now
		}
	}
}

// readCPUUsage reads CPU usage from cgroup stats in microseconds.
func (mc *MetricsCollector) readCPUUsage() uint64 {
	data, err := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return 0
	}

	lines := strings.SplitSeq(string(data), "\n")
	for line := range lines {
		if strings.HasPrefix(line, "usage_usec") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := strconv.ParseUint(parts[1], 10, 64)
				if err == nil {
					return val
				}
			}
		}
	}
	return 0
}

// readMemory reads memory usage from cgroup in bytes.
func (mc *MetricsCollector) readMemory() uint64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.current")
	if err != nil {
		return 0
	}

	val, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return val
}

// GetSamples returns samples within a given time range.
func (mc *MetricsCollector) GetSamples(start, end time.Time) []MetricsSample {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	var samples []MetricsSample
	for _, s := range mc.buffer {
		if s != nil && s.Timestamp.After(start) && s.Timestamp.Before(end) {
			samples = append(samples, *s)
		}
	}
	return samples
}

// Stop stops the metrics collection goroutine.
func (mc *MetricsCollector) Stop() {
	close(mc.stopCh)
	mc.wg.Wait()
}
