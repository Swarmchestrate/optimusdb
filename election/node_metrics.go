package election

import (
	"context"
	"optimusdb/logger"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

/*
================================================================================
NODE METRICS COLLECTOR
================================================================================

This collects node health metrics independently of the election system.
The Raft election doesn't use these metrics (it uses term-based voting),
but the HTTP API can display them for monitoring purposes.

Key Separation:
- Raft Elections: Use term numbers and majority voting
- Node Metrics: Used only for monitoring/display

This keeps the systems decoupled and simple.
================================================================================
*/

// NodeMetricsCollector collects and stores node health metrics
type NodeMetricsCollector struct {
	mu             sync.RWMutex
	currentMetrics NodeReputation
	startTime      time.Time
	lastUpdateTime time.Time
	updateInterval time.Duration
	ctx            context.Context
	cancel         context.CancelFunc

	// Disk I/O tracking
	lastDiskRead  uint64
	lastDiskWrite uint64
	lastDiskCheck time.Time
}

// Global metrics collector
var (
	GlobalMetricsCollector *NodeMetricsCollector
	metricsCollectorMutex  sync.RWMutex
)

// InitNodeMetrics initializes the node metrics collector
func InitNodeMetrics(nodeID string) *NodeMetricsCollector {
	ctx, cancel := context.WithCancel(context.Background())

	collector := &NodeMetricsCollector{
		currentMetrics: NodeReputation{
			NodeID:          nodeID,
			Uptime:          1.0,
			LeadershipCount: 0,
		},
		startTime:      time.Now(),
		lastUpdateTime: time.Now(),
		updateInterval: 30 * time.Second, // Update every 30 seconds
		ctx:            ctx,
		cancel:         cancel,
		lastDiskCheck:  time.Now(),
	}

	// Store globally
	metricsCollectorMutex.Lock()
	GlobalMetricsCollector = collector
	metricsCollectorMutex.Unlock()

	// Start background collection
	go collector.collectMetricsPeriodically()

	logger.Info("[METRICS] Node metrics collector initialized for %s", nodeID)
	return collector
}

// collectMetricsPeriodically runs in background to collect metrics
func (nmc *NodeMetricsCollector) collectMetricsPeriodically() {
	ticker := time.NewTicker(nmc.updateInterval)
	defer ticker.Stop()

	// Collect immediately
	nmc.collectMetrics()

	for {
		select {
		case <-ticker.C:
			nmc.collectMetrics()
		case <-nmc.ctx.Done():
			return
		}
	}
}

// collectMetrics gathers current system metrics
func (nmc *NodeMetricsCollector) collectMetrics() {
	nmc.mu.Lock()
	defer nmc.mu.Unlock()

	// Update timestamp
	nmc.lastUpdateTime = time.Now()

	// 1. CPU Metrics
	if cpuPercents, err := cpu.Percent(time.Second, false); err == nil && len(cpuPercents) > 0 {
		totalCPU := cpuPercents[0]
		nmc.currentMetrics.IdleCPU = 100 - totalCPU

		// Approximate user/system split (60/40 of total usage)
		nmc.currentMetrics.UserCPU = totalCPU * 0.6
		nmc.currentMetrics.SystemCPU = totalCPU * 0.4
	}

	// 2. Memory Metrics
	if memStats, err := mem.VirtualMemory(); err == nil {
		nmc.currentMetrics.MemoryAvailable = float64(memStats.Available) / (1024 * 1024) // MB
		nmc.currentMetrics.MemorySystem = float64(memStats.Total) / (1024 * 1024)        // MB

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		nmc.currentMetrics.MemoryAllocationTotal = float64(m.Alloc) / (1024 * 1024) // MB
	}

	// 3. Disk I/O Metrics
	nmc.updateDiskIO()

	// 4. Uptime
	uptime := time.Since(nmc.startTime).Seconds()
	totalTime := time.Since(nmc.startTime).Seconds()
	if totalTime > 0 {
		nmc.currentMetrics.Uptime = uptime / totalTime // Always 1.0 for running node
	}

	// 5. Network Latency (approximate - use average from recent network stats)
	nmc.updateNetworkLatency()

	// 6. Leadership count (updated by election system)
	// This is updated separately when node becomes coordinator

	// 7. Geography score (default to 0.5 for single datacenter)
	nmc.currentMetrics.GeographyScore = 0.5

	logger.Debug("[METRICS] Updated: CPU=%.1f%%, Mem=%.1f/%.1fMB, Disk=%.2f/%.2f MB/s",
		nmc.currentMetrics.UserCPU+nmc.currentMetrics.SystemCPU,
		nmc.currentMetrics.MemoryAllocationTotal,
		nmc.currentMetrics.MemorySystem,
		nmc.currentMetrics.AvgReadMBs,
		nmc.currentMetrics.AvgWriteMBs,
	)
}

// updateDiskIO calculates average disk I/O rates
func (nmc *NodeMetricsCollector) updateDiskIO() {
	ioCounters, err := disk.IOCounters()
	if err != nil {
		return
	}

	now := time.Now()
	elapsed := now.Sub(nmc.lastDiskCheck).Seconds()

	if elapsed < 1 {
		return // Too soon
	}

	// Sum all disk I/O
	var totalRead, totalWrite uint64
	for _, counter := range ioCounters {
		totalRead += counter.ReadBytes
		totalWrite += counter.WriteBytes
	}

	// Calculate rates
	if nmc.lastDiskRead > 0 {
		readDelta := totalRead - nmc.lastDiskRead
		writeDelta := totalWrite - nmc.lastDiskWrite

		nmc.currentMetrics.AvgReadMBs = float64(readDelta) / elapsed / (1024 * 1024)   // MB/s
		nmc.currentMetrics.AvgWriteMBs = float64(writeDelta) / elapsed / (1024 * 1024) // MB/s
	}

	// Store for next calculation
	nmc.lastDiskRead = totalRead
	nmc.lastDiskWrite = totalWrite
	nmc.lastDiskCheck = now
}

// updateNetworkLatency approximates network latency from stats
func (nmc *NodeMetricsCollector) updateNetworkLatency() {
	// Get network stats
	netStats, err := net.IOCounters(false)
	if err != nil || len(netStats) == 0 {
		nmc.currentMetrics.Latency = 10.0 // Default 10ms
		return
	}

	// Approximate latency based on network activity
	// This is a rough estimate - for accurate latency you'd ping peers
	stat := netStats[0]

	if stat.PacketsRecv > 0 && stat.PacketsSent > 0 {
		// Lower latency if high packet rate (active network)
		packetsPerSec := float64(stat.PacketsRecv+stat.PacketsSent) / time.Since(nmc.startTime).Seconds()

		if packetsPerSec > 100 {
			nmc.currentMetrics.Latency = 5.0 // Fast network
		} else if packetsPerSec > 10 {
			nmc.currentMetrics.Latency = 15.0 // Normal network
		} else {
			nmc.currentMetrics.Latency = 30.0 // Slow network
		}
	} else {
		nmc.currentMetrics.Latency = 20.0 // Default
	}
}

// GetCurrentMetrics returns the latest collected metrics
func (nmc *NodeMetricsCollector) GetCurrentMetrics() NodeReputation {
	nmc.mu.RLock()
	defer nmc.mu.RUnlock()

	// Return a copy
	return nmc.currentMetrics
}

// UpdateLeadershipCount increments leadership count
func (nmc *NodeMetricsCollector) UpdateLeadershipCount() {
	nmc.mu.Lock()
	defer nmc.mu.Unlock()

	nmc.currentMetrics.LeadershipCount++
	logger.Info("[METRICS] Leadership count updated to %d", nmc.currentMetrics.LeadershipCount)
}

// SetGeographyScore sets the geography diversity score
func (nmc *NodeMetricsCollector) SetGeographyScore(score float64) {
	nmc.mu.Lock()
	defer nmc.mu.Unlock()

	nmc.currentMetrics.GeographyScore = score
}

// Stop stops the metrics collector
func (nmc *NodeMetricsCollector) Stop() {
	nmc.cancel()
	logger.Info("[METRICS] Metrics collector stopped")
}

/*
================================================================================
PUBLIC API FUNCTIONS (Updated to use metrics collector)
================================================================================
*/

// GetAllPeersReputation returns reputation for all peers
// Updated to use metrics collector instead of returning empty data
func GetAllPeersReputationWithMetrics() ([]NodeReputation, error) {
	metricsCollectorMutex.RLock()
	defer metricsCollectorMutex.RUnlock()

	if GlobalMetricsCollector == nil {
		// Return empty if not initialized (maintains compatibility)
		return []NodeReputation{}, nil
	}

	// Return current node's metrics
	// In a real implementation, you'd collect from all peers via PubSub
	metrics := GlobalMetricsCollector.GetCurrentMetrics()
	return []NodeReputation{metrics}, nil
}

// GetPeerReputationWithMetrics returns reputation for a specific peer
func GetPeerReputationWithMetrics(peerID string) (*NodeReputation, error) {
	metricsCollectorMutex.RLock()
	defer metricsCollectorMutex.RUnlock()

	if GlobalMetricsCollector == nil {
		// Return stub if not initialized (maintains compatibility)
		return &NodeReputation{NodeID: peerID}, nil
	}

	metrics := GlobalMetricsCollector.GetCurrentMetrics()

	// If requesting self, return actual metrics
	if metrics.NodeID == peerID {
		return &metrics, nil
	}

	// For other peers, return stub (would need distributed collection)
	return &NodeReputation{NodeID: peerID}, nil
}

// GetCurrentNodeHealth returns current node health score
func GetCurrentNodeHealth() float64 {
	metricsCollectorMutex.RLock()
	defer metricsCollectorMutex.RUnlock()

	if GlobalMetricsCollector == nil {
		return 0.0
	}

	metrics := GlobalMetricsCollector.GetCurrentMetrics()
	return CalculateHealthScore(metrics)
}
