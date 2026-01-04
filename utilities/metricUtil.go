package utilities

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network" // ADD THIS (for Connected status)
	osstatnet "github.com/mackerelio/go-osstat/network"
	"github.com/shirou/gopsutil/disk"
	"runtime"
	"time"

	"github.com/mackerelio/go-osstat/cpu"
)

// Global variables for tracking
var (
	ProcessStartTime time.Time
	NodeRegion       = "eu-south" // Configure this per node
)

// Return CPU usage percentages
func GetCPUUsage() (userPercent, systemPercent, idlePercent float64, err error) {
	before, err := cpu.Get()
	if err != nil {
		return
	}

	time.Sleep(1 * time.Second)

	after, err := cpu.Get()
	if err != nil {
		return
	}

	total := float64(after.Total - before.Total)
	userPercent = float64(after.User-before.User) / total * 100
	systemPercent = float64(after.System-before.System) / total * 100
	idlePercent = float64(after.Idle-before.Idle) / total * 100

	return
}

// Return Memory usage in MB
// Note: Removed TotalAlloc (cumulative counter) - was causing validation failures
// Now returns only current allocation and system memory
func GetMemoryUsage() (allocMB, sysMB float64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	allocMB = float64(m.Alloc) / 1024 / 1024
	sysMB = float64(m.Sys) / 1024 / 1024

	return
}

// Return disk read/write speeds in MB/s (averaged across devices)
func GetDiskUsage(interval time.Duration) (avgReadMBs, avgWriteMBs float64, err error) {
	before, err := disk.IOCounters()
	if err != nil {
		return
	}

	time.Sleep(interval)

	after, err := disk.IOCounters()
	if err != nil {
		return
	}

	var totalRead, totalWrite float64
	var deviceCount int

	for device, beforeStats := range before {
		afterStats, exists := after[device]
		if !exists {
			continue
		}
		readSpeed := float64(afterStats.ReadBytes-beforeStats.ReadBytes) / 1024 / 1024 / interval.Seconds()
		writeSpeed := float64(afterStats.WriteBytes-beforeStats.WriteBytes) / 1024 / 1024 / interval.Seconds()

		totalRead += readSpeed
		totalWrite += writeSpeed
		deviceCount++
	}

	if deviceCount > 0 {
		avgReadMBs = totalRead / float64(deviceCount)
		avgWriteMBs = totalWrite / float64(deviceCount)
	}

	return
}

// Return network RX/TX in KB/s (averaged across interfaces)
func GetNetworkUsage() (avgRxKBs, avgTxKBs float64, err error) {
	before, err := osstatnet.Get()
	if err != nil {
		return
	}

	time.Sleep(1 * time.Second)

	after, err := osstatnet.Get()
	if err != nil {
		return
	}

	var totalRx, totalTx float64
	var ifaceCount int

	for i := range before {
		if i >= len(after) {
			break
		}
		devBefore := before[i]
		devAfter := after[i]

		rxBytes := devAfter.RxBytes - devBefore.RxBytes
		txBytes := devAfter.TxBytes - devBefore.TxBytes

		totalRx += float64(rxBytes) / 1024
		totalTx += float64(txBytes) / 1024
		ifaceCount++
	}

	if ifaceCount > 0 {
		avgRxKBs = totalRx / float64(ifaceCount)
		avgTxKBs = totalTx / float64(ifaceCount)
	}

	return
}

// InitMetrics initializes uptime tracking - call once at startup
func InitMetrics() {
	ProcessStartTime = time.Now()
}

// GetActualLatency measures average RTT to connected peers in milliseconds
func GetActualLatency(h host.Host) float64 {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	peers := h.Network().Peers()
	if len(peers) == 0 {
		return 10.0 // Default when no peers
	}

	var totalLatency time.Duration
	successCount := 0

	for _, peerID := range peers {
		// Skip if not connected
		if h.Network().Connectedness(peerID) != network.Connected {
			continue
		}

		// Measure connection time as latency approximation
		start := time.Now()
		stream, err := h.NewStream(ctx, peerID, "/ipfs/ping/1.0.0")
		if err != nil {
			continue
		}
		latency := time.Since(start)
		stream.Close()

		totalLatency += latency
		successCount++
	}

	if successCount == 0 {
		return 10.0 // Fallback
	}

	avgLatency := totalLatency / time.Duration(successCount)
	return float64(avgLatency.Milliseconds())
}

// GetGeographyScore returns diversity score 0.0-1.0 based on peer distribution
// Higher = more diverse peer connections
func GetGeographyScore(h host.Host) float64 {
	peers := h.Network().Peers()
	if len(peers) == 0 {
		return 0.5 // Neutral with no peers
	}

	// Simple heuristic: more peers = higher geographic diversity
	// Assumes random peer distribution leads to diversity
	peerCount := len(peers)

	// Score increases with peer count: 1-3 peers=0.3, 4-7 peers=0.5, 8+ peers=0.7
	if peerCount >= 8 {
		return 0.7
	} else if peerCount >= 4 {
		return 0.5
	} else if peerCount >= 1 {
		return 0.3
	}

	return 0.5
}

// GetActualUptime returns uptime as 0.0-1.0 normalized to 30 days
func GetActualUptime() float64 {
	if ProcessStartTime.IsZero() {
		return 0.0
	}

	uptimeSeconds := time.Since(ProcessStartTime).Seconds()
	uptimeDays := uptimeSeconds / 86400.0 // Convert to days

	// Normalize to 30 days: 30 days = 1.0, less = proportional
	normalized := uptimeDays / 30.0

	if normalized > 1.0 {
		return 1.0 // Cap at 100%
	}

	return normalized
}

// GetUptimeFormatted returns human-readable uptime
func GetUptimeFormatted() string {
	if ProcessStartTime.IsZero() {
		return "unknown"
	}

	duration := time.Since(ProcessStartTime)
	days := int(duration.Hours() / 24)
	hours := int(duration.Hours()) % 24
	minutes := int(duration.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	} else if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}
