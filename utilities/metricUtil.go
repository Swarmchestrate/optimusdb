package utilities

import (
	"github.com/mackerelio/go-osstat/network"
	"github.com/shirou/gopsutil/disk"
	"runtime"
	"time"

	"github.com/mackerelio/go-osstat/cpu"
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
func GetMemoryUsage() (allocMB, totalAllocMB, sysMB float64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	allocMB = float64(m.Alloc) / 1024 / 1024
	totalAllocMB = float64(m.TotalAlloc) / 1024 / 1024
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
	before, err := network.Get()
	if err != nil {
		return
	}

	time.Sleep(1 * time.Second)

	after, err := network.Get()
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
