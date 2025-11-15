package contextualmetadata

import (
	"sync"
	"time"
)

type EnrichmentMetrics struct {
	mu               sync.RWMutex
	TotalEnrichments int64
	SuccessfulLLM    int64
	FailedLLM        int64
	FallbackToBasic  int64
	AvgProfileTime   time.Duration
	AvgLLMLatency    time.Duration
	AvgTotalTime     time.Duration
}

var globalMetrics = &EnrichmentMetrics{}

func (m *EnrichmentMetrics) RecordEnrichment(profileTime, llmTime, totalTime time.Duration, llmSuccess bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalEnrichments++
	if llmSuccess {
		m.SuccessfulLLM++
	} else {
		m.FallbackToBasic++
	}

	// Update averages (simple moving average)
	m.AvgProfileTime = (m.AvgProfileTime*time.Duration(m.TotalEnrichments-1) + profileTime) / time.Duration(m.TotalEnrichments)
	m.AvgLLMLatency = (m.AvgLLMLatency*time.Duration(m.TotalEnrichments-1) + llmTime) / time.Duration(m.TotalEnrichments)
	m.AvgTotalTime = (m.AvgTotalTime*time.Duration(m.TotalEnrichments-1) + totalTime) / time.Duration(m.TotalEnrichments)
}

func GetMetrics() EnrichmentMetrics {
	globalMetrics.mu.RLock()
	defer globalMetrics.mu.RUnlock()
	return *globalMetrics
}
