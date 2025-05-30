package sfu

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils"
)

type ForwardStats struct {
	lock           sync.Mutex
	lastLeftNano   atomic.Int64
	latency        *utils.LatencyAggregate
	captureLatency *utils.LatencyAggregate
	closeCh        chan struct{}
}

func NewForwardStats(latencyUpdateInterval, reportInterval, latencyWindowLength time.Duration) *ForwardStats {
	s := &ForwardStats{
		latency:        utils.NewLatencyAggregate(latencyUpdateInterval, latencyWindowLength),
		captureLatency: utils.NewLatencyAggregate(latencyUpdateInterval, latencyWindowLength),
		closeCh:        make(chan struct{}),
	}

	go s.report(reportInterval)
	return s
}

func (s *ForwardStats) Update(arrival, left, capture int64) {
	transit := left - arrival

	// ignore if transit is too large or negative, this could happen if system time is adjusted
	if transit < 0 || time.Duration(transit) > 5*time.Second {
		return
	}
	lastLeftNano := s.lastLeftNano.Load()
	if left < lastLeftNano || !s.lastLeftNano.CompareAndSwap(lastLeftNano, left) {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	s.latency.Update(time.Duration(arrival), float64(transit))
	if capture != 0 {
		captureTransit := left - capture
		if captureTransit >= 0 && time.Duration(captureTransit) <= 5*time.Second {
			s.captureLatency.Update(time.Duration(capture), float64(captureTransit))
		}
	}
}

func (s *ForwardStats) GetStats() (latency, jitter time.Duration) {
	s.lock.Lock()
	w := s.latency.Summarize()
	s.lock.Unlock()
	latency, jitter = time.Duration(w.Mean()), time.Duration(w.StdDev())
	// TODO: remove this check after debugging unexpected jitter issue
	if jitter > 10*time.Second {
		logger.Infow("unexpected forward jitter",
			"jitter", jitter,
			"stats", fmt.Sprintf("count %.2f, mean %.2f, stdDev %.2f", w.Count(), w.Mean(), w.StdDev()),
		)
	}
	return
}

func (s *ForwardStats) GetCaptureStats() (latency, jitter time.Duration) {
	s.lock.Lock()
	w := s.captureLatency.Summarize()
	s.lock.Unlock()
	latency, jitter = time.Duration(w.Mean()), time.Duration(w.StdDev())
	return
}

func (s *ForwardStats) GetLastCaptureStats(duration time.Duration) (latency, jitter time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	w := s.captureLatency.SummarizeLast(duration)
	return time.Duration(w.Mean()), time.Duration(w.StdDev())
}

func (s *ForwardStats) GetLastStats(duration time.Duration) (latency, jitter time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	w := s.latency.SummarizeLast(duration)
	return time.Duration(w.Mean()), time.Duration(w.StdDev())
}

func (s *ForwardStats) Stop() {
	close(s.closeCh)
}

func (s *ForwardStats) report(reportInterval time.Duration) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			latency, jitter := s.GetLastStats(reportInterval)
			latencySlow, jitterSlow := s.GetStats()
			captureLatency, captureJitter := s.GetLastCaptureStats(reportInterval)
			captureLatencySlow, captureJitterSlow := s.GetCaptureStats()

			logger.Infow("forward latency", "latency_ms", latency.Milliseconds(), "jitter_ms", jitter.Milliseconds(),
				"latency_ms_slow", latencySlow.Milliseconds(), "jitter_ms_slow", jitterSlow.Milliseconds(),
				"capture_latency_ms", captureLatency.Milliseconds(), "capture_jitter_ms", captureJitter.Milliseconds(),
				"capture_latency_ms_slow", captureLatencySlow.Milliseconds(), "capture_jitter_ms_slow", captureJitterSlow.Milliseconds())

			prometheus.RecordForwardJitter(uint32(jitter/time.Millisecond), uint32(jitterSlow/time.Millisecond))
			prometheus.RecordForwardLatency(uint32(latency/time.Millisecond), uint32(latencySlow/time.Millisecond))
		}
	}
}
