package raftboltdb

import (
	"context"
	"time"

	"github.com/boltdb/bolt"
)

const (
	defaultMetricsInterval = 5 * time.Second
)

// RunMetrics should be executed in a go routine and will periodically emit
// metrics on the given interval until the context has been cancelled.
func (b *BoltStore) RunMetrics(ctx context.Context, interval time.Duration) {
	if interval == 0 {
		interval = defaultMetricsInterval
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()

	stats := b.emitMetrics(nil)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			stats = b.emitMetrics(stats)
		}
	}
}

func (b *BoltStore) emitMetrics(prev *bolt.Stats) *bolt.Stats {
	newStats := b.conn.Stats()
	return &newStats
}
