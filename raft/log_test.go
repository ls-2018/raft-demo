package raft

import (
	"bytes"
	"fmt"
	"github.com/armon/go-metrics"
	"testing"
)

func TestOldestLog(t *testing.T) {
	cases := []struct {
		Name    string
		Logs    []*Log
		WantIdx uint64
		WantErr bool
	}{
		{
			Name:    "empty logs",
			Logs:    nil,
			WantIdx: 0,
			WantErr: true,
		},
		{
			Name: "simple case",
			Logs: []*Log{
				&Log{
					Index: 1234,
					Term:  1,
				},
				&Log{
					Index: 1235,
					Term:  1,
				},
				&Log{
					Index: 1236,
					Term:  2,
				},
			},
			WantIdx: 1234,
			WantErr: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			s := NewInmemStore()
			if err := s.StoreLogs(tc.Logs); err != nil {
				t.Fatalf("expected store logs not to fail: %s", err)
			}

			got, err := oldestLog(s)
			switch {
			case tc.WantErr && err == nil:
				t.Fatalf("wanted error got nil")
			case !tc.WantErr && err != nil:
				t.Fatalf("wanted no error got: %s", err)
			}

			if got.Index != tc.WantIdx {
				t.Fatalf("got index %v, want %v", got.Index, tc.WantIdx)
			}
		})
	}
}

func getCurrentGaugeValue(t *testing.T, sink *metrics.InmemSink, name string) float32 {
	t.Helper()

	data := sink.Data()

	// Loop backward through intervals until there is a non-empty one
	// Addresses flakiness around recording to one interval but accessing during the next
	for i := len(data) - 1; i >= 0; i-- {
		currentInterval := data[i]

		currentInterval.RLock()
		if gv, ok := currentInterval.Gauges[name]; ok {
			currentInterval.RUnlock()
			return gv.Value
		}
		currentInterval.RUnlock()
	}

	// Debug print all the gauges
	buf := bytes.NewBuffer(nil)
	for _, intv := range data {
		intv.RLock()
		for name, val := range intv.Gauges {
			fmt.Fprintf(buf, "[%v][G] '%s': %0.3f\n", intv.Interval, name, val.Value)
		}
		intv.RUnlock()
	}
	t.Log(buf.String())

	t.Fatalf("didn't find gauge %q", name)
	return 0
}
