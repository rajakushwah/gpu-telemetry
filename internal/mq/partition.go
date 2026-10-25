package mq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Partition is an append-only log. A bounded in-memory ring serves reads and
// a WAL provides durability; [start, next) is the readable offset range.
type Partition struct {
	mu     sync.Mutex
	ring   []Record
	start  uint64
	next   uint64
	notify chan struct{} // closed on every append to wake long-polling readers
	wal    *wal
}

// OpenPartition recovers a partition from dir, keeping up to capacity records in memory.
func OpenPartition(dir string, capacity int, segBytes int64) (*Partition, error) {
	p := &Partition{ring: make([]Record, capacity), notify: make(chan struct{})}
	w, err := openWAL(dir, segBytes, p.store)
	if err != nil {
		return nil, err
	}
	p.wal = w
	if base := w.lastBase(); p.next < base { // newest segment is empty: resume at its base
		p.start, p.next = base, base
	}
	return p, nil
}

// store places r in the ring, evicting the oldest record when full.
func (p *Partition) store(r Record) {
	if p.start == p.next {
		p.start = r.Offset
	}
	p.ring[r.Offset%uint64(len(p.ring))] = r
	p.next = r.Offset + 1
	if p.next-p.start > uint64(len(p.ring)) {
		p.start = p.next - uint64(len(p.ring))
	}
}

// Append durably stores msgs. It returns ErrBackpressure instead of evicting
// records at or above floor (the lowest offset a consumer group still needs).
func (p *Partition) Append(msgs []Message, floor uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	floor = min(max(floor, p.start), p.next)
	if p.next+uint64(len(msgs))-floor > uint64(len(p.ring)) {
		return ErrBackpressure
	}
	recs := make([]Record, len(msgs))
	for i, m := range msgs {
		recs[i] = Record{Offset: p.next + uint64(i), Message: m}
	}
	if err := p.wal.append(recs); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	for _, r := range recs {
		p.store(r)
	}
	p.wal.dropBefore(p.start)
	close(p.notify)
	p.notify = make(chan struct{})
	return nil
}

// Fetch returns up to limit records from offset, long-polling up to wait for
// new data. It also returns the high watermark (next offset to be written).
func (p *Partition) Fetch(ctx context.Context, offset uint64, limit int, wait time.Duration) ([]Record, uint64) {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		p.mu.Lock()
		if offset < p.next || ctx.Err() != nil {
			defer p.mu.Unlock()
			return p.read(offset, limit), p.next
		}
		ch := p.notify
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}
}

// read copies records out of the ring; offsets below start are clamped forward.
func (p *Partition) read(offset uint64, limit int) []Record {
	offset = min(max(offset, p.start), p.next)
	out := make([]Record, min(uint64(limit), p.next-offset))
	for i := range out {
		out[i] = p.ring[(offset+uint64(i))%uint64(len(p.ring))]
	}
	return out
}

// HighWatermark is the offset the next appended record will receive.
func (p *Partition) HighWatermark() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.next
}

// Sync fsyncs pending writes.
func (p *Partition) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wal.sync()
}

// Close syncs and closes the WAL.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wal.close()
}
