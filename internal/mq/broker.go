package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

// Options configures a Broker.
type Options struct {
	Dir          string        // data directory for WAL segments and offsets
	Partitions   int           // number of partitions in the topic
	Capacity     int           // records kept in memory per partition (also the backpressure bound)
	SegmentBytes int64         // WAL segment roll size
	SessionTTL   time.Duration // lease duration for streamers and consumers
}

// Broker owns the partitions, consumer groups and the streamer registry.
type Broker struct {
	opts      Options
	parts     []*Partition
	streamers *Membership
	mu        sync.Mutex
	groups    map[string]*Group
}

// Open recovers (or creates) a broker in opts.Dir.
func Open(opts Options) (*Broker, error) {
	if opts.Partitions <= 0 || opts.Capacity <= 0 || opts.SegmentBytes <= 0 {
		return nil, errors.New("mq: partitions, capacity and segment size must be positive")
	}
	b := &Broker{opts: opts, streamers: NewMembership(opts.SessionTTL), groups: map[string]*Group{}}
	for i := range opts.Partitions {
		p, err := OpenPartition(filepath.Join(opts.Dir, fmt.Sprintf("p-%03d", i)), opts.Capacity, opts.SegmentBytes)
		if err != nil {
			_ = b.Close()
			return nil, fmt.Errorf("partition %d: %w", i, err)
		}
		b.parts = append(b.parts, p)
	}
	return b, b.loadOffsets()
}

// Publish routes each message to its key's partition and appends per-partition batches.
// Delivery is at-least-once: on error the producer retries the whole batch.
func (b *Broker) Publish(msgs []Message) error {
	batches := map[int][]Message{}
	for _, m := range msgs {
		p := PartitionFor(m.Key, len(b.parts))
		batches[p] = append(batches[p], m)
	}
	for p, batch := range batches {
		if err := b.parts[p].Append(batch, b.floor(p)); err != nil {
			return err
		}
	}
	return nil
}

// floor is the lowest committed offset for partition p across groups.
func (b *Broker) floor(p int) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var f uint64
	first := true
	for _, g := range b.groups {
		if o := g.Offset(p); first || o < f {
			f, first = o, false
		}
	}
	return f
}

// Fetch long-polls partition p from offset.
func (b *Broker) Fetch(ctx context.Context, p int, offset uint64, limit int, wait time.Duration) (FetchResponse, error) {
	if p < 0 || p >= len(b.parts) {
		return FetchResponse{}, ErrNoPartition
	}
	recs, hw := b.parts[p].Fetch(ctx, offset, limit, wait)
	return FetchResponse{Records: recs, HighWatermark: hw}, nil
}

// group returns the named consumer group, creating it on first use.
func (b *Broker) group(name string) *Group {
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.groups[name]
	if !ok {
		g = newGroup(len(b.parts), b.opts.SessionTTL, nil)
		b.groups[name] = g
	}
	return g
}

// GroupHeartbeat renews a consumer's lease and returns its partition assignment.
func (b *Broker) GroupHeartbeat(group, member string) GroupState {
	return b.group(group).State(member)
}

// GroupLeave removes a consumer so its partitions are reassigned immediately.
func (b *Broker) GroupLeave(group, member string) { b.group(group).Leave(member) }

// Commit stores a consumer's progress, rejecting stale owners and offsets past the end.
func (b *Broker) Commit(group string, req CommitRequest) error {
	if req.Partition < 0 || req.Partition >= len(b.parts) {
		return ErrNoPartition
	}
	if req.Offset > b.parts[req.Partition].HighWatermark() {
		return ErrBadOffset
	}
	return b.group(group).Commit(req.Member, req.Generation, req.Partition, req.Offset)
}

// StreamerHeartbeat renews a streamer's lease and returns its shard (index of count).
func (b *Broker) StreamerHeartbeat(member string) StreamerState {
	gen, members := b.streamers.Heartbeat(member)
	return StreamerState{Generation: gen, Index: slices.Index(members, member), Count: len(members)}
}

// StreamerLeave deregisters a streamer so the others take over its shard.
func (b *Broker) StreamerLeave(member string) { b.streamers.Leave(member) }

func (b *Broker) offsetsPath() string { return filepath.Join(b.opts.Dir, "offsets.json") }

func (b *Broker) loadOffsets() error {
	data, err := os.ReadFile(b.offsetsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	var snap map[string]map[int]uint64
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("decode offsets: %w", err)
	}
	for name, offs := range snap {
		b.groups[name] = newGroup(len(b.parts), b.opts.SessionTTL, offs)
	}
	return nil
}

// Flush fsyncs every partition and atomically persists committed offsets.
// Offsets are snapshotted before the fsync so a saved offset never points past
// records that are durable on disk. Callers must not run Flush concurrently.
func (b *Broker) Flush() error {
	b.mu.Lock()
	snap := make(map[string]map[int]uint64, len(b.groups))
	for name, g := range b.groups {
		snap[name] = g.Offsets()
	}
	b.mu.Unlock()
	var errs []error
	for _, p := range b.parts {
		errs = append(errs, p.Sync())
	}
	data, err := json.Marshal(snap)
	if err == nil {
		err = writeFileAtomic(b.offsetsPath(), data)
	}
	return errors.Join(append(errs, err)...)
}

// writeFileAtomic writes via a synced temp file and rename, so a crash leaves
// either the old or the new contents, never an empty or partial file.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// FlushEvery runs Flush on an interval until ctx ends (batched fsync).
func (b *Broker) FlushEvery(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := b.Flush(); err != nil {
				slog.Error("flush failed", "err", err)
			}
		}
	}
}

// Close flushes and closes every partition.
func (b *Broker) Close() error {
	errs := []error{b.Flush()}
	for _, p := range b.parts {
		errs = append(errs, p.Close())
	}
	return errors.Join(errs...)
}
