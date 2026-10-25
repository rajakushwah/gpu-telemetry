package mq

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestMembershipGenerationAndExpiry(t *testing.T) {
	now := time.Unix(0, 0)
	m := NewMembership(time.Second)
	m.now = func() time.Time { return now }

	g1, _ := m.Heartbeat("a")
	g2, members := m.Heartbeat("b")
	if g2 != g1+1 || !slices.Equal(members, []string{"a", "b"}) {
		t.Fatalf("join: gen %d->%d members %v", g1, g2, members)
	}
	if g3, _ := m.Heartbeat("a"); g3 != g2 {
		t.Fatal("renewal must not bump generation")
	}
	now = now.Add(2 * time.Second)
	g4, members := m.Heartbeat("a") // b's lease expired
	if g4 <= g2 || !slices.Equal(members, []string{"a"}) {
		t.Fatalf("expiry: gen %d members %v", g4, members)
	}
}

func TestAssignCoversEveryPartitionOnce(t *testing.T) {
	members := []string{"a", "b", "c"}
	seen := map[int]int{}
	for _, m := range members {
		for _, p := range Assign(16, members, m) {
			seen[p]++
		}
	}
	for p := range 16 {
		if seen[p] != 1 {
			t.Fatalf("partition %d owned %d times", p, seen[p])
		}
	}
}

func TestGroupCommitIsFenced(t *testing.T) {
	g := newGroup(4, time.Minute, nil)
	st := g.State("a")
	if err := g.Commit("a", st.Generation, 1, 42); err != nil {
		t.Fatal(err)
	}
	g.State("b") // rebalance: b joins, generation changes
	if err := g.Commit("a", st.Generation, 1, 50); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale commit accepted: %v", err)
	}
	if g.Offset(1) != 42 {
		t.Fatal("fenced commit must not change offsets")
	}
}

func TestBrokerPersistsOffsets(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Partitions: 2, Capacity: 10, SegmentBytes: 1 << 20, SessionTTL: time.Minute}
	b, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(msgs(3)); err != nil {
		t.Fatal(err)
	}
	p := PartitionFor("k", 2)
	st := b.GroupHeartbeat("g", "m")
	if err := b.Commit("g", CommitRequest{Member: "m", Generation: st.Generation, Partition: p, Offset: 3}); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()

	b, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if got := b.GroupHeartbeat("g", "m").Offsets[p]; got != 3 {
		t.Fatalf("offset after restart %d, want 3", got)
	}
}
