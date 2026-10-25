package mq

import (
	"maps"
	"slices"
	"sync"
	"time"
)

// Membership tracks live members by TTL lease and bumps the generation
// whenever the member set changes. It backs consumer groups and the streamer registry.
type Membership struct {
	mu   sync.Mutex
	ttl  time.Duration
	seen map[string]time.Time
	gen  uint64
	now  func() time.Time
}

// NewMembership returns an empty membership whose leases expire after ttl.
func NewMembership(ttl time.Duration) *Membership {
	return &Membership{ttl: ttl, seen: map[string]time.Time{}, now: time.Now}
}

// Heartbeat creates or renews id's lease and returns the generation and sorted members.
func (m *Membership) Heartbeat(id string) (uint64, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expire()
	if _, ok := m.seen[id]; !ok {
		m.gen++
	}
	m.seen[id] = m.now()
	return m.gen, m.members()
}

// Leave removes id immediately so a graceful shutdown rebalances without waiting for the TTL.
func (m *Membership) Leave(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.seen[id]; ok {
		delete(m.seen, id)
		m.gen++
	}
}

// Snapshot returns the current generation and members after expiring stale leases.
func (m *Membership) Snapshot() (uint64, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expire()
	return m.gen, m.members()
}

func (m *Membership) expire() {
	now := m.now()
	for id, t := range m.seen {
		if now.Sub(t) > m.ttl {
			delete(m.seen, id)
			m.gen++
		}
	}
}

func (m *Membership) members() []string {
	return slices.Sorted(maps.Keys(m.seen))
}

// Assign returns the partitions owned by id under round-robin over sorted members.
func Assign(partitions int, members []string, id string) []int {
	out := []int{}
	idx := slices.Index(members, id)
	if idx < 0 {
		return out
	}
	for p := idx; p < partitions; p += len(members) {
		out = append(out, p)
	}
	return out
}

// Group is a consumer group: membership plus committed offsets, with commits fenced by generation.
// mu serialises membership changes with commits, so a rebalance can't slip between
// a commit's ownership check and its offset write.
type Group struct {
	*Membership
	partitions int
	mu         sync.Mutex
	offsets    map[int]uint64
}

func newGroup(partitions int, ttl time.Duration, offsets map[int]uint64) *Group {
	if offsets == nil {
		offsets = map[int]uint64{}
	}
	return &Group{Membership: NewMembership(ttl), partitions: partitions, offsets: offsets}
}

// State renews member's lease and returns its assignment with the offsets to resume from.
func (g *Group) State(member string) GroupState {
	g.mu.Lock()
	defer g.mu.Unlock()
	gen, members := g.Heartbeat(member)
	parts := Assign(g.partitions, members, member)
	offs := make(map[int]uint64, len(parts))
	for _, p := range parts {
		offs[p] = g.offsets[p]
	}
	return GroupState{Generation: gen, Partitions: parts, Offsets: offs}
}

// Commit stores offset for partition p only if member owns p in the current generation.
func (g *Group) Commit(member string, gen uint64, p int, offset uint64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	cur, members := g.Snapshot()
	if gen != cur || !slices.Contains(Assign(g.partitions, members, member), p) {
		return ErrFenced
	}
	g.offsets[p] = offset
	return nil
}

// Leave removes member under mu, like every other membership change in a group.
func (g *Group) Leave(member string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Membership.Leave(member)
}

// Offset is the committed offset for p (0 if never committed).
func (g *Group) Offset(p int) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.offsets[p]
}

// Offsets returns a copy of all committed offsets.
func (g *Group) Offsets() map[int]uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return maps.Clone(g.offsets)
}
