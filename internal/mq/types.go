// Package mq is the custom message queue: partitioned, WAL-backed logs,
// consumer groups with fenced offset commits, a streamer shard registry,
// and an HTTP/JSON transport with a matching client.
package mq

import (
	"encoding/json"
	"errors"
	"hash/fnv"
)

// Errors shared by the broker, the HTTP transport and the client.
var (
	ErrBackpressure = errors.New("mq: partition full, consumers are lagging")
	ErrFenced       = errors.New("mq: stale generation or partition not owned")
	ErrNoPartition  = errors.New("mq: unknown partition")
	ErrBadOffset    = errors.New("mq: offset beyond high watermark")
)

// Message is a producer payload; Key selects the partition, so per-key order is preserved.
type Message struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// Record is a Message stored at an offset in a partition.
type Record struct {
	Offset uint64 `json:"offset"`
	Message
}

// PartitionFor maps key to one of n buckets with FNV-1a (stable across processes).
func PartitionFor(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

// Wire types for the HTTP transport.
type (
	PublishRequest struct {
		Messages []Message `json:"messages"`
	}
	FetchResponse struct {
		Records       []Record `json:"records"`
		HighWatermark uint64   `json:"high_watermark"`
	}
	MemberRequest struct {
		Member string `json:"member"`
	}
	GroupState struct {
		Generation uint64         `json:"generation"`
		Partitions []int          `json:"partitions"`
		Offsets    map[int]uint64 `json:"offsets"`
	}
	CommitRequest struct {
		Member     string `json:"member"`
		Generation uint64 `json:"generation"`
		Partition  int    `json:"partition"`
		Offset     uint64 `json:"offset"`
	}
	StreamerState struct {
		Generation uint64 `json:"generation"`
		Index      int    `json:"index"`
		Count      int    `json:"count"`
	}
)
