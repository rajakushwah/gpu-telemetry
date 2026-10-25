package mq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rajakushwah/gpu-telemetry/internal/platform"
)

// Client talks to the broker's HTTP API. It is safe for concurrent use.
type Client struct {
	base string
	hc   *http.Client
}

// NewClient returns a client for the broker at baseURL (e.g. http://broker:9000).
func NewClient(baseURL string) *Client {
	return &Client{
		base: strings.TrimRight(baseURL, "/"),
		hc:   &http.Client{Timeout: maxWait + 15*time.Second},
	}
}

// Publish sends a batch, retrying on backpressure or transient errors until ctx
// ends; a request the broker rejects (4xx) fails immediately.
func (c *Client) Publish(ctx context.Context, msgs []Message) error {
	return platform.Retry(ctx, func() error {
		return c.do(ctx, http.MethodPost, "/v1/publish", PublishRequest{Messages: msgs}, nil)
	})
}

// Fetch reads up to limit records from partition p, waiting up to wait for new data.
func (c *Client) Fetch(ctx context.Context, p int, offset uint64, limit int, wait time.Duration) (FetchResponse, error) {
	var resp FetchResponse
	path := fmt.Sprintf("/v1/partitions/%d/records?offset=%d&max=%d&wait_ms=%d", p, offset, limit, wait.Milliseconds())
	return resp, c.do(ctx, http.MethodGet, path, nil, &resp)
}

// GroupHeartbeat renews membership in group and returns the current assignment.
func (c *Client) GroupHeartbeat(ctx context.Context, group, member string) (GroupState, error) {
	var st GroupState
	return st, c.do(ctx, http.MethodPost, "/v1/groups/"+url.PathEscape(group)+"/heartbeat", MemberRequest{member}, &st)
}

// GroupLeave leaves group so partitions rebalance immediately.
func (c *Client) GroupLeave(ctx context.Context, group, member string) error {
	return c.do(ctx, http.MethodPost, "/v1/groups/"+url.PathEscape(group)+"/leave", MemberRequest{member}, nil)
}

// Commit records progress; it returns ErrFenced if the partition moved to another member.
func (c *Client) Commit(ctx context.Context, group string, req CommitRequest) error {
	return c.do(ctx, http.MethodPost, "/v1/groups/"+url.PathEscape(group)+"/commit", req, nil)
}

// StreamerHeartbeat renews a streamer lease and returns its shard.
func (c *Client) StreamerHeartbeat(ctx context.Context, member string) (StreamerState, error) {
	var st StreamerState
	return st, c.do(ctx, http.MethodPost, "/v1/streamers/heartbeat", MemberRequest{member}, &st)
}

// StreamerLeave deregisters a streamer.
func (c *Client) StreamerLeave(ctx context.Context, member string) error {
	return c.do(ctx, http.MethodPost, "/v1/streamers/leave", MemberRequest{member}, nil)
}

// do performs one JSON request and maps broker status codes back to domain errors.
func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return ErrBackpressure
	case resp.StatusCode == http.StatusConflict:
		return ErrFenced
	case resp.StatusCode >= 300:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		err := fmt.Errorf("broker %s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(msg))
		if resp.StatusCode < 500 {
			return platform.Permanent(err)
		}
		return err
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection is reused
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
