package telemetry

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

const sample = `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-abc","NVIDIA H100 80GB HBM3","host-1","","trainer-0","ml","87.5","DCGM_FI_DRIVER_VERSION=""535.129.03"",gpu=""0"""
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","1","nvidia1","GPU-def","NVIDIA H100 80GB HBM3","host-1","","","","not-a-number",""
`

func TestReaderStampsProcessingTimeAndParsesLabels(t *testing.T) {
	r, err := NewReader(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	p, err := r.Next(now)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Timestamp.Equal(now.Truncate(time.Microsecond)) {
		t.Fatalf("timestamp %v must be the processing time", p.Timestamp)
	}
	if p.GPUUUID != "GPU-abc" || p.Value != 87.5 || p.Hostname != "host-1" {
		t.Fatalf("unexpected point %+v", p)
	}
	if p.Labels["DCGM_FI_DRIVER_VERSION"] != "535.129.03" || p.Labels["pod"] != "trainer-0" || p.Labels["namespace"] != "ml" {
		t.Fatalf("labels %v", p.Labels)
	}
	if _, err := r.Next(now); !errors.Is(err, ErrBadRow) {
		t.Fatalf("want ErrBadRow for invalid value, got %v", err)
	}
	if _, err := r.Next(now); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestReaderRejectsMissingColumns(t *testing.T) {
	if _, err := NewReader(strings.NewReader("metric_name,value\n")); err == nil {
		t.Fatal("expected missing column error")
	}
}

func TestParseLabelsHandlesEscapesAndGarbage(t *testing.T) {
	got := parseLabels(`a="x\"y",b="2",broken`)
	if got["a"] != `x"y` || got["b"] != "2" || len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	if parseLabels("") != nil {
		t.Fatal("empty input should yield nil")
	}
}
