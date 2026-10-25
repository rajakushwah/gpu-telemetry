// Package telemetry defines the GPU telemetry data point and parses DCGM CSV exports.
package telemetry

import (
	"errors"
	"time"
)

// Point is one GPU metric sample as carried on the queue and returned by the API.
type Point struct {
	Timestamp time.Time         `json:"timestamp"`
	GPUUUID   string            `json:"gpu_uuid"`
	GPUID     string            `json:"gpu_id"`
	Device    string            `json:"device,omitempty"`
	ModelName string            `json:"model_name,omitempty"`
	Hostname  string            `json:"hostname"`
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Validate reports whether the point has the fields needed to store and query it.
func (p Point) Validate() error {
	switch {
	case p.GPUUUID == "":
		return errors.New("missing gpu uuid")
	case p.Metric == "":
		return errors.New("missing metric")
	case p.Timestamp.IsZero():
		return errors.New("missing timestamp")
	}
	return nil
}
