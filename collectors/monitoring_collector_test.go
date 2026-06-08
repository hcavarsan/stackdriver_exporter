// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collectors

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/monitoring/v3"
	"google.golang.org/api/option"
)

func newTestMonitoringService(t *testing.T, handler http.HandlerFunc) *monitoring.Service {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	service, err := monitoring.NewService(context.Background(),
		option.WithEndpoint(server.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("creating test monitoring service: %v", err)
	}
	return service
}

func writeJSONResponse(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Errorf("encoding test response: %v", err)
	}
}

// Fake delta stores, we can't use the real ones because the delta package
// imports collectors.
type noopCounterStore struct{}

func (s *noopCounterStore) Increment(*monitoring.MetricDescriptor, *ConstMetric) {}

func (s *noopCounterStore) ListMetrics(string) []*ConstMetric { return nil }

type noopHistogramStore struct{}

func (s *noopHistogramStore) Increment(*monitoring.MetricDescriptor, *HistogramMetric) {}

func (s *noopHistogramStore) ListMetrics(string) []*HistogramMetric { return nil }

func TestIsGoogleMetric(t *testing.T) {
	good := []string{
		"pubsub.googleapis.com/some/metric",
	}

	bad := []string{
		"my.metric/a/b",
		"my.metrics/pubsub.googleapis.com/a",
	}

	for _, e := range good {
		if !isGoogleMetric(e) {
			t.Errorf("should be a google metric: %s", e)
		}
	}

	for _, e := range bad {
		if isGoogleMetric(e) {
			t.Errorf("should not be a google metric: %s", e)
		}
	}
}

func TestParseMetricExtraFilters(t *testing.T) {
	input := []string{
		"pubsub.googleapis.com/subscription:resource.labels.subscription_id=monitoring.regex.full_match(\"my-subs-prefix.*\")",
		"missing-separator",
		"compute.googleapis.com/instance:metric.labels.instance_name=\"example:vm\"",
	}

	got := ParseMetricExtraFilters(input)
	want := []MetricFilter{
		{
			TargetedMetricPrefix: "pubsub.googleapis.com/subscription",
			FilterQuery:          "resource.labels.subscription_id=monitoring.regex.full_match(\"my-subs-prefix.*\")",
		},
		{
			TargetedMetricPrefix: "compute.googleapis.com/instance",
			FilterQuery:          "metric.labels.instance_name=\"example:vm\"",
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseMetricExtraFilters() = %#v, want %#v", got, want)
	}
}

func TestSplitExtraFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		wantPrefix string
		wantFilter string
	}{
		{
			name:       "incomplete filter returns empty",
			input:      "This_is__a-MetricName.Example/with/no/filter",
			wantPrefix: "",
			wantFilter: "",
		},
		{
			name:       "basic filter",
			input:      "This_is__a-MetricName.Example/with:filter.name=filter_value",
			wantPrefix: "This_is__a-MetricName.Example/with",
			wantFilter: "filter.name=filter_value",
		},
		{
			name:       "filter value containing the separator",
			input:      `This_is__a-MetricName.Example/with:filter.name="filter:value"`,
			wantPrefix: "This_is__a-MetricName.Example/with",
			wantFilter: `filter.name="filter:value"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotPrefix, gotFilter := splitExtraFilter(tt.input, ":")
			if gotPrefix != tt.wantPrefix || gotFilter != tt.wantFilter {
				t.Fatalf("splitExtraFilter() = (%q, %q), want (%q, %q)", gotPrefix, gotFilter, tt.wantPrefix, tt.wantFilter)
			}
		})
	}
}

func TestProjectResource(t *testing.T) {
	t.Parallel()

	if got := projectResource("fake-project-1"); got != "projects/fake-project-1" {
		t.Fatalf("projectResource() = %q, want %q", got, "projects/fake-project-1")
	}
}

func TestReportMonitoringMetricsDeduplicatesDescriptorsAcrossPages(t *testing.T) {
	t.Parallel()

	const (
		projectID  = "test-project"
		metricType = "pubsub.googleapis.com/subscription/num_undelivered_messages"
	)

	var (
		descriptorPages   atomic.Int32
		timeSeriesQueries atomic.Int32
	)

	endTime := time.Now().UTC().Format(time.RFC3339Nano)

	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/metricDescriptors"):
			descriptorPages.Add(1)
			// Same type on both pages
			resp := &monitoring.ListMetricDescriptorsResponse{
				MetricDescriptors: []*monitoring.MetricDescriptor{
					{Type: metricType, MetricKind: "GAUGE", ValueType: "DOUBLE", Unit: "1"},
				},
			}
			if r.URL.Query().Get("pageToken") == "" {
				resp.NextPageToken = "page-2"
			}
			writeJSONResponse(t, w, resp)
		case strings.HasSuffix(r.URL.Path, "/timeSeries"):
			timeSeriesQueries.Add(1)
			value := 1.0
			resp := &monitoring.ListTimeSeriesResponse{
				TimeSeries: []*monitoring.TimeSeries{
					{
						Metric:     &monitoring.Metric{Type: metricType, Labels: map[string]string{"subscription_id": "subscription-a"}},
						Resource:   &monitoring.MonitoredResource{Type: "pubsub_subscription", Labels: map[string]string{"project_id": projectID}},
						MetricKind: "GAUGE",
						ValueType:  "DOUBLE",
						Points: []*monitoring.Point{
							{
								Interval: &monitoring.TimeInterval{EndTime: endTime},
								Value:    &monitoring.TypedValue{DoubleValue: &value},
							},
						},
					},
				},
			}
			writeJSONResponse(t, w, resp)
		default:
			t.Errorf("unexpected request path: %s", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusNotFound)
		}
	}

	service := newTestMonitoringService(t, handler)

	collector, err := NewMonitoringCollector(
		projectID,
		service,
		MonitoringCollectorOptions{
			MetricTypePrefixes: []string{metricType},
			RequestInterval:    5 * time.Minute,
		},
		slog.New(slog.DiscardHandler),
		&noopCounterStore{},
		&noopHistogramStore{},
	)
	if err != nil {
		t.Fatalf("NewMonitoringCollector() error = %v", err)
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)

	if _, err := registry.Gather(); err != nil {
		t.Fatalf("Gather() returned an error, the descriptor was fetched more than once: %v", err)
	}

	if got := descriptorPages.Load(); got != 2 {
		t.Fatalf("expected both descriptor pages to be listed, got %d page request(s)", got)
	}

	if got := timeSeriesQueries.Load(); got != 1 {
		t.Fatalf("expected exactly one time series query, got %d", got)
	}
}
