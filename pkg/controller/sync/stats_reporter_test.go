package sync

import (
	"context"
	"testing"
	"time"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

func TestReportSync(t *testing.T) {
	const expectedValue int64 = 10
	const expectedRowLength = 1
	expectedTags := Tags{
		Kind:   "Pod",
		Status: metrics.ActiveStatus,
	}

	r, err := NewStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}
	ctx := context.Background()
	err = r.reportSync(ctx, expectedTags, expectedValue)
	if err != nil {
		t.Errorf("reportSync error %v", err)
	}
	row := checkData(t, syncMetricName, expectedRowLength)
	value, ok := row.Data.(*view.LastValueData)
	if !ok {
		t.Error("reportSync should have aggregation LastValue()")
	}
	found := contains(row.Tags, expectedTags.Kind)
	if !found {
		t.Errorf("reportSync tags does not match for %v", expectedTags.Kind)
	}
	found = contains(row.Tags, string(expectedTags.Status))
	if !found {
		t.Errorf("reportSync tags does not match for %v", expectedTags.Status)
	}
	if int64(value.Value) != expectedValue {
		t.Errorf("Metric: %v - Expected %v, got %v", syncMetricName, expectedValue, value.Value)
	}
}

func TestReportSyncLatency(t *testing.T) {
	const expectedLatencyValueMin = time.Duration(100 * time.Second)
	const expectedLatencyValueMax = time.Duration(500 * time.Second)
	const expectedLatencyCount int64 = 2
	const expectedLatencyMin float64 = 100
	const expectedLatencyMax float64 = 500
	const expectedRowLength = 1

	r, err := NewStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}

	ctx := context.Background()
	err = r.reportSyncDuration(ctx, expectedLatencyValueMin)
	if err != nil {
		t.Errorf("ReportLatency error %v", err)
	}
	err = r.reportSyncDuration(ctx, expectedLatencyValueMax)
	if err != nil {
		t.Errorf("ReportLatency error %v", err)
	}
	row := checkData(t, syncDurationMetricName, expectedRowLength)
	latencyValue, ok := row.Data.(*view.DistributionData)
	if !ok {
		t.Error("reportSyncDuration should have aggregation type Distribution")
	}
	if latencyValue.Count != expectedLatencyCount {
		t.Errorf("Metric: %v - Expected %v, got %v", syncDurationMetricName, latencyValue.Count, expectedLatencyCount)
	}
	if latencyValue.Min != expectedLatencyMin {
		t.Errorf("Metric: %v - Expected %v, got %v", syncDurationMetricName, latencyValue.Min, expectedLatencyMin)
	}
	if latencyValue.Max != expectedLatencyMax {
		t.Errorf("Metric: %v - Expected %v, got %v", syncDurationMetricName, latencyValue.Max, expectedLatencyMax)
	}
}

func TestLastRunSync(t *testing.T) {
	const expectedTime float64 = 11
	const expectedRowLength = 1

	fakeNow := func() float64 {
		return float64(expectedTime)
	}

	r, err := NewStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}
	r.now = fakeNow
	ctx := context.Background()
	err = r.reportLastSync(ctx)
	if err != nil {
		t.Errorf("reportRestartCheck error %v", err)
	}
	row := checkData(t, lastRunTimeMetricName, expectedRowLength)
	value, ok := row.Data.(*view.LastValueData)
	if !ok {
		t.Error("reportRestartCheck should have aggregation LastValue()")
	}
	if len(row.Tags) != 0 {
		t.Errorf("reportRestartCheck tags is non-empty, got: %v", row.Tags)
	}
	if value.Value != expectedTime {
		t.Errorf("Metric: %v - Expected %v, got %v", lastRunTimeMetricName, expectedTime, value.Value)
	}
}

func contains(s []tag.Tag, e string) bool {
	for _, a := range s {
		if a.Value == e {
			return true
		}
	}
	return false
}

func checkData(t *testing.T, name string, expectedRowLength int) *view.Row {
	row, err := view.RetrieveData(name)
	if err != nil {
		t.Errorf("Error when retrieving data: %v from %v", err, name)
	}
	if len(row) != expectedRowLength {
		t.Errorf("Expected length %v, got %v", expectedRowLength, len(row))
	}
	if row[0].Data == nil {
		t.Errorf("Expected row data not to be nil")
	}
	return row[0]
}
