package constrainttemplate

import (
	"context"
	"testing"
	"time"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats/view"
)

func TestReportIngestion(t *testing.T) {
	if err := reset(); err != nil {
		t.Errorf("Could not reset stats: %v", err)
	}
	expectedTags := map[string]string{
		"status": "active",
	}
	const expectedDurationValueMin = time.Duration(1 * time.Second)
	const expectedDurationValueMax = time.Duration(5 * time.Second)
	const expectedDurationMin float64 = 1
	const expectedDurationMax float64 = 5
	const expectedCount int64 = 2
	const expectedRowLength = 1

	r, err := newStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}
	ctx := context.Background()
	err = r.reportIngestDuration(ctx, metrics.ActiveStatus, expectedDurationValueMin)
	if err != nil {
		t.Errorf("reportIngestDuration error %v", err)
	}
	err = r.reportIngestDuration(ctx, metrics.ActiveStatus, expectedDurationValueMax)
	if err != nil {
		t.Errorf("reportIngestDuration error %v", err)
	}

	// count test
	row := checkData(t, ingestCount, expectedRowLength)
	count, ok := row.Data.(*view.CountData)
	if !ok {
		t.Error("ingestCount should have aggregation Count()")
	}
	for _, tag := range row.Tags {
		if tag.Value != expectedTags[tag.Key.Name()] {
			t.Errorf("ingestCount tags does not match for %v", tag.Key.Name())
		}
	}
	if count.Value != expectedCount {
		t.Errorf("Metric: %v - Expected %v, got %v. ", ingestCount, expectedCount, count.Value)
	}

	// Duration test
	row = checkData(t, ingestDuration, expectedRowLength)
	durationValue, ok := row.Data.(*view.DistributionData)
	if !ok {
		t.Error("ingestDuration should have aggregation Distribution()")
	}
	for _, tag := range row.Tags {
		if tag.Value != expectedTags[tag.Key.Name()] {
			t.Errorf("ingestDuration tags does not match for %v", tag.Key.Name())
		}
	}
	if durationValue.Min != expectedDurationMin {
		t.Errorf("Metric: %v - Expected %v, got %v. ", ingestDuration, durationValue.Min, expectedDurationMin)
	}
	if durationValue.Max != expectedDurationMax {
		t.Errorf("Metric: %v - Expected %v, got %v. ", ingestDuration, durationValue.Max, expectedDurationMax)
	}
}

func TestGauges(t *testing.T) {
	r, err := newStatsReporter()
	if err != nil {
		t.Fatalf("newStatsReporter() error %v", err)
	}
	tc := []struct {
		name string
		fn   func(context.Context, metrics.Status, int64) error
	}{
		{
			name: ctMetricName,
			fn:   r.reportCtMetric,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			const expectedValue int64 = 10
			const expectedRowLength = 1
			expectedTags := map[string]string{
				"status": "active",
			}

			ctx := context.Background()
			err = tt.fn(ctx, metrics.ActiveStatus, expectedValue)
			if err != nil {
				t.Errorf("function error %v", err)
			}
			row := checkData(t, tt.name, expectedRowLength)
			value, ok := row.Data.(*view.LastValueData)
			if !ok {
				t.Errorf("metric %s should have aggregation LastValue()", tt.name)
			}

			if len(row.Tags) != 1 {
				t.Errorf("%s expected %v tags, got: %v", tt.name, len(expectedTags), len(row.Tags))
			}
			for _, tag := range row.Tags {
				if tag.Value != expectedTags[tag.Key.Name()] {
					t.Errorf("%v tags does not match for %v", tt.name, tag.Key.Name())
				}
			}
			if int64(value.Value) != expectedValue {
				t.Errorf("Metric: %v - Expected %v, got %v", tt.name, expectedValue, value.Value)
			}
		})
	}
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
