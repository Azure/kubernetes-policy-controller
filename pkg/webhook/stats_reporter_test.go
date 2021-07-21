package webhook

import (
	"context"
	"testing"
	"time"

	"go.opencensus.io/stats/view"
)

const (
	expectedDurationValueMin         = time.Duration(1 * time.Second)
	expectedDurationValueMax         = time.Duration(5 * time.Second)
	expectedDurationMin      float64 = 1
	expectedDurationMax      float64 = 5
	expectedCount            int64   = 2
	expectedRowLength                = 1
)

func TestValidationReportRequest(t *testing.T) {
	expectedTags := map[string]string{
		"admission_status": "allow",
	}

	ctx := context.Background()
	r, err := newStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}
	err = r.ReportValidationRequest(ctx, allowResponse, expectedDurationValueMin)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}
	err = r.ReportValidationRequest(ctx, allowResponse, expectedDurationValueMax)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}
	check(t, expectedTags, validationRequestCountMetricName, validationRequestDurationMetricName)
}

func TestMutationReportRequest(t *testing.T) {
	expectedTags := map[string]string{
		"mutation_status": "success",
	}

	ctx := context.Background()
	r, err := newStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}
	err = r.ReportMutationRequest(ctx, successResponse, expectedDurationValueMin)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}
	err = r.ReportMutationRequest(ctx, successResponse, expectedDurationValueMax)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}

	check(t, expectedTags, mutationRequestCountMetricName, mutationRequestDurationMetricName)
}

func check(t *testing.T, expectedTags map[string]string, requestCountMetricName string, requestDurationMetricName string) {
	// count test
	row := checkData(t, requestCountMetricName, expectedRowLength)

	count, ok := row.Data.(*view.CountData)
	if !ok {
		t.Error("ReportRequest should have aggregation Count()")
	}
	for _, tag := range row.Tags {
		if tag.Value != expectedTags[tag.Key.Name()] {
			t.Errorf("ReportRequest tags does not match for %v", tag.Key.Name())
		}
	}
	if count.Value != expectedCount {
		t.Errorf("Metric: %v - Expected %v, got %v. ", requestCountMetricName, expectedCount, count.Value)
	}

	// Duration test
	row = checkData(t, requestDurationMetricName, expectedRowLength)
	durationValue, ok := row.Data.(*view.DistributionData)
	if !ok {
		t.Error("ReportRequest should have aggregation Distribution()")
	}
	for _, tag := range row.Tags {
		if tag.Value != expectedTags[tag.Key.Name()] {
			t.Errorf("ReportRequest tags does not match for %v", tag.Key.Name())
		}
	}
	if durationValue.Min != expectedDurationMin {
		t.Errorf("Metric: %v - Expected %v, got %v. ", requestDurationMetricName, expectedDurationMin, durationValue.Min)
	}
	if durationValue.Max != expectedDurationMax {
		t.Errorf("Metric: %v - Expected %v, got %v. ", requestDurationMetricName, expectedDurationMax, durationValue.Max)
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
