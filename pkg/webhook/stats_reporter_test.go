package webhook

import (
	"testing"
	"time"

	"go.opencensus.io/stats/view"
)

func TestReportRequest(t *testing.T) {
	admissionResponse := "allow"
	expectedTags := map[string]string{
		"admission_status": "allow",
	}

	expectedDurationValueMin := time.Duration(1 * time.Second)
	expectedDurationValueMax := time.Duration(5 * time.Second)
	var expectedDurationMin float64 = 1
	var expectedDurationMax float64 = 5
	var expectedCount int64 = 4

	r, err := NewStatsReporter()
	if err != nil {
		t.Errorf("NewStatsReporter() error %v", err)
	}

	err = r.ReportRequest(admissionResponse, expectedDurationValueMin)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}
	err = r.ReportRequest(admissionResponse, expectedDurationValueMax)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}

	// count test
	row, err := view.RetrieveData(requestCountName)
	if err != nil {
		t.Errorf("Error when retrieving data: %v from %v", err, requestCountName)
	}
	count, ok := row[0].Data.(*view.CountData)
	if !ok {
		t.Error("ReportRequest should have aggregation Count()")
	}
	for _, tag := range row[0].Tags {
		if tag.Value != expectedTags[tag.Key.Name()] {
			t.Errorf("ReportRequest tags does not match for %v", tag.Key.Name())
		}
	}
	if count.Value != expectedCount {
		t.Errorf("Metric: %v - Expected %v, got %v. ", requestCountName, count.Value, expectedCount)
	}

	// Duration test
	row, err = view.RetrieveData(requestLatenciesName)
	if err != nil {
		t.Errorf("Error when retrieving data: %v from %v", err, requestLatenciesName)
	}
	DurationValue, ok := row[0].Data.(*view.DistributionData)
	if !ok {
		t.Error("ReportRequest should have aggregation Distribution()")
	}
	for _, tag := range row[0].Tags {
		if tag.Value != expectedTags[tag.Key.Name()] {
			t.Errorf("ReportRequest tags does not match for %v", tag.Key.Name())
		}
	}
	if DurationValue.Min != expectedDurationMin {
		t.Errorf("Metric: %v - Expected %v, got %v. ", requestLatenciesName, DurationValue.Min, expectedDurationMin)
	}
	if DurationValue.Max != expectedDurationMax {
		t.Errorf("Metric: %v - Expected %v, got %v. ", requestLatenciesName, DurationValue.Max, expectedDurationMax)
	}
}
