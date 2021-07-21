package mutationreport

import (
	"testing"
	"time"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

func TestReportMutatorIngestionRequest(t *testing.T) {
	expectedTags := map[string]string{
		"status": "active",
	}

	const (
		expectedDurationValueMin         = time.Duration(1 * time.Second)
		expectedDurationValueMax         = time.Duration(5 * time.Second)
		expectedDurationMin      float64 = 1
		expectedDurationMax      float64 = 5
		expectedRowLength                = 1
	)

	r, err := metrics.NewMetricsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}

	err = ReportMutatorIngestionRequest(r, MutatorStatusActive, expectedDurationValueMin)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}
	err = ReportMutatorIngestionRequest(r, MutatorStatusActive, expectedDurationValueMax)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}

	// Count test
	row := checkData(t, mutatorIngestionCountMetricName, expectedRowLength)
	count, ok := row.Data.(*view.CountData)
	if !ok {
		t.Error("ReportRequest should have aggregation Count()")
	}
	if count.Value != 2 {
		t.Errorf("Metric: %v - Expected %v, got %v. ", mutatorIngestionCountMetricName, 2, count.Value)
	}

	verifyTags(t, expectedTags, row.Tags)

	// Duration test
	row = checkData(t, mutatorIngestionDurationMetricName, expectedRowLength)
	durationValue, ok := row.Data.(*view.DistributionData)
	if !ok {
		t.Error("ReportRequest should have aggregation Distribution()")
	}
	if durationValue.Min != expectedDurationMin {
		t.Errorf("Metric: %v - Expected %v, got %v. ", mutatorIngestionDurationMetricName, expectedDurationMin, durationValue.Min)
	}
	if durationValue.Max != expectedDurationMax {
		t.Errorf("Metric: %v - Expected %v, got %v. ", mutatorIngestionDurationMetricName, expectedDurationMax, durationValue.Max)
	}

	verifyTags(t, expectedTags, row.Tags)
}

func checkData(t *testing.T, name string, rowLength int) *view.Row {
	row, err := view.RetrieveData(name)
	if err != nil {
		t.Errorf("Error when retrieving data: %v from %v", err, name)
	}
	if len(row) != rowLength {
		t.Errorf("Expected '%v' row to have length %v, got %v", name, rowLength, len(row))
	}
	if row[0].Data == nil {
		t.Errorf("Expected row data not to be nil")
	}
	return row[0]
}

func verifyTags(t *testing.T, expected map[string]string, actual []tag.Tag) {
	for _, tag := range actual {
		ex := expected[tag.Key.Name()]
		if tag.Value != ex {
			t.Errorf("Expected tag '%v' to have value '%v' but found '%v'", tag.Key.Name(), ex, tag.Value)
		}
	}
}
