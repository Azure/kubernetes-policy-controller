package reporter

import (
	"testing"
	"time"

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

	r, err := NewStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}

	err = r.ReportMutatorIngestionRequest(MutatorStatusActive, expectedDurationValueMin)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}
	err = r.ReportMutatorIngestionRequest(MutatorStatusActive, expectedDurationValueMax)
	if err != nil {
		t.Errorf("ReportRequest error %v", err)
	}

	// Count test
	row := checkData(t, MutatorIngestionCountMetricName, expectedRowLength)
	count, ok := row.Data.(*view.CountData)
	if !ok {
		t.Error("ReportRequest should have aggregation Count()")
	}
	if count.Value != 2 {
		t.Errorf("Metric: %v - Expected %v, got %v. ", MutatorIngestionCountMetricName, 2, count.Value)
	}

	verifyTags(t, expectedTags, row.Tags)

	// Duration test
	row = checkData(t, MutatorIngestionDurationMetricName, expectedRowLength)
	durationValue, ok := row.Data.(*view.DistributionData)
	if !ok {
		t.Error("ReportRequest should have aggregation Distribution()")
	}
	if durationValue.Min != expectedDurationMin {
		t.Errorf("Metric: %v - Expected %v, got %v. ", MutatorIngestionDurationMetricName, expectedDurationMin, durationValue.Min)
	}
	if durationValue.Max != expectedDurationMax {
		t.Errorf("Metric: %v - Expected %v, got %v. ", MutatorIngestionDurationMetricName, expectedDurationMax, durationValue.Max)
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

func TestReportMutatorsStatus(t *testing.T) {
	r, err := NewStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}

	// Set to one set of values and then to another.  Prove that the view reflects the most up
	// to date version of the data.

	// 4 active, 4 error
	err = r.ReportMutatorsStatus(MutatorStatusActive, 4)
	if err != nil {
		t.Errorf("reportMutatorsStatus error: %v", err)
	}
	err = r.ReportMutatorsStatus(MutatorStatusError, 4)
	if err != nil {
		t.Errorf("reportMutatorsStatus error: %v", err)
	}
	// 5 active, 3 error
	err = r.ReportMutatorsStatus(MutatorStatusActive, 5)
	if err != nil {
		t.Errorf("reportMutatorsStatus error: %v", err)
	}
	err = r.ReportMutatorsStatus(MutatorStatusError, 3)
	if err != nil {
		t.Errorf("reportMutatorsStatus error: %v", err)
	}

	data, err := view.RetrieveData(MutatorsMetricName)
	if err != nil {
		t.Errorf("Error when retrieving data: %v from %v", err, MutatorsMetricName)
	}

	l := len(data)
	if l != 2 {
		t.Errorf("Expected '%v' view to have length %v, got %v", MutatorsMetricName, 2, l)
	}

	verifyLastValueRow(t, data, MutatorStatusActive, 5)
	verifyLastValueRow(t, data, MutatorStatusError, 3)
}

func verifyLastValueRow(t *testing.T, rows []*view.Row, tag MutatorIngestionStatus, expectedValue int) {
	for _, r := range rows {
		if !hasTag(r, mutatorStatusKey.Name(), string(tag)) {
			continue
		}

		lastValueData, ok := r.Data.(*view.LastValueData)
		if !ok {
			t.Errorf("Data is not of type *view.LastValueData")
		}

		if int(lastValueData.Value) != expectedValue {
			t.Errorf("Expected value '%v' for tag '%v' but received '%v'", expectedValue, tag, lastValueData.Value)
		}

		return
	}

	t.Errorf("Expected to find row with tag '%v' but none were found", tag)
}

func TestReportIterationConvergence(t *testing.T) {
	r, err := NewStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}

	const (
		successMax = 5
		successMin = 3
		failureMax = 8
		failureMin = failureMax
	)

	err = r.ReportIterationConvergence(SystemConvergenceTrue, successMax)
	if err != nil {
		t.Errorf("reportIterationConvergence error: %v", err)
	}
	err = r.ReportIterationConvergence(SystemConvergenceFalse, failureMax)
	if err != nil {
		t.Errorf("reportIterationConvergence error: %v", err)
	}
	err = r.ReportIterationConvergence(SystemConvergenceTrue, successMin)
	if err != nil {
		t.Errorf("reportIterationConvergence error: %v", err)
	}

	rows, err := view.RetrieveData(MutationSystemIterationsMetricName)
	if err != nil {
		t.Errorf("Error when retrieving data: %v from %v", err, MutationSystemIterationsMetricName)
	}

	validConvergenceStatuses := 2
	l := len(rows)
	if l != validConvergenceStatuses {
		t.Errorf("Expected '%v' view to have length %v, got %v", MutatorsMetricName, validConvergenceStatuses, l)
	}

	verifyDistributionRow(t, rows, SystemConvergenceTrue, 2, successMin, successMax)
	verifyDistributionRow(t, rows, SystemConvergenceFalse, 1, failureMin, failureMax)
}

func verifyDistributionRow(t *testing.T, rows []*view.Row, tag SystemConvergenceStatus, count, min, max int) {
	for _, r := range rows {
		if !hasTag(r, systemConvergenceKey.Name(), string(tag)) {
			continue
		}

		distData, ok := r.Data.(*view.DistributionData)
		if !ok {
			t.Errorf("Data is not of type *view.DistributionData")
		}

		if int(distData.Count) != count {
			t.Errorf("Expected count '%v' for tag '%v' but received '%v'", count, tag, distData.Count)
		}
		if int(distData.Min) != min {
			t.Errorf("Expected count '%v' for tag '%v' but received '%v'", min, tag, distData.Min)
		}
		if int(distData.Max) != max {
			t.Errorf("Expected max '%v' for tag '%v' but received '%v'", max, tag, distData.Max)
		}

		return
	}

	t.Errorf("Expected to find row with tag '%v' but none were found", tag)
}

func hasTag(row *view.Row, key, value string) bool {
	for _, tag := range row.Tags {
		if tag.Key.Name() == key && tag.Value == value {
			return true
		}
	}

	return false
}
