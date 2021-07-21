package mutation

import (
	"testing"

	"go.opencensus.io/stats/view"
)

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

	rows, err := view.RetrieveData(mutationSystemIterationsMetricName)
	if err != nil {
		t.Errorf("Error when retrieving data: %v from %v", err, mutationSystemIterationsMetricName)
	}

	validConvergenceStatuses := 2
	l := len(rows)
	if l != validConvergenceStatuses {
		t.Errorf("Expected '%v' view to have length %v, got %v", mutationSystemIterationsMetricName, validConvergenceStatuses, l)
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
