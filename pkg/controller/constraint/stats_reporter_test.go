package constraint

import (
	"testing"

	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"go.opencensus.io/stats/view"
)

func TestReportConstraints(t *testing.T) {
	var expectedValue int64 = 10
	expectedTags := tags{
		enforcementAction: util.Deny,
	}

	r, err := newStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}

	err = r.reportConstraints(expectedTags, expectedValue)
	if err != nil {
		t.Errorf("ReportConstraints error %v", err)
	}
	row, err := view.RetrieveData(totalConstraintsName)
	if err != nil {
		t.Errorf("Error when retrieving data: %v from %v", err, totalConstraintsName)
	}
	value, ok := row[0].Data.(*view.LastValueData)
	if !ok {
		t.Error("ReportConstraints should have aggregation LastValue()")
	}
	for _, tag := range row[0].Tags {
		if tag.Value != string(expectedTags.enforcementAction) {
			t.Errorf("ReportConstraints tags does not match for %v", tag.Key.Name())
		}
	}
	if int64(value.Value) != expectedValue {
		t.Errorf("Metric: %v - Expected %v, got %v", totalConstraintsName, value.Value, expectedValue)
	}
}
