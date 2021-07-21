package constraint

import (
	"context"
	"testing"

	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"go.opencensus.io/stats/view"
)

func TestReportConstraints(t *testing.T) {
	const expectedValue int64 = 10
	const expectedRowLength = 1
	expectedTags := tags{
		enforcementAction: util.Deny,
	}

	ctx := context.Background()
	r, err := newStatsReporter()
	if err != nil {
		t.Errorf("newStatsReporter() error %v", err)
	}
	err = r.reportConstraints(ctx, expectedTags, expectedValue)
	if err != nil {
		t.Errorf("ReportConstraints error %v", err)
	}
	row := checkData(t, constraintsMetricName, expectedRowLength)
	value, ok := row.Data.(*view.LastValueData)
	if !ok {
		t.Error("ReportConstraints should have aggregation LastValue()")
	}
	for _, tag := range row.Tags {
		if tag.Value != string(expectedTags.enforcementAction) {
			t.Errorf("ReportConstraints tags does not match for %v", tag.Key.Name())
		}
	}
	if int64(value.Value) != expectedValue {
		t.Errorf("Metric: %v - Expected %v, got %v", constraintsMetricName, expectedValue, value.Value)
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
