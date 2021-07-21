package watch

import (
	"context"
	"testing"

	"go.opencensus.io/stats/view"
)

func TestGauges(t *testing.T) {
	r, err := newStatsReporter()
	if err != nil {
		t.Fatalf("newStatsReporter() error %v", err)
	}
	tc := []struct {
		name string
		fn   func(context.Context, int64) error
	}{
		{
			name: gvkCountMetricName,
			fn:   r.reportGvkCount,
		},
		{
			name: gvkIntentCountMetricName,
			fn:   r.reportGvkIntentCount,
		},
	}
	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			const expectedValue int64 = 10
			const expectedRowLength = 1

			ctx := context.Background()
			err = tt.fn(ctx, expectedValue)
			if err != nil {
				t.Errorf("function error %v", err)
			}
			row := checkData(t, tt.name, expectedRowLength)
			value, ok := row.Data.(*view.LastValueData)
			if !ok {
				t.Errorf("metric %s should have aggregation LastValue()", tt.name)
			}
			if len(row.Tags) != 0 {
				t.Errorf("%s tags is non-empty, got: %v", tt.name, row.Tags)
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
