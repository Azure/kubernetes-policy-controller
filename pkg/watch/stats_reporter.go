package watch

import (
	"context"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

const (
	gvkCountMetricName       = "watch_manager_watched_gvk"
	gvkIntentCountMetricName = "watch_manager_intended_watch_gvk"
)

var (
	gvkCountM       = stats.Int64(gvkCountMetricName, "Total number of watched GroupVersionKinds", stats.UnitDimensionless)
	gvkIntentCountM = stats.Int64(gvkIntentCountMetricName, "Total number of GroupVersionKinds with a registered watch intent", stats.UnitDimensionless)

	views = []*view.View{
		{
			Name:        gvkCountMetricName,
			Measure:     gvkCountM,
			Description: "The total number of Group/Version/Kinds currently watched by the watch manager",
			Aggregation: view.LastValue(),
		},
		{
			Name:        gvkIntentCountMetricName,
			Measure:     gvkIntentCountM,
			Description: "The total number of Group/Version/Kinds that the watch manager has instructions to watch. This could differ from the actual count due to resources being pending, non-existent, or a failure of the watch manager to restart",
			Aggregation: view.LastValue(),
		},
	}
)

func init() {
	if err := register(); err != nil {
		panic(err)
	}
}

func register() error {
	return view.Register(views...)
}

func (r *reporter) reportGvkCount(ctx context.Context, count int64) error {
	return metrics.Record(ctx, gvkCountM.M(count))
}

func (r *reporter) reportGvkIntentCount(ctx context.Context, count int64) error {
	return metrics.Record(ctx, gvkIntentCountM.M(count))
}

// newStatsReporter creates a reporter for watch metrics.
func newStatsReporter() (*reporter, error) {
	return &reporter{}, nil
}

type reporter struct{}
