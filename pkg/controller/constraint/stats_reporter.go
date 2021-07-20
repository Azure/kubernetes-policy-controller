package constraint

import (
	"context"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	constraintsMetricName = "constraints"
)

var (
	constraintsM = stats.Int64(constraintsMetricName, "Current number of known constraints", stats.UnitDimensionless)

	enforcementActionKey = tag.MustNewKey("enforcement_action")
	statusKey            = tag.MustNewKey("status")
)

func init() {
	if err := register(); err != nil {
		panic(err)
	}
}

func register() error {
	views := []*view.View{
		{
			Name:        constraintsMetricName,
			Measure:     constraintsM,
			Aggregation: view.LastValue(),
			TagKeys:     []tag.Key{enforcementActionKey, statusKey},
		},
	}
	return view.Register(views...)
}

func (r *reporter) reportConstraints(t tags, v int64) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(enforcementActionKey, string(t.enforcementAction)),
		tag.Insert(statusKey, string(t.status)))
	if err != nil {
		return err
	}

	return r.report(ctx, constraintsM.M(v))
}

// StatsReporter reports audit metrics.
type StatsReporter interface {
	reportConstraints(t tags, v int64) error
}

// newStatsReporter creaters a reporter for audit metrics.
func newStatsReporter(ctx context.Context) (StatsReporter, error) {
	ctx, err := tag.New(ctx)
	if err != nil {
		return nil, err
	}

	return &reporter{ctx: ctx}, nil
}

type reporter struct {
	ctx context.Context
}

func (r *reporter) report(ctx context.Context, m stats.Measurement) error {
	return metrics.Record(ctx, m)
}
