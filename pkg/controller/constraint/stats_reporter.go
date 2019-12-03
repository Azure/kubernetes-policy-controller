package constraint

import (
	"context"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	totalConstraintsName = "total_constraints"
)

var (
	constraintsTotalM = stats.Int64(totalConstraintsName, "Total number of constraints", stats.UnitDimensionless)

	enforcementActionKey = tag.MustNewKey("enforcement_action")
	statusKey            = tag.MustNewKey("status")
)

func init() {
	register()
}

func register() {
	views := []*view.View{
		{
			Name:        totalConstraintsName,
			Measure:     constraintsTotalM,
			Aggregation: view.LastValue(),
			TagKeys:     []tag.Key{enforcementActionKey, statusKey},
		},
	}

	if err := view.Register(views...); err != nil {
		panic(err)
	}
}

func (r *reporter) reportConstraints(t tags, v int64) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(enforcementActionKey, string(t.enforcementAction)),
		tag.Insert(statusKey, string(t.status)))
	if err != nil {
		return err
	}

	return r.report(ctx, constraintsTotalM.M(v))
}

// StatsReporter reports audit metrics
type StatsReporter interface {
	reportConstraints(t tags, v int64) error
}

// newStatsReporter creaters a reporter for audit metrics
func newStatsReporter() (StatsReporter, error) {
	ctx, err := tag.New(
		context.Background(),
	)
	if err != nil {
		return nil, err
	}

	return &reporter{ctx: ctx}, nil
}

type reporter struct {
	ctx context.Context
}

func (r *reporter) report(ctx context.Context, m stats.Measurement) error {
	metrics.Record(ctx, m)
	return nil
}
