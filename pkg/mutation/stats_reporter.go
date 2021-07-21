package mutation

import (
	"context"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	mutationSystemIterationsMetricName = "mutation_system_iterations"
)

// SystemConvergenceStatus defines the outcomes of the attempted mutation of an object by the
// mutation System.  The System is meant to converge on a fully mutated object.
type SystemConvergenceStatus string

var (
	systemConvergenceKey = tag.MustNewKey("success")

	// SystemConvergenceTrue denotes a successfully converged mutation system request.
	SystemConvergenceTrue SystemConvergenceStatus = "true"
	// SystemConvergenceFalse denotes an unsuccessfully converged mutation system request.
	SystemConvergenceFalse SystemConvergenceStatus = "false"

	systemIterationsM = stats.Int64(
		mutationSystemIterationsMetricName,
		"The distribution of Mutator ingestion durations",
		stats.UnitDimensionless)
)

func init() {
	if err := register(); err != nil {
		panic(err)
	}
}

// StatsReporter reports mutator-related metrics.
type StatsReporter interface {
	ReportIterationConvergence(scs SystemConvergenceStatus, iterations int) error
}

// reporter implements StatsReporter interface.
type reporter struct {
	ctx context.Context
}

// NewStatsReporter creaters a reporter for webhook metrics.
func NewStatsReporter() (StatsReporter, error) {
	ctx, err := tag.New(
		context.Background(),
	)
	if err != nil {
		return nil, err
	}

	return &reporter{ctx: ctx}, nil
}

// ReportIterationConvergence reports the success or failure of the mutation system to converge.
// It also records the number of system iterations that were required to reach this end.
func (r *reporter) ReportIterationConvergence(scs SystemConvergenceStatus, iterations int) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(systemConvergenceKey, string(scs)),
	)
	if err != nil {
		return err
	}

	return report(ctx, systemIterationsM.M(int64(iterations)))
}

func report(ctx context.Context, m stats.Measurement) error {
	return metrics.Record(ctx, m)
}

func register() error {
	views := []*view.View{
		{
			Name:        mutationSystemIterationsMetricName,
			Description: systemIterationsM.Description(),
			Measure:     systemIterationsM,
			// JULIAN - We'll need to tune this.  I'm not sure if these histogram sections are valid.
			Aggregation: view.Distribution(2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233),
			TagKeys:     []tag.Key{systemConvergenceKey},
		},
	}
	return view.Register(views...)
}
