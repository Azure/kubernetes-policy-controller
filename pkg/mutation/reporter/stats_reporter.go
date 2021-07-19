package reporter

import (
	"context"
	"time"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	mutatorIngestionCountMetricName    = "mutator_ingestion_count"
	mutatorIngestionDurationMetricName = "mutator_ingestion_duration_seconds"
	mutatorsMetricName                 = "mutators"
	mutationSystemIterationsMetricName = "mutation_system_iterations"
)

// MutatorIngestionStatus defines the outcomes of an attempt to add a Mutator to the mutation System.
type MutatorIngestionStatus string

// SystemConvergenceStatus defines the outcomes of the attempted mutation of an object by the
// mutation System.  The System is meant to converge on a fully mutated object.
type SystemConvergenceStatus string

var (
	mutatorStatusKey = tag.MustNewKey("status")

	// MutatorStatusActive denotes a successfully ingested mutator, ready to mutate objects.
	MutatorStatusActive MutatorIngestionStatus = "active"
	// MutatorStatusError denotes a mutator that failed to ingest.
	MutatorStatusError MutatorIngestionStatus = "error"

	systemConvergenceKey = tag.MustNewKey("success")

	// SystemConvergenceTrue denotes a successfully converged mutation system request.
	SystemConvergenceTrue SystemConvergenceStatus = "true"
	// SystemConvergenceFalse denotes an unsuccessfully converged mutation system request.
	SystemConvergenceFalse SystemConvergenceStatus = "false"

	responseTimeInSecM = stats.Float64(
		mutatorIngestionDurationMetricName,
		"The distribution of Mutator ingestion durations",
		stats.UnitSeconds)

	mutatorsM = stats.Int64(
		mutatorsMetricName,
		"The current number of Mutator objects",
		stats.UnitDimensionless)

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

type Reporter struct {
	ctx context.Context
}

// ReportMutatorIngestionRequest reports both the action of a mutator ingestion and the time
// required for this request to complete.  The outcome of the ingestion attempt is recorded via the
// status argument.
func (r *Reporter) ReportMutatorIngestionRequest(ms MutatorIngestionStatus, d time.Duration) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(mutatorStatusKey, string(ms)),
	)
	if err != nil {
		return err
	}

	return r.report(ctx, responseTimeInSecM.M(d.Seconds()))
}

// ReportMutatorsStatus reports the number of mutators of a specific status that are present in the
// mutation System.
func (r *Reporter) ReportMutatorsStatus(ms MutatorIngestionStatus, n int) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(mutatorStatusKey, string(ms)),
	)
	if err != nil {
		return err
	}

	return r.report(ctx, mutatorsM.M(int64(n)))
}

// ReportIterationConvergence reports the success or failure of the mutation system to converge.
// It also records the number of system iterations that were required to reach this end.
func (r *Reporter) ReportIterationConvergence(scs SystemConvergenceStatus, iterations int) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(systemConvergenceKey, string(scs)),
	)
	if err != nil {
		return err
	}

	return r.report(ctx, systemIterationsM.M(int64(iterations)))
}

func (r *Reporter) report(ctx context.Context, m stats.Measurement) error {
	return metrics.Record(ctx, m)
}

// NewStatsReporter creaters a reporter for mutation metrics.
func NewStatsReporter() (*Reporter, error) {
	ctx, err := tag.New(
		context.Background(),
	)
	if err != nil {
		return nil, err
	}

	return &Reporter{ctx: ctx}, nil
}

func register() error {
	views := []*view.View{
		{
			Name:        mutatorIngestionCountMetricName,
			Description: "Total number of Mutator ingestion actions",
			Measure:     responseTimeInSecM,
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{mutatorStatusKey},
		},
		{
			Name:        mutatorIngestionDurationMetricName,
			Description: responseTimeInSecM.Description(),
			Measure:     responseTimeInSecM,
			// JULIAN - We'll need to tune this.  I'm not sure if these histogram sections are valid.
			Aggregation: view.Distribution(0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01, 0.02, 0.03, 0.04, 0.05),
			TagKeys:     []tag.Key{mutatorStatusKey},
		},
		{
			Name:        mutatorsMetricName,
			Description: "The current number of Mutator objects",
			Measure:     mutatorsM,
			Aggregation: view.LastValue(),
			TagKeys:     []tag.Key{mutatorStatusKey},
		},
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
