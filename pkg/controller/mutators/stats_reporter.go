package mutators

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
)

// MutatorIngestionStatus defines the outcomes of an attempt to add a Mutator to the mutation System.
type MutatorIngestionStatus string

var (
	mutatorStatusKey = tag.MustNewKey("status")

	// MutatorStatusActive denotes a successfully ingested mutator, ready to mutate objects.
	MutatorStatusActive MutatorIngestionStatus = "active"
	// MutatorStatusError denotes a mutator that failed to ingest.
	MutatorStatusError MutatorIngestionStatus = "error"

	responseTimeInSecM = stats.Float64(
		mutatorIngestionDurationMetricName,
		"The distribution of Mutator ingestion durations",
		stats.UnitSeconds)

	mutatorsM = stats.Int64(
		mutatorsMetricName,
		"The current number of Mutator objects",
		stats.UnitDimensionless)
)

func init() {
	if err := register(); err != nil {
		panic(err)
	}
}

// StatsReporter reports mutator-related controller metrics.
type StatsReporter interface {
	ReportMutatorIngestionRequest(ms MutatorIngestionStatus, d time.Duration) error
	ReportMutatorsStatus(ms MutatorIngestionStatus, n int) error
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

// ReportMutatorIngestionRequest reports both the action of a mutator ingestion and the time
// required for this request to complete.  The outcome of the ingestion attempt is recorded via the
// status argument.
func (r *reporter) ReportMutatorIngestionRequest(ms MutatorIngestionStatus, d time.Duration) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(mutatorStatusKey, string(ms)),
	)
	if err != nil {
		return err
	}

	return report(ctx, responseTimeInSecM.M(d.Seconds()))
}

// ReportMutatorsStatus reports the number of mutators of a specific status that are present in the
// mutation System.
func (r *reporter) ReportMutatorsStatus(ms MutatorIngestionStatus, n int) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(mutatorStatusKey, string(ms)),
	)
	if err != nil {
		return err
	}

	return report(ctx, mutatorsM.M(int64(n)))
}

func report(ctx context.Context, m stats.Measurement) error {
	return metrics.Record(ctx, m)
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
	}
	return view.Register(views...)
}
