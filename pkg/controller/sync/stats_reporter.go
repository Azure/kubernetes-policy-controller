package sync

import (
	"context"
	"time"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	syncMetricName         = "sync"
	syncDurationMetricName = "sync_duration_seconds"
	lastRunTimeMetricName  = "sync_last_run_time"
)

var (
	syncM         = stats.Int64(syncMetricName, "Total number of resources of each kind being cached", stats.UnitDimensionless)
	syncDurationM = stats.Float64(syncDurationMetricName, "Latency of sync operation in seconds", stats.UnitSeconds)
	lastRunSyncM  = stats.Float64(lastRunTimeMetricName, "Timestamp of last sync operation", stats.UnitSeconds)

	kindKey   = tag.MustNewKey("kind")
	statusKey = tag.MustNewKey("status")

	views = []*view.View{
		{
			Name:        syncM.Name(),
			Measure:     syncM,
			Description: syncM.Description(),
			Aggregation: view.LastValue(),
			TagKeys:     []tag.Key{kindKey, statusKey},
		},
		{
			Name:        syncDurationM.Name(),
			Measure:     syncDurationM,
			Description: syncDurationM.Description(),
			Aggregation: view.Distribution(0.0001, 0.0002, 0.0003, 0.0004, 0.0005, 0.0006, 0.0007, 0.0008, 0.0009, 0.001, 0.002, 0.003, 0.004, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05),
		},
		{
			Name:        lastRunSyncM.Name(),
			Measure:     lastRunSyncM,
			Description: lastRunSyncM.Description(),
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

type Reporter struct {
	now func() float64
}

// NewStatsReporter creates a reporter for sync metrics.
func NewStatsReporter() (*Reporter, error) {
	return &Reporter{now: now}, nil
}

func (r *Reporter) report(ctx context.Context, m stats.Measurement) error {
	return metrics.Record(ctx, m)
}

func (r *Reporter) reportSyncDuration(ctx context.Context, d time.Duration) error {
	return r.report(ctx, syncDurationM.M(d.Seconds()))
}

func (r *Reporter) reportLastSync(ctx context.Context) error {
	return metrics.Record(ctx, lastRunSyncM.M(r.now()))
}

func (r *Reporter) reportSync(ctx context.Context, t Tags, v int64) error {
	ctx, err := tag.New(
		ctx,
		tag.Insert(kindKey, string(t.Kind)),
		tag.Insert(statusKey, string(t.Status)))
	if err != nil {
		return err
	}

	return r.report(ctx, syncM.M(v))
}

// now returns the timestamp as a second-denominated float.
func now() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}
