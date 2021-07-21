package constrainttemplate

import (
	"context"
	"time"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"k8s.io/apimachinery/pkg/types"
)

const (
	ctMetricName   = "constraint_templates"
	ingestCount    = "constraint_template_ingestion_count"
	ingestDuration = "constraint_template_ingestion_duration_seconds"

	ctDesc = "Number of observed constraint templates"
)

var (
	ctM             = stats.Int64(ctMetricName, ctDesc, stats.UnitDimensionless)
	ingestDurationM = stats.Float64(ingestDuration, "How long it took to ingest a constraint template in seconds", stats.UnitSeconds)

	statusKey = tag.MustNewKey("status")

	views = []*view.View{
		{
			Name:        ctMetricName,
			Measure:     ctM,
			Description: ctDesc,
			Aggregation: view.LastValue(),
			TagKeys:     []tag.Key{statusKey},
		},
		{
			Name:        ingestCount,
			Measure:     ingestDurationM,
			Description: "Total number of constraint template ingestion actions",
			Aggregation: view.Count(),
			TagKeys:     []tag.Key{statusKey},
		},
		{
			Name:        ingestDuration,
			Measure:     ingestDurationM,
			Description: "Distribution of how long it took to ingest a constraint template in seconds",
			Aggregation: view.Distribution(0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 2, 3, 4, 5),
			TagKeys:     []tag.Key{statusKey},
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

func reset() error {
	view.Unregister(views...)
	return register()
}

func (r *reporter) reportCtMetric(ctx context.Context, status metrics.Status, count int64) error {
	ctx, err := tag.New(
		ctx,
		tag.Insert(statusKey, string(status)),
	)
	if err != nil {
		return err
	}
	return metrics.Record(ctx, ctM.M(count))
}

func (r *reporter) reportIngestDuration(ctx context.Context, status metrics.Status, d time.Duration) error {
	ctx, err := tag.New(
		ctx,
		tag.Insert(statusKey, string(status)),
	)
	if err != nil {
		return err
	}

	return metrics.Record(ctx, ingestDurationM.M(d.Seconds()))
}

// newStatsReporter creates a reporter for watch metrics.
func newStatsReporter() (*reporter, error) {
	reg := &ctRegistry{cache: make(map[types.NamespacedName]metrics.Status)}
	return &reporter{registry: reg}, nil
}

type reporter struct {
	registry *ctRegistry
}

type ctRegistry struct {
	cache map[types.NamespacedName]metrics.Status
	dirty bool
}

func (r *ctRegistry) add(key types.NamespacedName, status metrics.Status) {
	v, ok := r.cache[key]
	if ok && v == status {
		return
	}
	r.cache[key] = status
	r.dirty = true
}

func (r *ctRegistry) remove(key types.NamespacedName) {
	if _, ok := r.cache[key]; !ok {
		return
	}
	delete(r.cache, key)
	r.dirty = true
}

func (r *ctRegistry) report(ctx context.Context, mReporter *reporter) {
	if !r.dirty {
		return
	}
	totals := make(map[metrics.Status]int64)
	for _, status := range r.cache {
		totals[status]++
	}
	hadErr := false
	for _, status := range metrics.AllStatuses {
		if err := mReporter.reportCtMetric(ctx, status, totals[status]); err != nil {
			log.Error(err, "failed to report total constraint templates")
			hadErr = true
		}
	}
	if !hadErr {
		r.dirty = false
	}
}
