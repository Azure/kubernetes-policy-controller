package audit

import (
	"context"
	"time"

	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	violationsMetricName    = "violations"
	auditDurationMetricName = "audit_duration_seconds"
	lastRunTimeMetricName   = "audit_last_run_time"
)

var (
	violationsM    = stats.Int64(violationsMetricName, "Total number of audited violations", stats.UnitDimensionless)
	auditDurationM = stats.Float64(auditDurationMetricName, "Latency of audit operation in seconds", stats.UnitSeconds)
	lastRunTimeM   = stats.Float64(lastRunTimeMetricName, "Timestamp of last audit run time", stats.UnitSeconds)

	enforcementActionKey = tag.MustNewKey("enforcement_action")
)

func init() {
	if err := register(); err != nil {
		panic(err)
	}
}

func register() error {
	views := []*view.View{
		{
			Name:        violationsMetricName,
			Measure:     violationsM,
			Aggregation: view.LastValue(),
			TagKeys:     []tag.Key{enforcementActionKey},
		},
		{
			Name:        auditDurationMetricName,
			Measure:     auditDurationM,
			Aggregation: view.Distribution(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 2, 3, 4, 5),
		},
		{
			Name:        lastRunTimeMetricName,
			Measure:     lastRunTimeM,
			Description: "Timestamp of last audit run time",
			Aggregation: view.LastValue(),
		},
	}
	return view.Register(views...)
}

func (r *reporter) reportTotalViolations(enforcementAction util.EnforcementAction, v int64) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(enforcementActionKey, string(enforcementAction)))
	if err != nil {
		return err
	}

	return r.report(ctx, violationsM.M(v))
}

func (r *reporter) reportLatency(d time.Duration) error {
	ctx, err := tag.New(r.ctx)
	if err != nil {
		return err
	}

	return r.report(ctx, auditDurationM.M(d.Seconds()))
}

func (r *reporter) reportRunStart(t time.Time) error {
	val := float64(t.UnixNano()) / 1e9
	return metrics.Record(r.ctx, lastRunTimeM.M(val))
}

// newStatsReporter creates a reporter for audit metrics.
func newStatsReporter(ctx context.Context) (*reporter, error) {
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
