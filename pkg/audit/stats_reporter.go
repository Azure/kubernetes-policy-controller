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
	totalViolationsName = "total_violations"
	auditDurationName   = "audit_duration_seconds"
)

var (
	violationsTotalM = stats.Int64(totalViolationsName, "Total number of violations per constraint", stats.UnitDimensionless)
	auditDurationM   = stats.Float64(auditDurationName, "Latency of audit operation in seconds", stats.UnitSeconds)

	enforcementActionKey = tag.MustNewKey("enforcement_action")
)

func init() {
	register()
}

func register() {
	views := []*view.View{
		{
			Name:        totalViolationsName,
			Measure:     violationsTotalM,
			Aggregation: view.LastValue(),
			TagKeys:     []tag.Key{enforcementActionKey},
		},
		{
			Name:        auditDurationName,
			Measure:     auditDurationM,
			Aggregation: view.Distribution(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 2, 3, 4, 5),
		},
	}

	if err := view.Register(views...); err != nil {
		panic(err)
	}
}

func (r *reporter) ReportTotalViolations(enforcementAction util.EnforcementAction, v int64) error {
	ctx, err := tag.New(
		r.ctx,
		tag.Insert(enforcementActionKey, string(enforcementAction)))
	if err != nil {
		return err
	}

	return r.report(ctx, violationsTotalM.M(v))
}

func (r *reporter) ReportLatency(d time.Duration) error {
	ctx, err := tag.New(r.ctx)
	if err != nil {
		return err
	}

	return r.report(ctx, auditDurationM.M(d.Seconds()))
}

// StatsReporter reports audit metrics
type StatsReporter interface {
	ReportTotalViolations(enforcementAction util.EnforcementAction, v int64) error
	ReportLatency(d time.Duration) error
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
