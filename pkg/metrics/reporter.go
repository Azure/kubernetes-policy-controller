package metrics

import (
	"context"

	"go.opencensus.io/tag"
)

type Reporter struct {
	Ctx context.Context
}

// NewMetricsReporter creaters a Reporter for generic metrics.
func NewMetricsReporter() (*Reporter, error) {
	ctx, err := tag.New(
		context.Background(),
	)
	if err != nil {
		return nil, err
	}

	return &Reporter{Ctx: ctx}, nil
}
