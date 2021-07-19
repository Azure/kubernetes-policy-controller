package mutation

import (
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/reporter"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
)

type mutatorCache map[types.ID]reporter.MutatorIngestionStatus
