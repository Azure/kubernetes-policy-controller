package mutation

import (
	"sync"

	"github.com/open-policy-agent/gatekeeper/pkg/mutation/reporter"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
)

type Cache struct {
	cache map[types.ID]reporter.MutatorIngestionStatus
	mux   sync.RWMutex
}

func NewMutationCache() *Cache {
	return &Cache{
		cache: make(map[types.ID]reporter.MutatorIngestionStatus),
	}
}

func (c *Cache) Upsert(mID types.ID, status reporter.MutatorIngestionStatus) {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.cache[mID] = status
}

func (c *Cache) Remove(mID types.ID) {
	c.mux.Lock()
	defer c.mux.Unlock()

	delete(c.cache, mID)
}

func (c *Cache) Tally() map[reporter.MutatorIngestionStatus]int {
	c.mux.RLock()
	defer c.mux.RUnlock()

	statusTally := make(map[reporter.MutatorIngestionStatus]int)
	for _, status := range c.cache {
		statusTally[status]++
	}
	return statusTally
}
