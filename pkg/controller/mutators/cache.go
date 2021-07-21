package mutators

import (
	"sync"

	"github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
)

type Cache struct {
	cache map[types.ID]MutatorIngestionStatus
	mux   sync.RWMutex
}

func NewMutationCache() *Cache {
	return &Cache{
		cache: make(map[types.ID]MutatorIngestionStatus),
	}
}

func (c *Cache) Upsert(mID types.ID, status MutatorIngestionStatus) {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.cache[mID] = status
}

func (c *Cache) Remove(mID types.ID) {
	c.mux.Lock()
	defer c.mux.Unlock()

	delete(c.cache, mID)
}

func (c *Cache) Tally() map[MutatorIngestionStatus]int {
	c.mux.RLock()
	defer c.mux.RUnlock()

	statusTally := make(map[MutatorIngestionStatus]int)
	for _, status := range c.cache {
		statusTally[status]++
	}
	return statusTally
}
