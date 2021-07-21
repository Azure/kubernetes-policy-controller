/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package syncutil

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

// SingleRunner wraps an errgroup to run keyed goroutines as singletons.
// Keys are single-use and subsequent usage to schedule will be silently ignored.
// Goroutines can be individually canceled provided they respect the context passed to them.
type SingleRunner struct {
	m   map[string]context.CancelFunc
	mu  sync.Mutex
	grp *errgroup.Group
}

// RunnerWithContext returns an initialized SingleRunner.
// The provided context is used as the parent of subsequently scheduled goroutines.
func RunnerWithContext(ctx context.Context) *SingleRunner {
	grp, _ := errgroup.WithContext(ctx)
	return &SingleRunner{
		grp: grp,
		m:   make(map[string]context.CancelFunc),
	}
}

// Wait waits for all goroutines managed by the SingleRunner to complete.
// Returns the first error returned from a managed goroutine, or nil.
func (s *SingleRunner) Wait() error {
	if s.grp == nil {
		return nil
	}
	err := s.grp.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.m {
		if c == nil {
			continue
		}
		c()
	}
	return err
}

// Go schedules the provided function on a new goroutine if the provided key has
// not been used for scheduling before.
func (s *SingleRunner) Go(ctx context.Context, key string, f func(context.Context) error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.m == nil {
		s.m = make(map[string]context.CancelFunc)
	}

	if _, ok := s.m[key]; ok {
		// Reject if already running
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	s.m[key] = cancel
	s.grp.Go(func() error {
		return f(ctx)
	})
}

// Cancel cancels a keyed goroutine if it exists.
func (s *SingleRunner) Cancel(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cancel := s.m[key]; cancel != nil {
		cancel()
		s.m[key] = nil
		// Leave the key in the map to prevent its re-use.
	}
}
