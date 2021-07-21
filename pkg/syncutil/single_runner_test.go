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
	"testing"
	"time"
)

func Test_SingleRunner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	syncOne := make(chan struct{})
	syncTwo := make(chan struct{})

	r := RunnerWithContext(ctx)

	wg.Add(1)
	r.Go(ctx, "one", func(ctx context.Context) error {
		defer wg.Done()
		defer close(syncOne)
		<-ctx.Done()
		return nil
	})

	// Repeat key won't be scheduled.
	r.Go(ctx, "one", func(ctx context.Context) error {
		t.Fatal("repeat key will never be scheduled")
		return nil
	})

	wg.Add(1)
	r.Go(ctx, "two", func(ctx context.Context) error {
		defer wg.Done()
		defer close(syncTwo)
		<-ctx.Done()
		return nil
	})

	select {
	case <-syncTwo:
		t.Fatalf("two should not have been canceled yet")
	case <-time.After(10 * time.Millisecond):
	}

	// Show we can cancel a routine by key
	r.Cancel("two")
	<-syncTwo

	select {
	case <-syncOne:
		t.Fatalf("one should not have been canceled yet")
	case <-time.After(10 * time.Millisecond):
	}

	cancel()
	wg.Wait()
}
