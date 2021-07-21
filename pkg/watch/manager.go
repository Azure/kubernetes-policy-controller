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

package watch

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var log = logf.Log.WithName("watch-manager")

// Manager allows us to dynamically configure what kinds are watched.
type Manager struct {
	cache      RemovableCache
	startedMux sync.Mutex
	stopped    chan struct{}
	// started is a bool
	started bool
	// managedKinds stores the kinds that should be managed, mapping CRD Kind to CRD Name
	managedKinds *recordKeeper
	watchedMux   sync.RWMutex
	// watchedKinds are the kinds that have a currently running constraint controller
	watchedKinds vitalsByGVK
	metrics      *reporter

	// replayTracker allows us to block until replays are complete (or fully canceled)
	replayTracker *replayTracker

	// Events are passed internally from informer event handlers to handleEvents for distribution.
	events chan interface{}
	// replayRequests is used to request or cancel replay for a registrar joining an existing watch.
	replayRequests chan replayRequest
}

type AddFunction func(manager.Manager) error

// RemovableCache is a subset variant of the cache.Cache interface.
// It supports non-blocking calls to get informers, as well as the
// ability to remove an informer dynamically.
type RemovableCache interface {
	GetInformerNonBlocking(obj client.Object) (cache.Informer, error)
	List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
	Remove(obj client.Object) error
}

func New(c RemovableCache) (*Manager, error) {
	metrics, err := newStatsReporter()
	if err != nil {
		return nil, err
	}
	recordKeeper, err := newRecordKeeper()
	if err != nil {
		return nil, err
	}
	wm := &Manager{
		cache:          c,
		stopped:        make(chan struct{}),
		managedKinds:   recordKeeper,
		watchedKinds:   make(vitalsByGVK),
		metrics:        metrics,
		events:         make(chan interface{}, 1024),
		replayRequests: make(chan replayRequest),
		replayTracker:  newReplayTracker(),
	}
	wm.managedKinds.mgr = wm
	return wm, nil
}

func (wm *Manager) NewRegistrar(parent string, events chan<- event.GenericEvent) (*Registrar, error) {
	return wm.managedKinds.NewRegistrar(parent, events)
}

// RemoveRegistrar removes a registrar and all its watches.
func (wm *Manager) RemoveRegistrar(parentName string) error {
	return wm.managedKinds.RemoveRegistrar(parentName)
}

// Start runs the watch manager, processing events received from dynamic informers and distributing them
// to registrars.
func (wm *Manager) Start(ctx context.Context) error {
	if err := wm.checkStarted(); err != nil {
		return err
	}

	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		<-ctx.Done()
		// Unblock any informer event handlers
		close(wm.stopped)
		return context.Canceled
	})
	// Routine for distributing events to listeners.
	grp.Go(func() error {
		wm.eventLoop(ctx.Done())
		return context.Canceled
	})
	// Routine for asynchronous replay of past events to joining listeners.
	grp.Go(wm.replayEventsLoop)
	_ = grp.Wait()
	return nil
}

func (wm *Manager) checkStarted() error {
	wm.startedMux.Lock()
	defer wm.startedMux.Unlock()
	if wm.started {
		return errors.New("already started")
	}
	wm.started = true
	return nil
}

func (wm *Manager) GetManagedGVK() []schema.GroupVersionKind {
	return wm.managedKinds.GetGVK()
}

func (wm *Manager) addWatch(r *Registrar, gvk schema.GroupVersionKind) error {
	wm.watchedMux.Lock()
	defer wm.watchedMux.Unlock()
	return wm.doAddWatch(r, gvk)
}

func (wm *Manager) doAddWatch(r *Registrar, gvk schema.GroupVersionKind) error {
	// lock acquired by caller

	if r == nil {
		return fmt.Errorf("nil registrar cannot watch")
	}

	// watchers is everyone who is *already* watching.
	watchers := wm.watchedKinds[gvk]

	// m is everyone who *wants* to watch.
	m := wm.managedKinds.Get() // Not a deadlock but beware if assumptions change...
	if _, ok := m[gvk]; !ok {
		return fmt.Errorf("could not mark %+v as managed", gvk)
	}

	// Sanity
	if !m[gvk].registrars[r] {
		return fmt.Errorf("registrar %s not in desired watch set", r.parentName)
	}

	if watchers.registrars[r] {
		// Already watching.
		return nil
	}

	switch {
	case len(watchers.registrars) > 0:
		// Someone else was watching, replay events in the cache to the new watcher.
		wm.requestReplay(r, gvk)
	default:
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		informer, err := wm.cache.GetInformerNonBlocking(u)
		if err != nil || informer == nil {
			// This is expected to fail if a CRD is unregistered.
			return fmt.Errorf("getting informer for kind: %+v %w", gvk, err)
		}

		// First watcher gets a fresh informer, register for events.
		informer.AddEventHandler(wm)
	}

	// Mark it as watched.
	wv := vitals{
		gvk:        gvk,
		registrars: map[*Registrar]bool{r: true},
	}
	wm.watchedKinds[gvk] = watchers.merge(wv)
	if err := wm.metrics.reportGvkCount(int64(len(wm.watchedKinds))); err != nil {
		log.Error(err, "while trying to report gvk count metric")
	}
	return nil
}

func (wm *Manager) removeWatch(r *Registrar, gvk schema.GroupVersionKind) error {
	wm.watchedMux.Lock()
	defer wm.watchedMux.Unlock()
	return wm.doRemoveWatch(r, gvk)
}

func (wm *Manager) doRemoveWatch(r *Registrar, gvk schema.GroupVersionKind) error {
	// lock acquired by caller

	v, ok := wm.watchedKinds[gvk]
	if !ok || !v.registrars[r] {
		// Not watching.
		return nil
	}

	// Cancel any replays that may be pending
	wm.cancelReplay(r, gvk)

	// Remove this registrar from the watch list
	delete(v.registrars, r)

	// Skip if there are additional watchers that would prevent us from removing it
	if len(v.registrars) > 0 {
		return nil
	}

	log.Info("all watches removed for gvk, waiting for replays to end", "gvk", gvk)

	// Wait until all replays have exited before canceling the watch,
	// otherwise the list may unintentionally restart a watch
	select {
	case <-wm.replayTracker.replayWaitCh(gvk):
	case <-wm.stopped:
	}

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	if err := wm.cache.Remove(u); err != nil {
		return fmt.Errorf("removing %+v: %w", gvk, err)
	}
	delete(wm.watchedKinds, gvk)
	if err := wm.metrics.reportGvkCount(int64(len(wm.watchedKinds))); err != nil {
		log.Error(err, "while trying to report gvk count metric")
	}
	log.Info("watch removed", "gvk", gvk)
	return nil
}

// replaceWatches ensures all and only desired watches are running.
func (wm *Manager) replaceWatches(r *Registrar) error {
	wm.watchedMux.Lock()
	defer wm.watchedMux.Unlock()

	var errlist errorList

	desired := wm.managedKinds.Get()
	for gvk := range wm.watchedKinds {
		if v, ok := desired[gvk]; ok && v.registrars[r] {
			// This registrar still desires this gvk, skip.
			continue
		}
		if err := wm.doRemoveWatch(r, gvk); err != nil {
			errlist = append(errlist, fmt.Errorf("removing watch for %+v %w", gvk, err))
		}
	}

	// Add desired watches. This is idempotent for existing watches.
	for gvk, v := range desired {
		if !v.registrars[r] {
			continue
		}
		if err := wm.doAddWatch(r, gvk); err != nil {
			errlist = append(errlist, fmt.Errorf("adding watch for %+v %w", gvk, err))
		}
	}

	if err := wm.metrics.reportGvkCount(int64(len(wm.watchedKinds))); err != nil {
		log.Error(err, "while trying to report gvk count metric")
	}

	if errlist != nil {
		return errlist
	}
	return nil
}

// OnAdd implements cache.ResourceEventHandler. Called by informers.
func (wm *Manager) OnAdd(obj interface{}) {
	// Send event to eventLoop() for processing
	select {
	case wm.events <- obj:
	case <-wm.stopped:
	}
}

// OnUpdate implements cache.ResourceEventHandler. Called by informers.
func (wm *Manager) OnUpdate(oldObj, newObj interface{}) {
	// Send event to eventLoop() for processing
	select {
	case wm.events <- oldObj:
	case <-wm.stopped:
	}
	select {
	case wm.events <- newObj:
	case <-wm.stopped:
	}
}

// OnDelete implements cache.ResourceEventHandler. Called by informers.
func (wm *Manager) OnDelete(obj interface{}) {
	// Send event to eventLoop() for processing
	select {
	case wm.events <- obj:
	case <-wm.stopped:
	}
}

// eventLoop receives events from informer callbacks and distributes them to registrars.
func (wm *Manager) eventLoop(stop <-chan struct{}) {
	for {
		select {
		case e, ok := <-wm.events:
			if !ok {
				return
			}
			wm.distributeEvent(stop, e)
		case <-stop:
			return
		}
	}
}

// distributeEvent distributes a single event to all registrars listening for that resource kind.
func (wm *Manager) distributeEvent(stop <-chan struct{}, obj interface{}) {
	o, ok := obj.(client.Object)
	if !ok || o == nil {
		// Invalid object, drop it
		return
	}
	gvk := o.GetObjectKind().GroupVersionKind()
	e := event.GenericEvent{
		Object: o,
	}

	// Critical lock section
	var watchers []chan<- event.GenericEvent
	func() {
		wm.watchedMux.RLock()
		defer wm.watchedMux.RUnlock()

		r, ok := wm.watchedKinds[gvk]
		if !ok {
			// Nobody is watching, drop it
			return
		}

		// TODO(OREN) reduce allocations here
		watchers = make([]chan<- event.GenericEvent, 0, len(r.registrars))
		for w := range r.registrars {
			if w.events == nil {
				continue
			}
			watchers = append(watchers, w.events)
		}
	}()

	// Distribute the event
	for _, w := range watchers {
		select {
		case w <- e:
		// TODO(OREN) add timeout
		case <-stop:
		}
	}
}
