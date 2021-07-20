package watch

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"github.com/open-policy-agent/gatekeeper/pkg/syncutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

type replayRequest struct {
	r        *Registrar
	gvk      schema.GroupVersionKind
	isCancel bool // If true, this is a request to cancel a pending replay.
}

type cancelMap map[*Registrar]context.CancelFunc

func (m *cancelMap) Set(r *Registrar, c context.CancelFunc) {
	if *m == nil {
		*m = make(cancelMap)
	}
	(*m)[r] = c
}

// replayEventsLoop processes requests to start and stop replaying events for
// registrars that join an existing informer and need historical data.
// Events for each registrar will be listed and replayed in an independent goroutine.
func (wm *Manager) replayEventsLoop(ctx context.Context) func() error {
	return func() error {
		var wg sync.WaitGroup
		defer wg.Wait()

		ctx, cancel := syncutil.ContextForChannel(ctx, wm.stopped)
		defer cancel()

		// Entries remain until a watch is removed.
		m := make(map[schema.GroupVersionKind]cancelMap)

		for {
			select {
			case <-ctx.Done():
				return nil
			case req := <-wm.replayRequests:
				if req.r == nil {
					log.Info("skipping replay for nil registrar")
					continue
				}
				log := log.WithValues("registrar", req.r.parentName, "gvk", req.gvk)

				byRegistrar := m[req.gvk]
				c, inProgress := byRegistrar[req.r]

				// Handle cancellation requests
				if req.isCancel && inProgress {
					log.Info("canceling replay")
					delete(byRegistrar, req.r)
					if len(byRegistrar) == 0 {
						delete(m, req.gvk)
					}

					if c == nil {
						continue
					}
					// Cancel the pending replay.
					// Note we do not wait for the individual goroutine to complete,
					// but we will sync on all of them when stopping the watch manager.
					c()
					continue
				}

				if req.isCancel || inProgress {
					// Replay in progress or cancel request, either way, do not proceed to replay again.
					continue
				}

				if req.r.events == nil {
					log.Info("skipping replay: can't deliver to nil channel")
					continue
				}

				// Handle replay requests
				log.Info("replaying events")
				childCtx, childCancel := context.WithCancel(ctx)
				byRegistrar.Set(req.r, childCancel)
				m[req.gvk] = byRegistrar
				wg.Add(1)
				go func(group *sync.WaitGroup, ctx context.Context, cancel context.CancelFunc, log logr.Logger) {
					defer wg.Done()
					defer cancel()

					err := wait.ExponentialBackoff(retry.DefaultBackoff, func() (bool, error) {
						err := wm.replayEvents(childCtx, req.r, req.gvk)
						if err != nil && err != context.Canceled {
							// Log and retry w/ backoff
							log.Error(err, "replaying events")
							return false, nil
						}
						if err == context.Canceled {
							// Give up
							return false, err
						}
						// Success
						return true, nil
					})
					if err != nil && err != context.Canceled {
						log.Error(err, "replaying events")
					}
				}(&wg, childCtx, childCancel, log)
			}
		}
	}
}

// requestReplay sends a request to replayEventsLoop to start replaying for the specified registrar.
// If a replay is in progress, this is a no-op.
// NOTE: blocks if the manager is not running.
func (wm *Manager) requestReplay(r *Registrar, gvk schema.GroupVersionKind) {
	req := replayRequest{r: r, gvk: gvk}
	select {
	case wm.replayRequests <- req:
	case <-wm.stopped:
	}
}

// requestReplay sends a request to replayEventsLoop to cancel replaying for the specified registrar.
// If no replay is in progress, this is a no-op.
// NOTE: blocks if the manager is not running.
func (wm *Manager) cancelReplay(r *Registrar, gvk schema.GroupVersionKind) {
	req := replayRequest{r: r, gvk: gvk, isCancel: true}
	select {
	case wm.replayRequests <- req:
	case <-wm.stopped:
	}
}

// replayEvents replays all resources of type gvk currently in the cache to the requested registrar.
// This is called when a registrar begins watching an existing informer.
func (wm *Manager) replayEvents(ctx context.Context, r *Registrar, gvk schema.GroupVersionKind) error {
	c := wm.cache
	if c == nil {
		return fmt.Errorf("nil cache")
	}
	if r == nil {
		return fmt.Errorf("nil registrar")
	}
	if r.events == nil {
		// Skip replay if there's no channel to deliver to
		return nil
	}

	lst := &unstructured.UnstructuredList{}
	lst.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	})
	if err := c.List(ctx, lst); err != nil {
		return fmt.Errorf("listing resources %+v: %w", gvk, err)
	}

	for _, o := range lst.Items {
		o := o
		e := event.GenericEvent{
			Object: &o,
		}
		select {
		case r.events <- e:
		case <-ctx.Done():
			return context.Canceled
		case <-wm.stopped:
			return context.Canceled
		}
	}
	return nil
}
