package watch

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestReplayTrackerBlocking(t *testing.T) {
	tracker := newReplayTracker()
	gvk := schema.GroupVersionKind{Group: "hello", Version: "v1", Kind: "hola"}
	oGVK := schema.GroupVersionKind{Group: "foo", Version: "v1", Kind: "bar"}

	t.Run("No replay, no waiting", func(t *testing.T) {
		blocked := false
		select {
		case <-tracker.replayWaitCh(gvk):
		default:
			blocked = true
		}
		if blocked {
			t.Error("unexpected wait")
		}
	})

	t.Run("Replay -> waiting", func(t *testing.T) {
		tracker.addReplay(gvk)
		blocked := false
		select {
		case <-tracker.replayWaitCh(gvk):
		default:
			blocked = true
		}
		if !blocked {
			t.Error("should be blocked")
		}
	})

	t.Run("Replay != waiting for other gvks", func(t *testing.T) {
		blocked := false
		select {
		case <-tracker.replayWaitCh(oGVK):
		default:
			blocked = true
		}
		if blocked {
			t.Error("unexpected wait")
		}
	})

	t.Run("other GVK also blocks", func(t *testing.T) {
		tracker.addReplay(oGVK)
		blocked := false
		select {
		case <-tracker.replayWaitCh(oGVK):
		default:
			blocked = true
		}
		if !blocked {
			t.Error("should be blocked")
		}
	})

	t.Run("First done does not close channel", func(t *testing.T) {
		tracker.addReplay(gvk)
		tracker.replayDone(gvk)
		blocked := false
		select {
		case <-tracker.replayWaitCh(gvk):
		default:
			blocked = true
		}
		if !blocked {
			t.Error("should be blocked")
		}
	})

	t.Run("Second done does close channel", func(t *testing.T) {
		tracker.replayDone(gvk)
		blocked := false
		select {
		case <-tracker.replayWaitCh(gvk):
		default:
			blocked = true
		}
		if blocked {
			t.Error("unexpected wait")
		}
	})

	t.Run("Other GVK still blocked", func(t *testing.T) {
		blocked := false
		select {
		case <-tracker.replayWaitCh(oGVK):
		default:
			blocked = true
		}
		if !blocked {
			t.Error("should be blocked")
		}
	})

	t.Run("Other GVK can unblock", func(t *testing.T) {
		tracker.replayDone(oGVK)
		blocked := false
		select {
		case <-tracker.replayWaitCh(oGVK):
		default:
			blocked = true
		}
		if blocked {
			t.Error("unexpected wait")
		}
	})

	t.Run("State is cleaned", func(t *testing.T) {
		if len(tracker.channels) != 0 {
			t.Errorf("len(tracker.channels) = %d, expected 0", len(tracker.channels))
		}
		if len(tracker.counts) != 0 {
			t.Errorf("len(tracker.counts) = %d, expected 0", len(tracker.counts))
		}
	})
}

func TestReplayTrackerIntent(t *testing.T) {
	tracker := newReplayTracker()
	gvk := schema.GroupVersionKind{Group: "hello", Version: "v1", Kind: "hola"}
	oGVK := schema.GroupVersionKind{Group: "foo", Version: "v1", Kind: "bar"}
	reg := &Registrar{}

	t.Run("Zero state => no intent", func(t *testing.T) {
		if tracker.replayIntended(reg, gvk) {
			t.Error("tracker.replayIntended(reg, gvk) = true, wanted false")
		}
	})

	t.Run("Replay intent honored", func(t *testing.T) {
		tracker.setIntent(reg, gvk, true)
		if !tracker.replayIntended(reg, gvk) {
			t.Error("tracker.replayIntended(reg, gvk) = false, wanted true")
		}
		tracker.setIntent(reg, oGVK, true)
		if !tracker.replayIntended(reg, oGVK) {
			t.Error("tracker.replayIntended(reg, oGVK) = false, wanted true")
		}
	})

	t.Run("Intent can be canceled", func(t *testing.T) {
		tracker.setIntent(reg, gvk, false)
		if tracker.replayIntended(reg, gvk) {
			t.Error("tracker.replayIntended(reg, gvk) = true, wanted false")
		}
		if !tracker.replayIntended(reg, oGVK) {
			t.Error("tracker.replayIntended(reg, oGVK) = false, wanted true")
		}
	})

	t.Run("State is cleaned", func(t *testing.T) {
		tracker.setIntent(reg, oGVK, false)
		if tracker.replayIntended(reg, oGVK) {
			t.Error("tracker.replayIntended(reg, oGVK) = true, wanted false")
		}

		if len(tracker.intent[reg]) != 0 {
			t.Errorf("len(tracker.intent[reg])  = %d, wanted 0", len(tracker.intent[reg]))
		}
	})
}
