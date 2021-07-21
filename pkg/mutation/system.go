package mutation

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/open-policy-agent/gatekeeper/pkg/logging"
	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/schema"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// System keeps the list of mutators and provides an interface to apply mutations.
type System struct {
	schemaDB        schema.DB
	orderedMutators []types.Mutator
	mutatorsMap     map[types.ID]types.Mutator
	mux             sync.RWMutex
	reporter        *metrics.Reporter
	metricsHooks    SystemReporter
}

// JULIAN - I can remove this interface once I've reattched the mutation stats_reporter call to a
// reporter object.  Than I can just expect a pointer to that object and it will support being nil.
type SystemReporter interface {
	ReportIterationConvergence(r *metrics.Reporter, scs SystemConvergenceStatus, iterations int) error
}

// NewSystem initializes an empty mutation system.
func NewSystem(reporter *metrics.Reporter) *System {
	// JULIAN - metricsHooks can be nil.  Should I throw an error for that?
	return &System{
		schemaDB:        *schema.New(),
		orderedMutators: make([]types.Mutator, 0),
		mutatorsMap:     make(map[types.ID]types.Mutator),
		reporter:        reporter,
	}
}

func (s *System) InjectReporting(sr SystemReporter) {
	s.metricsHooks = sr
}

// Upsert updates or insert the given object, and returns
// an error in case of conflicts.
func (s *System) Upsert(m types.Mutator) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	current, ok := s.mutatorsMap[m.ID()]
	if ok && !m.HasDiff(current) {
		return nil
	}

	toAdd := m.DeepCopy()

	// Checking schema consistency only if the mutator has schema
	if withSchema, ok := toAdd.(schema.MutatorWithSchema); ok {
		err := s.schemaDB.Upsert(withSchema)
		if err != nil {
			s.schemaDB.Remove(withSchema.ID())
			return errors.Wrapf(err, "Schema upsert caused conflict for %v", m.ID())
		}
	}

	s.mutatorsMap[toAdd.ID()] = toAdd

	i := sort.Search(len(s.orderedMutators), func(i int) bool {
		return greaterOrEqual(s.orderedMutators[i].ID(), toAdd.ID())
	})

	if i == len(s.orderedMutators) { // Adding to the bottom of the list
		s.orderedMutators = append(s.orderedMutators, toAdd)
		return nil
	}

	found := equal(s.orderedMutators[i].ID(), toAdd.ID())
	if found {
		s.orderedMutators[i] = toAdd
		return nil
	}

	s.orderedMutators = append(s.orderedMutators, nil)
	copy(s.orderedMutators[i+1:], s.orderedMutators[i:])
	s.orderedMutators[i] = toAdd
	return nil
}

// Mutate applies the mutation in place to the given object. Returns
// true if a mutation was performed.
func (s *System) Mutate(obj *unstructured.Unstructured, ns *corev1.Namespace) (bool, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()
	mutationUUID := uuid.New()
	original := obj.DeepCopy()
	maxIterations := len(s.orderedMutators) + 1

	var allAppliedMutations [][]types.Mutator
	if *MutationLoggingEnabled || *MutationAnnotationsEnabled {
		allAppliedMutations = [][]types.Mutator{}
	}

	iterationsComplete := 0
	convergence := SystemConvergenceFalse
	defer func() {
		if s.reporter == nil || s.metricsHooks == nil {
			return
		}

		err := s.metricsHooks.ReportIterationConvergence(s.reporter, convergence, iterationsComplete)
		if err != nil {
			log.Error(err, "failed to report mutator ingestion request")
		}
	}()

	for i := 0; i < maxIterations; i++ {
		var appliedMutations []types.Mutator
		old := obj.DeepCopy()
		for _, m := range s.orderedMutators {
			if s.schemaDB.HasConflicts(m.ID()) {
				// Don't try to apply Mutators which have conflicts.
				continue
			}

			if m.Matches(obj, ns) {
				mutated, err := m.Mutate(obj)
				if mutated && (*MutationLoggingEnabled || *MutationAnnotationsEnabled) {
					appliedMutations = append(appliedMutations, m)
				}
				if err != nil {
					return false, errors.Wrapf(err, "mutation %s for mutator %v failed for %s %s %s %s",
						mutationUUID,
						m.ID(),
						obj.GroupVersionKind().Group,
						obj.GroupVersionKind().Kind,
						obj.GetNamespace(),
						obj.GetName())
				}
			}
		}
		if cmp.Equal(old, obj) {
			if i == 0 {
				// JULIAN - Is this right?  I believe that a system that doesn't do any mutations is converging.
				convergence = SystemConvergenceTrue
				return false, nil
			}
			if cmp.Equal(original, obj) {
				if *MutationLoggingEnabled {
					logAppliedMutations("Oscillating mutation.", mutationUUID, original, allAppliedMutations)
				}
				return false, fmt.Errorf("oscillating mutation for %s %s %s %s",
					obj.GroupVersionKind().Group,
					obj.GroupVersionKind().Kind,
					obj.GetNamespace(),
					obj.GetName())
			}
			if *MutationLoggingEnabled {
				logAppliedMutations("Mutation applied", mutationUUID, original, allAppliedMutations)
			}

			if *MutationAnnotationsEnabled {
				err := mutationAnnotations(obj, allAppliedMutations, mutationUUID)
				if err != nil {
					log.Error(err, "Error applying mutation annotations", "mutation id", mutationUUID)
				}
			}

			convergence = SystemConvergenceTrue
			return true, nil
		}
		if *MutationLoggingEnabled || *MutationAnnotationsEnabled {
			allAppliedMutations = append(allAppliedMutations, appliedMutations)
		}

		iterationsComplete = i
	}
	if *MutationLoggingEnabled {
		logAppliedMutations("Mutation not converging", mutationUUID, original, allAppliedMutations)
	}
	return false, fmt.Errorf("mutation %s not converging for %s %s %s %s",
		mutationUUID,
		obj.GroupVersionKind().Group,
		obj.GroupVersionKind().Kind,
		obj.GetNamespace(),
		obj.GetName())
}

func mutationAnnotations(obj *unstructured.Unstructured, allAppliedMutations [][]types.Mutator, mutationUUID uuid.UUID) error {
	mutatorStringSet := make(map[string]struct{})
	for _, mutationsForIteration := range allAppliedMutations {
		for _, mutator := range mutationsForIteration {
			mutatorStringSet[mutator.String()] = struct{}{}
		}
	}
	mutatorStrings := []string{}
	for mutatorString := range mutatorStringSet {
		mutatorStrings = append(mutatorStrings, mutatorString)
	}

	metadata, ok := obj.Object["metadata"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("Incorrect metadata type")
	}
	annotations, ok := metadata["annotations"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("Incorrect metadata type")
	}
	annotations["gatekeeper.sh/mutations"] = strings.Join(mutatorStrings, ", ")
	annotations["gatekeeper.sh/mutation-id"] = mutationUUID
	return nil
}

func logAppliedMutations(message string, mutationUUID uuid.UUID, obj *unstructured.Unstructured, allAppliedMutations [][]types.Mutator) {
	iterations := []interface{}{}
	for i, appliedMutations := range allAppliedMutations {
		if len(appliedMutations) == 0 {
			continue
		}
		var appliedMutationsText []string
		for _, mutator := range appliedMutations {
			appliedMutationsText = append(appliedMutationsText, mutator.String())
		}
		iterations = append(iterations, fmt.Sprintf("iteration_%d", i), strings.Join(appliedMutationsText, ", "))
	}
	if len(iterations) > 0 {
		logDetails := []interface{}{
			"Mutation Id", mutationUUID,
			logging.EventType, logging.MutationApplied,
			logging.ResourceGroup, obj.GroupVersionKind().Group,
			logging.ResourceKind, obj.GroupVersionKind().Kind,
			logging.ResourceAPIVersion, obj.GroupVersionKind().Version,
			logging.ResourceNamespace, obj.GetNamespace(),
			logging.ResourceName, obj.GetName(),
		}
		logDetails = append(logDetails, iterations...)
		log.Info(message, logDetails...)
	}
}

// Remove removes the mutator from the mutation system.
func (s *System) Remove(id types.ID) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if _, ok := s.mutatorsMap[id]; !ok {
		return nil
	}

	s.schemaDB.Remove(id)

	i := sort.Search(len(s.orderedMutators), func(i int) bool {
		res := equal(s.orderedMutators[i].ID(), id)
		if res {
			return true
		}
		return greaterOrEqual(s.orderedMutators[i].ID(), id)
	})

	delete(s.mutatorsMap, id)

	found := equal(s.orderedMutators[i].ID(), id)

	// The map is expected to be in sync with the list, so if we don't find it
	// we return an error.
	if !found {
		return fmt.Errorf("Failed to find mutator with ID %v on sorted list", id)
	}
	copy(s.orderedMutators[i:], s.orderedMutators[i+1:])
	s.orderedMutators[len(s.orderedMutators)-1] = nil
	s.orderedMutators = s.orderedMutators[:len(s.orderedMutators)-1]
	return nil
}

// Get mutator for given id.
func (s *System) Get(id types.ID) types.Mutator {
	mutator, found := s.mutatorsMap[id]
	if !found {
		return nil
	}
	return mutator.DeepCopy()
}

func greaterOrEqual(id1, id2 types.ID) bool {
	if id1.Group > id2.Group {
		return true
	}
	if id1.Group < id2.Group {
		return false
	}
	if id1.Kind > id2.Kind {
		return true
	}
	if id1.Kind < id2.Kind {
		return false
	}
	if id1.Namespace > id2.Namespace {
		return true
	}
	if id1.Namespace < id2.Namespace {
		return false
	}
	if id1.Name > id2.Name {
		return true
	}
	if id1.Name < id2.Name {
		return false
	}
	return true
}

func equal(id1, id2 types.ID) bool {
	if id1.Group == id2.Group &&
		id1.Kind == id2.Kind &&
		id1.Namespace == id2.Namespace &&
		id1.Name == id2.Name {
		return true
	}
	return false
}
