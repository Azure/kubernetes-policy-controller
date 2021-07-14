package schema

import (
	"fmt"
	"sync"

	"github.com/open-policy-agent/gatekeeper/pkg/mutation/types"
	"k8s.io/apimachinery/pkg/runtime/schema"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// MutatorWithSchema is a mutator exposing the implied
// schema of the target object.
type MutatorWithSchema interface {
	types.Mutator

	// SchemaBindings returns the set of GVKs this Mutator applies to.
	SchemaBindings() []schema.GroupVersionKind
}

var log = logf.Log.WithName("mutation_schema")

// New returns a new schema database.
func New() *DB {
	return &DB{
		cachedMutators: make(map[types.ID]MutatorWithSchema),
		schemas:        make(map[schema.GroupVersionKind]*node),
		conflicts:      make(map[types.ID]bool),
	}
}

// DB is a database that caches all the implied schemas.
// Returns an error when upserting a mutator which conflicts with the existing ones.
//
// Mutators implicitly define a part of the schema of the object they intend
// to mutate. For example, modifying `spec.containers[name: foo].image` implies
// that:
// - spec is an object
// - containers is a list
// - image exists, but no type information
//
// If another mutator on the same GVK declares that it modifies
// `spec.containers.image`, then we know we have a contradiction as that path
// implies containers is an object.
//
// Conflicting schemas are stored within DB. HasConflicts returns true for
// the IDs of Mutators with conflicting schemas until Remove() is called on
// the Mutators which conflict with the ID.
type DB struct {
	mutex sync.RWMutex

	// cachedMutators is a cache of all seen Mutators.
	cachedMutators map[types.ID]MutatorWithSchema

	// schemas are the per-GVK implicit schemas.
	schemas map[schema.GroupVersionKind]*node

	conflicts map[types.ID]bool
}

// Upsert inserts or updates the given mutator.
// Returns an error if the implicit schema in mutator conflicts with any
// mutators previously added.
//
// Schema conflicts are only detected using mutator.Path() - DB does not check
// that assigned types are compatible. For example, one Mutator might assign
// a string to a path and another might assign a list.
func (db *DB) Upsert(mutator MutatorWithSchema) error {
	if mutator == nil {
		return ErrNilMutator
	}
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.upsert(mutator)
}

func (db *DB) upsert(mutator MutatorWithSchema) error {
	id := mutator.ID()
	if oldMutator, ok := db.cachedMutators[id]; ok {
		if !mutator.HasDiff(oldMutator) {
			// We've already added a Mutator which has the same path and bindings, so
			// there's nothing to do.
			return nil
		}
		db.remove(id)
	}

	path := mutator.Path()
	bindings := mutator.SchemaBindings()
	var ok bool
	mutatorCopy := mutator.DeepCopy()
	db.cachedMutators[id], ok = mutatorCopy.(MutatorWithSchema)
	if !ok {
		panic(fmt.Sprintf("got mutator.DeepCopy() type %T, want %T", mutatorCopy, MutatorWithSchema(nil)))
	}

	var conflicts idSet
	for _, gvk := range bindings {
		s, ok := db.schemas[gvk]
		if !ok {
			s = &node{}
			db.schemas[gvk] = s
		}
		newConflicts := s.Add(id, path.Nodes)
		conflicts = merge(conflicts, newConflicts)
	}

	for c := range conflicts {
		db.conflicts[c] = true
	}
	if len(conflicts) > 0 {
		// Adding this mutator had schema conflicts with another, so return an error.
		return fmt.Errorf("%w: the following Mutators have conflicting schemas: %s",
			ErrConflictingSchema, conflicts.String())
	}
	return nil
}

// Remove removes the mutator with the given id from the db.
func (db *DB) Remove(id types.ID) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.remove(id)
}

func (db *DB) remove(id types.ID) {
	cachedMutator, found := db.cachedMutators[id]
	if !found {
		return
	}

	for _, gvk := range cachedMutator.SchemaBindings() {
		s, ok := db.schemas[gvk]
		if !ok {
			// This means there's a bug in the schema code. This means a mutator
			// is bound to this gvk with a previous call to upsert, but for some
			// reason there is no corresponding schema.
			log.Error(nil, "mutator associated with missing schema", "mutator", id, "schema", gvk)
			panic(fmt.Sprintf("mutator %v associated with missing schema %v", id, gvk))
		}
		s.Remove(id, cachedMutator.Path().Nodes)
		db.schemas[gvk] = s

		if len(s.ReferencedBy) == 0 {
			// The root node is no longer referenced by any mutators.
			delete(db.schemas, gvk)
		}
	}

	// Remove the mutator from the cache.
	delete(db.cachedMutators, id)

	// This ID's conflicts are resolved since the ID no longer exists.
	delete(db.conflicts, id)

	// Check existing conflicts.
	// TODO: Determine if there's a way of narrowing the list of potential conflicts.
	for conflictID := range db.conflicts {
		// Check all current conflicts to see if they have been resolved.
		// This optimizes for calls to HasConflicts()
		mutator := db.cachedMutators[conflictID]
		hasConflict := false
		for _, gvk := range mutator.SchemaBindings() {
			if isConflict, _ := db.schemas[gvk].HasConflicts(mutator.Path().Nodes); isConflict {
				hasConflict = true
				break
			}
		}
		// Only remove the conflict if all types now report there is no conflict
		// at the path.
		if !hasConflict {
			delete(db.conflicts, conflictID)
		}
	}
}

// HasConflicts returns true if the Mutator of the passed ID has been upserted
// in DB and has conflicts with another Mutator. Returns false if the Mutator
// does not exist.
func (db *DB) HasConflicts(id types.ID) bool {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.conflicts[id]
}
