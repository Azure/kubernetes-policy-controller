package gktest

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Suite defines a set of Constraint tests.
type Suite struct {
	metav1.ObjectMeta

	// Tests is a list of Template&Constraint pairs, with tests to run on
	// each.
	Tests []Test
}

// Test defines a Template&Constraint pair to instantiate, and Cases to
// run on the instantiated Constraint.
type Test struct {
	// Template is the path to the ConstraintTemplate, relative to the file
	// defining the Suite.
	Template string

	// Constraint is the path to the Constraint, relative to the file defining
	// the Suite. Must be an instance of Template.
	Constraint string

	// Cases are the test cases to run on the instantiated Constraint.
	Cases []Case
}

// Case runs Constraint against a YAML object.
type Case struct{}
