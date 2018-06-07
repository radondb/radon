/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import ()

// Plan interface.
type Plan interface {
	Build() error
	Type() PlanType
	JSON() string
	Size() int
	Children() *PlanTree
}

// PlanTree is a container for all plans
type PlanTree struct {
	size     int
	children []Plan
}

// NewPlanTree creates the new plan tree.
func NewPlanTree() *PlanTree {
	return &PlanTree{
		children: make([]Plan, 0, 8),
	}
}

// Add used to add new plan to the tree.
func (pt *PlanTree) Add(plan Plan) error {
	pt.children = append(pt.children, plan)
	pt.size += plan.Size()
	return nil
}

// Build used to build plans(we won't build sub-plans in this plan).
func (pt *PlanTree) Build() error {
	for _, plan := range pt.children {
		if err := plan.Build(); err != nil {
			return err
		}
	}
	return nil
}

// Plans returns all the plans of the tree.
func (pt *PlanTree) Plans() []Plan {
	return pt.children
}

// Size used to measure the memory usage for this plantree.
func (pt *PlanTree) Size() int {
	return pt.size
}
