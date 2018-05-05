/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package optimizer

import (
	"planner"
)

// Optimizer interface.
type Optimizer interface {
	BuildPlanTree() (*planner.PlanTree, error)
}
