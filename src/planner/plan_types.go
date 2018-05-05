/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

// PlanType type.
type PlanType string

const (
	// PlanTypeDDL enum.
	PlanTypeDDL PlanType = "PlanTypeDDL"

	// PlanTypeInsert enum.
	PlanTypeInsert PlanType = "PlanTypeInsert"

	// PlanTypeDelete enum.
	PlanTypeDelete PlanType = "PlanTypeDelete"

	// PlanTypeUpdate enum.
	PlanTypeUpdate PlanType = "PlanTypeUpdate"

	// PlanTypeSelect enum.
	PlanTypeSelect PlanType = "PlanTypeSelect"

	// PlanTypeOrderby enum.
	PlanTypeOrderby PlanType = "PlanTypeOrderby"

	// PlanTypeLimit enum.
	PlanTypeLimit PlanType = "PlanTypeLimit"

	// PlanTypeAggregate enum.
	PlanTypeAggregate PlanType = "PlanTypeAggregate"

	// PlanTypeJoin enum.
	PlanTypeJoin PlanType = "PlanTypeJoin"

	// PlanTypeDistinct enum.
	PlanTypeDistinct PlanType = "PlanTypeDistinct"
)
