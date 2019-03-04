package executor

import "xcontext"

// PlanExecutor interface.
type PlanExecutor interface {
	execute(reqCtx *xcontext.RequestContext, ctx *xcontext.ResultContext) error
}
