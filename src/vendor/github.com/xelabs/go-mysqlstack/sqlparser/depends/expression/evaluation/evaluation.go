package evaluation

import (
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/datum"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// Evaluation interface.
type Evaluation interface {
	FixField(fields map[string]*querypb.Field) (*datum.IField, error)
	Update(values map[string]datum.Datum) (datum.Datum, error)
	Result() datum.Datum
}
