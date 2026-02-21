package processor

import (
	"context"

	"github.com/jdiitm/graphrag-architect/workers/ingestion/internal/domain"
)

type DocumentProcessor interface {
	Process(ctx context.Context, job domain.Job) error
}
