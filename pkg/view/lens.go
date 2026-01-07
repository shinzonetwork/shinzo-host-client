package view

import (
	"context"
	"fmt"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
	"github.com/sourcenetwork/lens/host-go/config/model"
)

type LensService interface {
	SetMigration(ctx context.Context, defraNode *node.Node, config client.LensConfig) (string, error)
}

type lensService struct {
	defraNode *node.Node
}

func NewLensService(defraNode *node.Node) LensService {
	return &lensService{defraNode: defraNode}
}

func (ls *lensService) SetMigration(ctx context.Context, defraNode *node.Node, config client.LensConfig) (string, error) {
	lensID, err := defraNode.DB.SetMigration(ctx, config)
	if err != nil {
		return "", fmt.Errorf("failed to set migration: %w", err)
	}
	return lensID, nil
}

// build lens config for a transformation between query and sdl
// query: the source collection (e.g. " Block { _docID _version { cid signature { value identity type } } hash blockNumber from to value } ")
// sdl: the output schema (e.g. " FilteredBlock { _docID _version { cid signature { value identity type } } hash blockNumber from to value } ")
// lens: filteredBlock
func BuildLensConfig(query, sdl string, wasmPath string, args map[string]any) client.LensConfig {
	return client.LensConfig{
		SourceSchemaVersionID:      query,
		DestinationSchemaVersionID: sdl,
		Lens: model.Lens{
			Lenses: []model.LensModule{
				{
					Path:      wasmPath,
					Arguments: args,
				},
			},
		},
	}
}
