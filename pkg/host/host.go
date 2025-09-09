package host

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/shinzonetwork/indexer/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
)

func StartHosting(defraStorePath string, defraUrl string) error {
	ctx := context.Background()

	if defraStorePath != "" {
		options := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(defraStorePath),
		}
		defraNode, err := node.New(ctx, options...)
		if err != nil {
			return fmt.Errorf("Failed to create defra node %v: ", err)
		}

		err = defraNode.Start(ctx)
		if err != nil {
			return fmt.Errorf("Failed to start defra node %v: ", err)
		}
		defer defraNode.Close(ctx)

		err = applySchema(ctx, defraNode)
		if err != nil && !strings.HasPrefix(err.Error(), "collection already exists") { // Todo we are swallowing this error for now, but we should investigate how we update the schemas - do we need to not swallow this error?
			return fmt.Errorf("Failed to apply schema to defra node: %v", err)
		}

		err = defra.WaitForDefraDB(defraUrl)
		if err != nil {
			return err
		}
	}

	// Todo connect to the indexers and sync primitives

	// Todo process dataviews

	return nil
}

// Todo - we'll have to update this to include the policy id (which we should get back from ShinzoHub during registration)
func applySchema(ctx context.Context, defraNode *node.Node) error {
	fmt.Println("Applying schema...")

	// Try different possible paths for the schema file
	possiblePaths := []string{
		"schema/schema.graphql",       // From project root
		"../schema/schema.graphql",    // From bin/ directory
		"../../schema/schema.graphql", // From pkg/host/ directory - test context
	}

	var schemaPath string
	var err error
	for _, path := range possiblePaths {
		if _, err = os.Stat(path); err == nil {
			schemaPath = path
			break
		}
	}

	if schemaPath == "" {
		return fmt.Errorf("Failed to find schema file in any of the expected locations: %v", possiblePaths)
	}

	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("Failed to read schema file: %v", err)
	}

	_, err = defraNode.DB.AddSchema(ctx, string(schema))
	return err
}
