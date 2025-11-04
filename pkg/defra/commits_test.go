package defra

import (
	"context"
	"fmt"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestGetLatestCommit(t *testing.T) {
	ctx := context.Background()
	testDefra := startAndSeedDefraNode(t)
	defer testDefra.Close(ctx)

	docId, err := getDocId(ctx, testDefra)
	require.NoError(t, err)
	require.Greater(t, len(docId), 0)

	commit, err := GetLatestCommit(ctx, testDefra, docId)
	require.NoError(t, err)
	require.NotNil(t, commit)
}

type UserWithDocId struct {
	Name  string `json:"name"`
	DocId string `json:"_docID"`
}

func startAndSeedDefraNode(t *testing.T) *node.Node {
	logger.Init(true)

	defraNode, err := defra.StartDefraInstanceWithTestConfig(t, defra.DefaultConfig, defra.NewSchemaApplierFromProvidedSchema("type User {name: String}"), "User")
	require.NoError(t, err)

	mutation := `mutation {
			create_User(input: { name: "Quinn" }) {
				name
			}
		}`
	result, err := defra.PostMutation[UserWithDocId](t.Context(), defraNode, mutation)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "Quinn", result.Name)

	return defraNode
}

func getDocId(ctx context.Context, defraNode *node.Node) (string, error) {
	query := `query getUser { User(limit:1) { _docID name } }`
	user, err := defra.QuerySingle[UserWithDocId](ctx, defraNode, query)
	if err != nil {
		return "", fmt.Errorf("Error fetching block: %v", err)
	}
	if len(user.DocId) == 0 {
		return "", fmt.Errorf("Unable to retrieve docID from user %+v", user)
	}

	return user.DocId, nil
}
