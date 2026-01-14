package defra

import (
	"context"
	"fmt"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
)

func GetLatestCommitsQuery(docId string) string {
	return fmt.Sprintf(`query {
	_commits(docID: "%s") {
		collectionVersionId
		heads {
			cid
			docID
			fieldName
		}
		links {
			cid
			docID
			signature {
				value
				identity
			}
		}
	}
}`, docId)
}

type CommitSignature struct {
	Value    string `json:"value"`
	Identity string `json:"identity"`
}

type CommitLink struct {
	Cid       string          `json:"cid"`
	DocID     string          `json:"docID"`
	Signature CommitSignature `json:"signature"`
}

type CommitHead struct {
	Cid       string `json:"cid"`
	DocID     string `json:"docID"`
	FieldName string `json:"fieldName"`
}

type LatestCommits struct {
	Commits []Commit `json:"_commits"`
}

type Commit struct {
	SchemaVersionId string       `json:"collectionVersionId"`
	Heads           []CommitHead `json:"heads"`
	Links           []CommitLink `json:"links"`
}

func GetLatestCommit(ctx context.Context, defraNode *node.Node, docId string) (*Commit, error) {
	query := GetLatestCommitsQuery(docId)
	resp, err := defra.QuerySingle[LatestCommits](ctx, defraNode, query)
	if err != nil {
		return nil, fmt.Errorf("Error getting latest commit: %v", err)
	}
	if len(resp.Commits) == 0 {
		return nil, fmt.Errorf("Error getting latest commit: no commits found")
	}

	// Since there's no height field in the actual schema, just return the first commit
	// In a real implementation, you might sort by collectionVersionId or another field
	latest := resp.Commits[0]
	return &latest, nil
}
