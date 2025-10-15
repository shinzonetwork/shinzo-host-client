package defra

import (
	"context"
	"fmt"

	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/sourcenetwork/defradb/node"
)

func GetLatestCommitsQuery(docId string) string {
	return fmt.Sprintf(`query {
	latestCommits(docID: "%s") {
	cid
	delta
	height
	links {
	  cid
	  name
	}
  }
}`, docId)
}

type CommitLink struct {
	Cid  string `json:"cid"`
	Name string `json:"name"`
}

type LatestCommits struct {
	Commits []Commit `json:"latestCommits"`
}

type Commit struct {
	Cid    string       `json:"cid"`
	Delta  string       `json:"delta"`
	Height int          `json:"height"`
	Links  []CommitLink `json:"links"`
}

func GetLatestCommit(ctx context.Context, defraNode *node.Node, docId string) (*Commit, error) {
	query := GetLatestCommitsQuery(docId)
	commit, err := defra.QuerySingle[Commit](ctx, defraNode, query)
	if err != nil {
		return nil, fmt.Errorf("Error getting latest commit: %v", err)
	}

	return &commit, nil
}
