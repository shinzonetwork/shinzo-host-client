package view

import (
	"context"

	"github.com/shinzonetwork/app-sdk/pkg/views"
	"github.com/sourcenetwork/defradb/node"
)

type View views.View

func (v *View) SubscribeTo(ctx context.Context, defraNode *node.Node) error {
	subscribeableView := views.View(*v)
	return subscribeableView.SubscribeTo(ctx, defraNode)
}
