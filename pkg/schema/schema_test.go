package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

func init() {
	logger.Init(true, "")
}

func TestGetSchema(t *testing.T) {
	s := GetSchema()
	require.NotEmpty(t, s)
}
