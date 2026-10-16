package schema

import (
	"strings"
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

// The type is declared twice: the file appended to a fetched schema, and the copy in the
// fallback. A host applies one or the other, so they have to stay in step.
func TestAttestationRecordDeclarationsMatch(t *testing.T) {
	appended := attestationRecordType(t, AttestationRecordTypeDef)
	fallback := attestationRecordType(t, GetSchema())

	require.Equal(t, appended, fallback)
	require.Contains(t, appended, "blockNumber: Int @index",
		"the pruner orders on blockNumber, so it has to be indexed")
}

// attestationRecordType returns the AttestationRecord type block from an SDL string.
func attestationRecordType(t *testing.T, sdl string) string {
	t.Helper()
	const marker = "type Ethereum__Mainnet__AttestationRecord {"
	start := strings.Index(sdl, marker)
	require.NotEqual(t, -1, start, "AttestationRecord type not found")
	end := strings.Index(sdl[start:], "}")
	require.NotEqual(t, -1, end, "unterminated AttestationRecord type")
	return sdl[start : start+end+1]
}
