package accounting

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-querysig/billing"
)

// The posted record carries the user's request fields verbatim and the host's
// computed fields, and its two signatures recover to the payer and the host.
func TestRecorder_Record(t *testing.T) {
	const chainID = 91273002

	hostKey, err := ethcrypto.GenerateKey()
	require.NoError(t, err)
	hostAddr := ethcrypto.PubkeyToAddress(hostKey.PublicKey)

	userKey, err := ethcrypto.GenerateKey()
	require.NoError(t, err)
	userAddr := ethcrypto.PubkeyToAddress(userKey.PublicKey)

	signedPool := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	ext, err := billing.SignRequest(chainID, userKey, "query { FilteredLogs { hash } }", nil, signedPool, 3, 1735689600)
	require.NoError(t, err)

	var posted ServiceRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&posted)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	rec := NewRecorder(NewClient(srv.URL, time.Second), hostKey, chainID)
	rec.now = func() time.Time { return time.Unix(1735690000, 0) }

	require.NoError(t, rec.Record(context.Background(), RecordInput{
		Extensions:       ext,
		RowsQueried:      7,
		AttestedIndexers: []string{"idx1", "idx2"},
	}))

	require.Equal(t, ext.Nonce, posted.Nonce)
	require.Equal(t, ext.QueryHash, posted.QueryHash)
	require.Equal(t, ext.RequestSignature, posted.RequestSignature)
	require.Equal(t, ext.RequestTimestamp, posted.RequestTimestamp)
	require.Equal(t, hostAddr.Hex(), posted.HostAddress)
	require.Equal(t, signedPool.Hex(), posted.PoolID)
	require.Equal(t, uint64(7), posted.RowsQueried)
	require.Equal(t, uint64(1735690000), posted.RespondedAt)
	require.Equal(t, []string{"idx1", "idx2"}, posted.AttestedIndexers)

	rawHash, err := hexutil.Decode(posted.QueryHash)
	require.NoError(t, err)
	var queryHash [32]byte
	copy(queryHash[:], rawHash)
	sig, err := hexutil.Decode(posted.ResponseSignature)
	require.NoError(t, err)
	recovered, err := billing.RecoverQueryResponse(chainID, billing.QueryResponse{
		QueryHash:        queryHash,
		Host:             hostAddr,
		Pool:             signedPool,
		RowsQueried:      7,
		RespondedAt:      1735690000,
		ResponseCidsHash: billing.ResponseCidsHash([]string{}),
	}, sig)
	require.NoError(t, err)
	require.Equal(t, hostAddr, recovered, "response signature must recover to the host")

	rawNonce, err := hexutil.Decode(posted.Nonce)
	require.NoError(t, err)
	var nonce [32]byte
	copy(nonce[:], rawNonce)
	reqSig, err := hexutil.Decode(posted.RequestSignature)
	require.NoError(t, err)
	payer, err := billing.RecoverQueryRequest(chainID, billing.QueryRequest{
		QueryHash: queryHash,
		Nonce:     nonce,
		Timestamp: posted.RequestTimestamp,
		Pool:      signedPool,
	}, reqSig)
	require.NoError(t, err)
	require.Equal(t, userAddr, payer, "request signature must recover to the payer")
}

// A malformed Extensions must fail before anything is POSTed.
func TestRecorder_Record_MalformedExtensions(t *testing.T) {
	hostKey, err := ethcrypto.GenerateKey()
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("a malformed request must not be POSTed")
	}))
	defer srv.Close()

	rec := NewRecorder(NewClient(srv.URL, time.Second), hostKey, 91273002)
	err = rec.Record(context.Background(), RecordInput{
		Extensions:  billing.Extensions{QueryHash: "0xnothex"},
		RowsQueried: 1,
	})
	require.Error(t, err)
}
