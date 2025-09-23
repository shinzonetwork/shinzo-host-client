package shinzohub

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const expectedShinzoHubNodeUrl string = "http://localhost:8545"
const expectedShinzoHubNodeWebsocket string = "ws://localhost:26657/websocket"

// This test assumes that you are running a local instance of ShinzoHub
// `just sh-testnet` from the root dir of ShinzoHub - this will startup an instance of ShinzoHub
// The ShinzoHub node running locally should expose ws://localhost:26657/websocket for websockets and http://localhost:8545 for API requests
func TestEventSubscriptions(t *testing.T) {
	t.Skip("Skipping event subscription test - requires external WebSocket connection")

	cancel, eventChan, err := StartEventSubscription(expectedShinzoHubNodeWebsocket)
	require.NoError(t, err)
	defer cancel()

	// Wait a bit for the subscription to be ready
	time.Sleep(2 * time.Second)

	// Send a transaction to trigger an event
	sendSampleCurlRequest(t)

	// Process events from the channel
	timeout := time.After(10 * time.Second)
	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				t.Log("Event channel closed")
				return
			}
			t.Logf("Received event: %s", event.ToString())

			// You can process the event here
			if registeredEvent, ok := event.(*ViewRegisteredEvent); ok {
				require.Equal(t, "0xc26d2ef9f0e108c9e483849dbfb2cc685d77ffc03dfc22079778579610497858", registeredEvent.Key)
				require.Equal(t, "shinzo140fehngcrxvhdt84x729p3f0qmkmea8nq3rk92", registeredEvent.Creator)
				require.Equal(t, "hello", registeredEvent.View)
				return
			}

		case <-timeout:
			t.Fatalf("Timeout waiting for events")
			return
		}
	}
}

func sendSampleCurlRequest(t *testing.T) {
	url := expectedShinzoHubNodeUrl

	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_sendTransaction",
		"params": []map[string]interface{}{
			{
				"from":  "0xabd39bcd18199976acf5379450c52f06edbcf4f3",
				"to":    "0x0000000000000000000000000000000000000210",
				"gas":   "0x100000",
				"value": "0x0",
				"data":  "0x82fbdc9c0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000568656c6c6f000000000000000000000000000000000000000000000000000000",
			},
		},
		"id": 1,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	t.Logf("Response: %s", string(body))
}
