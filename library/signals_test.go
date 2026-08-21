package library

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSignalCarriesInstanceID checks that a signal identifies the instance that
// emitted it, so a consumer running several instances can route it.
func TestSignalCarriesInstanceID(t *testing.T) {
	first := Init()
	defer func() { _ = Free(first) }()
	second := Init()
	defer func() { _ = Free(second) }()

	var received [][]byte
	handler := func(data []byte) { received = append(received, data) }
	SetMobileSignalHandler(first, handler)
	SetMobileSignalHandler(second, handler)

	send(second, "message", map[string]string{"payload": "hello"})
	send(first, "message", map[string]string{"payload": "world"})

	require.Len(t, received, 2)
	require.Equal(t, second.ID, decodeInstanceID(t, received[0]))
	require.Equal(t, first.ID, decodeInstanceID(t, received[1]))
}

func decodeInstanceID(t *testing.T, data []byte) uint {
	t.Helper()

	var envelope struct {
		InstanceID uint `json:"instanceId"`
	}
	require.NoError(t, json.Unmarshal(data, &envelope))
	return envelope.InstanceID
}
