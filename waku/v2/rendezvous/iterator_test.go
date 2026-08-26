package rendezvous

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// TestIteratorSkipsPointsInBackoff verifies that a rendezvous point whose backoff
// has not elapsed yet is never handed out by the iterator while another point is
// ready to be dialed.
func TestIteratorSkipsPointsInBackoff(t *testing.T) {
	ready := NewRendezvousPoint(peer.ID("ready"))
	backedOff := NewRendezvousPoint(peer.ID("backed-off"))

	// ready has no pending backoff, so it is dialable right away.
	ready.nextTry = time.Now().Add(-time.Minute)

	// backedOff failed to connect, so it must not be dialed until its backoff expires.
	backedOff.Delay()
	require.True(t, backedOff.NextTry().After(time.Now()), "backed off point should not be dialable yet")

	iterator := &RendezvousPointIterator{
		rendezvousPoints: []*RendezvousPoint{ready, backedOff},
	}

	// The selection is random, so repeat it enough times to catch a point that
	// should have been filtered out.
	for i := 0; i < 100; i++ {
		rp := <-iterator.Next(context.Background())
		require.NotNil(t, rp)
		require.Equal(t, ready.id, rp.id, "iterator returned a rendezvous point that is still in backoff")
	}
}
