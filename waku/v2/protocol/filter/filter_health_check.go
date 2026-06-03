package filter

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/waku-org/go-waku/waku/v2/utils"
	"go.uber.org/zap"
)

const PingTimeout = 5 * time.Second

func (wf *WakuFilterLightNode) PingPeers() {
	//Send a ping to all the peers and report their status to corresponding subscriptions
	// Alive or not or set state of subcription??
	for _, peer := range wf.subscriptions.GetSubscribedPeers() {
		go wf.PingPeer(peer)
	}
}

func (wf *WakuFilterLightNode) PingPeer(peer peer.ID) {
	defer utils.LogOnPanic()
	ctxWithTimeout, cancel := context.WithTimeout(wf.CommonService.Context(), PingTimeout)
	defer cancel()
	err := wf.Ping(ctxWithTimeout, peer)
	if err != nil && wf.onlineChecker.IsOnline() {
		wf.log.Info("Filter ping failed towards peer", zap.Stringer("peer", peer), zap.Error(err))
		//quickly retry ping again before marking subscription as failure
		//Note that PingTimeout is a fraction of PingInterval so this shouldn't cause parallel pings being sent.
		ctxWithTimeout, cancel := context.WithTimeout(wf.CommonService.Context(), PingTimeout)
		defer cancel()
		err = wf.Ping(ctxWithTimeout, peer)
		if err != nil {
			subscriptions := wf.subscriptions.GetAllSubscriptionsForPeer(peer)
			for _, subscription := range subscriptions {
				wf.log.Debug("Notifying sub closing", zap.String("subID", subscription.ID))
				//Indicating that subscription is closing,
				subscription.SetClosing()
			}
		}
	}
}

// SetBackgroundMode suppresses (background=true) or re-enables (background=false)
// the periodic health-check pings sent to filter peers. Call with background=true
// when the app UI is not visible to avoid waking the LTE radio during Doze windows.
func (wf *WakuFilterLightNode) SetBackgroundMode(background bool) {
	wf.backgroundMode.Store(background)
}

func (wf *WakuFilterLightNode) FilterHealthCheckLoop() {
	defer utils.LogOnPanic()
	defer wf.WaitGroup().Done()
	ticker := time.NewTicker(wf.peerPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if wf.backgroundMode.Load() {
				// In background: skip health-check ping to avoid waking the LTE radio.
				// SetBackgroundMode(false) will resume pings on foreground return.
				continue
			}
			if wf.onlineChecker.IsOnline() {
				wf.PingPeers()
			}
		case <-wf.CommonService.Context().Done():
			return
		}
	}
}
