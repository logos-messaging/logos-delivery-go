package node

import (
	"net"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/waku-org/go-waku/waku/v2/peermanager"
	"github.com/waku-org/go-waku/waku/v2/protocol/filter"
	"github.com/waku-org/go-waku/waku/v2/protocol/legacy_store"
	"github.com/waku-org/go-waku/waku/v2/protocol/pb"
	r "github.com/waku-org/go-zerokit-rln/rln"
	"go.uber.org/zap"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/waku-org/go-waku/tests"
	"github.com/waku-org/go-waku/waku/persistence"
)

func handleSpam(msg *pb.WakuMessage, topic string) error {

	logger := new(zap.Logger)

	logger.Log(zap.InfoLevel, "Spam has been detected!")

	return nil
}

func TestWakuOptions(t *testing.T) {
	topicHealthStatusChan := make(chan peermanager.TopicHealthStatus, 100)

	key, err := tests.RandomHex(32)
	require.NoError(t, err)

	prvKey, err := crypto.HexToECDSA(key)
	require.NoError(t, err)

	hostAddr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:0")

	addr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/4000/ws")
	require.NoError(t, err)

	storeFactory := func(w *WakuNode) legacy_store.Store {
		return legacy_store.NewWakuStore(w.opts.messageProvider, w.peermanager, w.timesource, prometheus.DefaultRegisterer, w.log)
	}

	options := []WakuNodeOption{
		WithHostAddress(hostAddr),
		WithAdvertiseAddresses(addr),
		WithMultiaddress(addr),
		WithPrivateKey(prvKey),
		WithLibP2POptions(),
		WithWakuRelay(),
		WithDiscoveryV5(123, nil, false),
		WithWakuStore(),
		WithMessageProvider(&persistence.DBStore{}),
		WithLightPush(),
		WithKeepAlive(time.Minute, time.Hour),
		WithTopicHealthStatusChannel(topicHealthStatusChan),
		WithWakuStoreFactory(storeFactory),
	}

	params := new(WakuNodeParameters)

	for _, opt := range options {
		require.NoError(t, opt(params))
	}

	require.NotNil(t, params.multiAddr)
	require.NotNil(t, params.privKey)
	require.NotNil(t, params.topicHealthNotifCh)
}

func TestWakuFilterLightNodeOptions(t *testing.T) {
	t.Run("no options", func(t *testing.T) {
		wakuNode, err := New(WithPrometheusRegisterer(prometheus.NewRegistry()), WithWakuFilterLightNode())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, wakuNode.peerstore.Close()) })
		require.True(t, wakuNode.opts.enableFilterLightNode)
		require.NotNil(t, wakuNode.FilterLightnode())
	})

	for _, tt := range []struct {
		name    string
		options []filter.LightNodeOption
	}{
		{
			name: "defaults",
		},
		{
			name:    "custom rate limiter",
			options: []filter.LightNodeOption{filter.WithLightNodeRateLimiter(25, 80)},
		},
		{
			name: "last option wins",
			options: []filter.LightNodeOption{
				filter.WithLightNodeRateLimiter(1, 2),
				filter.WithLightNodeRateLimiter(25, 80),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			expected := new(filter.LightNodeParameters)
			for _, opt := range filter.DefaultLightNodeOptions() {
				opt(expected)
			}
			if len(tt.options) > 0 {
				filter.WithLightNodeRateLimiter(25, 80)(expected)
			}

			var actual filter.LightNodeParameters
			calls := 0
			options := append(tt.options, func(params *filter.LightNodeParameters) {
				actual = *params
				calls++
			})
			wakuNode, err := New(
				WithPrometheusRegisterer(prometheus.NewRegistry()),
				WithWakuFilterLightNode(options...),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wakuNode.peerstore.Close()) })
			require.True(t, wakuNode.opts.enableFilterLightNode)
			require.NotNil(t, wakuNode.FilterLightnode())
			require.Equal(t, 1, calls, "options must reach the Filter light-node constructor")
			require.Equal(t, *expected, actual)
		})
	}
}

func TestWakuRLNOptions(t *testing.T) {
	topicHealthStatusChan := make(chan peermanager.TopicHealthStatus, 100)

	key, err := tests.RandomHex(32)
	require.NoError(t, err)

	prvKey, err := crypto.HexToECDSA(key)
	require.NoError(t, err)

	hostAddr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:0")

	addr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/4000/ws")
	require.NoError(t, err)

	storeFactory := func(w *WakuNode) legacy_store.Store {
		return legacy_store.NewWakuStore(w.opts.messageProvider, w.peermanager, w.timesource, prometheus.DefaultRegisterer, w.log)
	}

	index := r.MembershipIndex(5)

	// Test WithStaticRLNRelay

	options := []WakuNodeOption{
		WithHostAddress(hostAddr),
		WithAdvertiseAddresses(addr),
		WithMultiaddress(addr),
		WithPrivateKey(prvKey),
		WithLibP2POptions(),
		WithWakuRelay(),
		WithDiscoveryV5(123, nil, false),
		WithWakuStore(),
		WithMessageProvider(&persistence.DBStore{}),
		WithLightPush(),
		WithKeepAlive(time.Minute, time.Hour),
		WithTopicHealthStatusChannel(topicHealthStatusChan),
		WithWakuStoreFactory(storeFactory),
		WithStaticRLNRelay(&index, handleSpam),
	}

	params := new(WakuNodeParameters)

	for _, opt := range options {
		require.NoError(t, opt(params))
	}

	require.True(t, params.enableRLN)
	require.False(t, params.rlnRelayDynamic)
	require.Equal(t, uint(5), *params.rlnRelayMemIndex)
	require.NotNil(t, params.rlnSpamHandler)

	// Test WithDynamicRLNRelay

	var (
		keystorePath     = "./rlnKeystore.json"
		keystorePassword = "password"
		rlnTreePath      = "root"
		contractAddress  = "0xF471d71E9b1455bBF4b85d475afb9BB0954A29c4"
		ethClientAddress = "wss://sepolia.infura.io/ws/v3/API_KEY_GOES_HERE"
	)

	index = uint(0)

	options2 := []WakuNodeOption{
		WithHostAddress(hostAddr),
		WithAdvertiseAddresses(addr),
		WithMultiaddress(addr),
		WithPrivateKey(prvKey),
		WithLibP2POptions(),
		WithWakuRelay(),
		WithDiscoveryV5(123, nil, false),
		WithWakuStore(),
		WithMessageProvider(&persistence.DBStore{}),
		WithLightPush(),
		WithKeepAlive(time.Minute, time.Hour),
		WithTopicHealthStatusChannel(topicHealthStatusChan),
		WithWakuStoreFactory(storeFactory),
		WithDynamicRLNRelay(keystorePath, keystorePassword, rlnTreePath, common.HexToAddress(contractAddress), &index, handleSpam, ethClientAddress),
	}

	params2 := new(WakuNodeParameters)

	for _, opt := range options2 {
		require.NoError(t, opt(params2))
	}

	require.True(t, params2.enableRLN)
	require.True(t, params2.rlnRelayDynamic)
	require.Equal(t, keystorePassword, params2.keystorePassword)
	require.Equal(t, uint(0), *params2.rlnRelayMemIndex)
	require.NotNil(t, params2.rlnSpamHandler)
	require.Equal(t, ethClientAddress, params2.rlnETHClientAddress)
	require.Equal(t, common.HexToAddress(contractAddress), params2.rlnMembershipContractAddress)
	require.Equal(t, rlnTreePath, params2.rlnTreePath)

}
