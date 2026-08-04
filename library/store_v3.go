package library

import (
	"context"
	"encoding/json"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/waku-org/go-waku/waku/v2/protocol"
	wpb "github.com/waku-org/go-waku/waku/v2/protocol/pb"
	"github.com/waku-org/go-waku/waku/v2/protocol/store"
)

// storeV3PagingOptions mirrors storePagingOptions but uses the StoreV3
// (`/vac/waku/store-query/3.0.0`) pagination cursor, which is an opaque byte
// string rather than the legacy structured Index. When marshalled to JSON the
// cursor is base64-encoded; pass it back verbatim on the next call to page.
type storeV3PagingOptions struct {
	PageSize uint64 `json:"pageSize,omitempty"`
	Cursor   []byte `json:"cursor,omitempty"`
	Forward  bool   `json:"forward,omitempty"`
}

type storeV3MessagesArgs struct {
	Topic         string                `json:"pubsubTopic,omitempty"`
	ContentTopics []string              `json:"contentTopics,omitempty"`
	StartTime     *int64                `json:"startTime,omitempty"`
	EndTime       *int64                `json:"endTime,omitempty"`
	PagingOptions *storeV3PagingOptions `json:"pagingOptions,omitempty"`
}

// storeV3MessagesReply intentionally keeps the same `messages` / `pagingInfo` /
// `error` shape as the legacy storeMessagesReply so existing callers that only
// read WakuMessage fields can parse it unchanged. The only behavioural
// difference is that pagingInfo.cursor is an opaque byte string.
type storeV3MessagesReply struct {
	Messages   []*wpb.WakuMessage   `json:"messages,omitempty"`
	PagingInfo storeV3PagingOptions `json:"pagingInfo,omitempty"`
	Error      string               `json:"error,omitempty"`
}

// StoreQueryV3 retrieves historic messages using the StoreV3 protocol
// (`/vac/waku/store-query/3.0.0`). Unlike StoreQuery, which speaks the legacy
// `/vac/waku/store/2.0.0-beta4` protocol, this talks to storenodes that only
// expose the v3 query protocol (e.g. current nwaku store nodes).
//
// peerID should contain the ID of a peer supporting the StoreV3 protocol. The
// peer must already be known to the node (e.g. an existing connection). Pass an
// empty string to let the node select a v3-capable peer automatically.
func StoreQueryV3(instance *WakuInstance, queryJSON string, peerID string, ms int) (string, error) {
	if err := validateInstance(instance, MustBeStarted); err != nil {
		return "", err
	}

	var args storeV3MessagesArgs
	if err := json.Unmarshal([]byte(queryJSON), &args); err != nil {
		return "", err
	}

	criteria := store.FilterCriteria{
		ContentFilter: protocol.NewContentFilter(args.Topic, args.ContentTopics...),
		TimeStart:     args.StartTime,
		TimeEnd:       args.EndTime,
	}

	options := []store.RequestOption{
		store.WithAutomaticRequestID(),
		store.IncludeData(true),
	}

	if args.PagingOptions != nil {
		options = append(options, store.WithPaging(args.PagingOptions.Forward, args.PagingOptions.PageSize))
		if len(args.PagingOptions.Cursor) != 0 {
			options = append(options, store.WithCursor(args.PagingOptions.Cursor))
		}
	}

	if peerID != "" {
		p, err := peer.Decode(peerID)
		if err != nil {
			return "", err
		}
		options = append(options, store.WithPeer(p))
	} else {
		options = append(options, store.WithAutomaticPeerSelection())
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if ms > 0 {
		ctx, cancel = context.WithTimeout(instance.ctx, time.Duration(ms)*time.Millisecond)
		defer cancel()
	} else {
		ctx = instance.ctx
	}

	result, err := instance.node.Store().Query(ctx, criteria, options...)

	reply := storeV3MessagesReply{}
	if err != nil {
		reply.Error = err.Error()
		return marshalJSON(reply)
	}

	for _, kv := range result.Messages() {
		if kv.Message != nil {
			reply.Messages = append(reply.Messages, kv.Message)
		}
	}

	reply.PagingInfo = storeV3PagingOptions{
		Cursor: result.Cursor(),
	}
	if args.PagingOptions != nil {
		reply.PagingInfo.PageSize = args.PagingOptions.PageSize
		reply.PagingInfo.Forward = args.PagingOptions.Forward
	}

	return marshalJSON(reply)
}
