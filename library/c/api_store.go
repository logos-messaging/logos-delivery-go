package main

/*
#include <cgo_utils.h>
*/
import "C"
import (
	"unsafe"

	"github.com/waku-org/go-waku/library"
)

// Query historic messages using waku store protocol.
// queryJSON must contain a valid json string with the following format:
//
//	{
//		"pubsubTopic": "...", // optional string
//		"startTime": 1234, // optional, unix epoch time in nanoseconds
//		"endTime": 1234, // optional, unix epoch time in nanoseconds
//		"contentTopics": [ // optional
//			"contentTopic1",
//			...
//		],
//		"pagingOptions": {// optional pagination information
//			"pageSize": 40, // number
//			"cursor": { // optional
//				"digest": ...,
//				"receiverTime": ...,
//				"senderTime": ...,
//				"pubsubTopic": ...,
//			},
//			"forward": true, // sort order
//		}
//	}
//
// If a non empty cursor is returned, this function should be executed again, setting  the `cursor` attribute with the cursor returned in the response
// peerID should contain the ID of a peer supporting the store protocol. Use NULL to automatically select a node
// If ms is greater than 0, the broadcast of the message must happen before the timeout
// (in milliseconds) is reached, or an error will be returned
//
//export waku_store_query
func waku_store_query(ctx unsafe.Pointer, queryJSON *C.char, peerID *C.char, ms C.int, cb C.WakuCallBack, userData unsafe.Pointer) C.int {
	return singleFnExec(func(instance *library.WakuInstance) (string, error) {
		return library.StoreQuery(instance, C.GoString(queryJSON), C.GoString(peerID), int(ms))
	}, ctx, cb, userData)
}

// Query historic messages using the StoreV3 protocol (/vac/waku/store-query/3.0.0),
// as opposed to waku_store_query which speaks the legacy
// /vac/waku/store/2.0.0-beta4 protocol.
// queryJSON must contain a valid json string with the following format:
//
//	{
//		"pubsubTopic": "...", // required string
//		"startTime": 1234, // optional, unix epoch time in nanoseconds
//		"endTime": 1234, // optional, unix epoch time in nanoseconds
//		"contentTopics": [ // optional
//			"contentTopic1",
//			...
//		],
//		"pagingOptions": {// optional pagination information
//			"pageSize": 40, // number
//			"cursor": "...", // optional, opaque base64 byte string returned by a previous call
//			"forward": true, // sort order
//		}
//	}
//
// The response `pagingInfo` reports the pagination that was actually used, so it
// can be passed back verbatim to fetch the next page. Its `cursor` is an opaque
// base64 byte string (NOT the structured legacy cursor). If a non empty cursor is
// returned, this function should be executed again with that `pagingInfo`.
// peerID should contain the ID of a peer supporting the StoreV3 protocol. Use NULL to automatically select a node
// If ms is greater than 0, the query must complete before the timeout
// (in milliseconds) is reached, or an error will be returned
//
//export waku_store_query_v3
func waku_store_query_v3(ctx unsafe.Pointer, queryJSON *C.char, peerID *C.char, ms C.int, cb C.WakuCallBack, userData unsafe.Pointer) C.int {
	return singleFnExec(func(instance *library.WakuInstance) (string, error) {
		return library.StoreQueryV3(instance, C.GoString(queryJSON), C.GoString(peerID), int(ms))
	}, ctx, cb, userData)
}

// Query historic messages stored in the localDB using waku store protocol.
// queryJSON must contain a valid json string with the following format:
//
//	{
//		"pubsubTopic": "...", // optional string
//		"startTime": 1234, // optional, unix epoch time in nanoseconds
//		"endTime": 1234, // optional, unix epoch time in nanoseconds
//		"contentTopics": [ // optional
//			"contentTopic1"
//			...
//		],
//		"pagingOptions": {// optional pagination information
//			"pageSize": 40, // number
//			"cursor": { // optional
//				"digest": ...,
//				"receiverTime": ...,
//				"senderTime": ...,
//				"pubsubTopic": ...,
//			},
//			"forward": true, // sort order
//		}
//	}
//
// If a non empty cursor is returned, this function should be executed again, setting  the `cursor` attribute with the cursor returned in the response
// Requires the `store` option to be passed when setting up the initial configuration
//
//export waku_store_local_query
func waku_store_local_query(ctx unsafe.Pointer, queryJSON *C.char, cb C.WakuCallBack, userData unsafe.Pointer) C.int {
	return singleFnExec(func(instance *library.WakuInstance) (string, error) {
		return library.StoreLocalQuery(instance, C.GoString(queryJSON))
	}, ctx, cb, userData)
}
