// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	// Taking out latches/locks across the entire SST span is very coarse, and we
	// could instead iterate over the SST and take out point latches/locks, but
	// the cost is likely not worth it since GCAndExciseRange is often used with
	// unpopulated spans.
	RegisterReadWriteCommand(kvpb.GCAndExciseRange, DefaultDeclareKeys, EvalGCAndExciseRange)
}

// EvalGCAndExciseRange evaluates a GCAndExciseRange command. For details, see doc comment
// on GCAndExciseRangeRequest.
func EvalGCAndExciseRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {

	args := cArgs.Args.(*kvpb.GCAndExciseRangeRequest)
	start, end := storage.MVCCKey{Key: args.Key}, storage.MVCCKey{Key: args.EndKey}
	log.Infof(ctx, "EvalGCAndExciseRange called with args: %+v", args)

	fmt.Printf("!!! IBRAHIM !!! start.Key: %+v, end.Key: %+v\n", start.Key, end.Key)

	ltStart, _ := keys.LockTableSingleKey(args.Span().Key, nil)
	ltEnd, _ := keys.LockTableSingleKey(args.Span().EndKey, nil)

	startRKey, err := keys.Addr(start.Key)
	if err != nil {
		return result.Result{}, err
	}

	endRKey, err := keys.Addr(end.Key)
	if err != nil {
		return result.Result{}, err
	}

	startRangeKeyPref := keys.MakeRangeKeyPrefix(startRKey)
	endRangeKeyPref := keys.MakeRangeKeyPrefix(endRKey)

	startTxnKey := keys.MakeRangeKey(startRKey, keys.LocalTransactionSuffix, nil)
	endTxnKey := keys.MakeRangeKey(endRKey, keys.LocalTransactionSuffix, nil)
	fmt.Printf("!!! IBRAHIM !!! startTxnKey: %+v, endTxnKey: %+v\n", startTxnKey, endTxnKey)

	rdStartKey := keys.RangeDescriptorKey(startRKey)
	rdEndKey := keys.RangeDescriptorKey(endRKey)

	fmt.Printf("!!! IBRAHIM !!! ltStart: %+v, ltEnd: %+v\n", ltStart, ltEnd)
	fmt.Printf("!!! IBRAHIM !!! startRangeKeyPref: %+v, endRangeKeyPref: %+v\n", startRangeKeyPref, endRangeKeyPref)
	fmt.Printf("!!! IBRAHIM !!! rdStartKey: %+v, rdEndKey: %+v\n", rdStartKey, rdEndKey)

	return result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			GCAndExciseRange: &kvserverpb.ReplicatedEvalResult_GCAndExciseRange{
				Span:          roachpb.Span{Key: start.Key, EndKey: end.Key},
				LockTableSpan: roachpb.Span{Key: ltStart, EndKey: ltEnd},
				RangeSpan:     roachpb.Span{Key: startRangeKeyPref, EndKey: endRangeKeyPref},
			},
		},
	}, nil
}
