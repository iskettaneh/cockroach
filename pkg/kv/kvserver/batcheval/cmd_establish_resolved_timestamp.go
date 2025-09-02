// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadOnlyCommand(kvpb.EstablishResolvedTimestamp, DefaultDeclareKeys, EstablishResolvedTimestamp)
}

// func declareKeysEstablishResolvedTimestamp(
// 	rs ImmutableRangeState,
// 	header *kvpb.Header,
// 	req kvpb.Request,
// 	latchSpans *spanset.SpanSet,
// 	lockSpans *lockspanset.LockSpanSet,
// 	maxOffset time.Duration,
// ) error {
// 	// We need to grab latches over the specified span to ensure proper
// 	// concurrency control, as mentioned in the RFC.
// 	latchSpans.AddNonMVCC(spanset.SpanReadOnly, req.Header().Span())

// 	// Declare lock spans to ensure we can check the lock table.
// 	lockSpans.Add(lock.Intent, req.Header().Span())

// 	return nil
// }

// EstablishResolvedTimestamp is used by followers to request that the leaseholder
// establish a resolved timestamp for consistent follower reads. This implements
// the follower-coordinated approach described in RFC issue #72593.
//
// The leaseholder grabs latches, checks the lock table, bumps the timestamp cache
// over the specified span, and returns the current lease_applied_index. The follower
// can then wait for its Raft log to catch up to this index before serving the read.
func EstablishResolvedTimestamp(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	reply := resp.(*kvpb.EstablishResolvedTimestampResponse)

	// Verify that this request is being processed on the leaseholder.
	// Only the leaseholder can establish resolved timestamps for followers.
	lease, _ := cArgs.EvalCtx.GetLease()
	if lease.Replica.StoreID != cArgs.EvalCtx.StoreID() {
		return result.Result{}, &kvpb.NotLeaseHolderError{
			RangeID:   cArgs.EvalCtx.GetRangeID(),
			RangeDesc: *cArgs.EvalCtx.Desc(),
			Replica:   lease.Replica,
			Lease:     &lease,
		}
	}

	// Get the current lease applied index. This tells the follower how far
	// it needs to catch up on its Raft log before it can serve reads at
	// the established timestamp.
	leaseAppliedIndex := cArgs.EvalCtx.GetLeaseAppliedIndex()

	// Set the response fields.
	reply.LeaseAppliedIndex = leaseAppliedIndex

	// Provide an observed timestamp to help avoid uncertainty restarts.
	// This is the timestamp at which the leaseholder processed this request.
	reply.ObservedTimestamp = cArgs.EvalCtx.Clock().Now()

	// The timestamp cache will be automatically updated by the KV infrastructure
	// after this command evaluation completes, which will bump the timestamp cache
	// over the requested span to args.Timestamp. This ensures that any future writes
	// will have timestamps higher than the established resolved timestamp.

	return result.Result{}, nil
}
