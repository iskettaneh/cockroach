// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/redact"
)

// FollowerReadsEnabled controls whether replicas attempt to serve follower
// reads. The closed timestamp machinery is unaffected by this, i.e. the same
// information is collected and passed around, regardless of the value of this
// setting.
var FollowerReadsEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible, // needed for planning in SQL
	"kv.closed_timestamp.follower_reads_enabled",
	"allow (all) replicas to serve consistent historical reads based on closed timestamp information",
	true,
	settings.WithName("kv.closed_timestamp.follower_reads.enabled"),
	settings.WithPublic)

// ConsistentFollowerReadsEnabled enables the consistent follower reads feature
// described in RFC issue #72593. When enabled, followers will coordinate with
// the leaseholder to establish resolved timestamps for consistent reads.
var ConsistentFollowerReadsEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"kv.closed_timestamp.consistent_follower_reads_enabled",
	"enable consistent follower reads by coordinating with leaseholder to establish resolved timestamps",
	false,
	settings.WithName("kv.closed_timestamp.consistent_follower_reads.enabled"),
	settings.WithPublic)

// BatchCanBeEvaluatedOnFollower determines if a batch consists exclusively of
// requests that can be evaluated on a follower replica, given a sufficiently
// advanced closed timestamp.
func BatchCanBeEvaluatedOnFollower(ctx context.Context, ba *kvpb.BatchRequest) bool {
	// Various restrictions apply to a batch for it to be successfully considered
	// for evaluation on a follower replica, which are described inline.
	//
	// The batch cannot have or intend to receive a timestamp set from a
	// server-side clock. If follower with a lagging clock sets its timestamp
	// and this then allows the follower to evaluate the batch as a follower read,
	// then the batch might miss past writes served at higher timestamps on the
	// leaseholder.
	tsFromServerClock := ba.Txn == nil && (ba.Timestamp.IsEmpty() || ba.TimestampFromServerClock != nil)
	if tsFromServerClock {
		return false
	}
	if len(ba.Requests) == 0 {
		// No requests to evaluate.
		return false
	}
	// Each request in the batch needs to have clearly defined semantics when
	// served under the closed timestamp.
	for _, ru := range ba.Requests {
		r := ru.GetInner()
		switch {
		case kvpb.IsTransactional(r):
			// Transactional requests have clear semantics when served under the
			// closed timestamp. The request must be read-only, as follower replicas
			// cannot propose writes to Raft. The request also needs to be
			// non-locking, because unreplicated locks are only held on the
			// leaseholder.
			if !kvpb.IsReadOnly(r) || kvpb.IsLocking(r) {
				return false
			}
		case r.Method() == kvpb.Export:
			// Export requests also have clear semantics when served under the closed
			// timestamp as well, even though they are non-transactional, as they
			// define the start and end timestamp to export data over.
			if r.(*kvpb.ExportRequest).ExportFingerprint {
				// Fingerprint reuses a lot of the backup code by sending export requests,
				// but unlike backup requests, doesn't have a job and multiple backup processors
				// to spread the workload around the cluster. In a 3-node cluster, the request
				// routing logic will determine that all replicas exist on the gateway node and
				// with following reads allowed, will attempt to route all the export requests to
				// the gateway node. This leads to one node doing all the work while the others sit
				// idle. Return false to prevent follower reads for fingerprinting
				return false
			}
		default:
			return false
		}
	}
	return true
}

// canServeFollowerRead tests, when a range lease could not be acquired,
// whether the batch can be served as a follower read despite the error. Only
// non-locking, read-only requests can be served as follower reads. The batch
// must be transactional and composed exclusively of this kind of request to be
// accepted as a follower read.
func (r *Replica) canServeFollowerRead(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	desc *roachpb.RangeDescriptor,
	appliedLAI kvpb.LeaseAppliedIndex,
	leaseholderNodeId roachpb.NodeID,
	raftClosed hlc.Timestamp,
) bool {
	eligible := BatchCanBeEvaluatedOnFollower(ctx, ba) && FollowerReadsEnabled.Get(&r.store.cfg.Settings.SV)
	if !eligible {
		// We couldn't do anything with the error, propagate it.
		return false
	}

	repDesc, err := getReplicaDescriptor(desc, r.RangeID, r.StoreID())
	if err != nil {
		return false
	}

	switch repDesc.Type {
	case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING, roachpb.NON_VOTER:
	default:
		log.Eventf(ctx, "%s replicas cannot serve follower reads", repDesc.Type)
		return false
	}

	requiredFrontier := ba.RequiredFrontier()
	maxClosed := r.getCurrentClosedTimestamp(ctx, requiredFrontier /* sufficient */, appliedLAI,
		leaseholderNodeId, raftClosed)
	canServeFollowerRead := requiredFrontier.LessEq(maxClosed)
	tsDiff := requiredFrontier.GoTime().Sub(maxClosed.GoTime())
	if !canServeFollowerRead {
		uncertaintyLimitStr := "n/a"
		if ba.Txn != nil {
			uncertaintyLimitStr = ba.Txn.GlobalUncertaintyLimit.String()
		}

		// Check if consistent follower reads are enabled and try to coordinate
		// with the leaseholder to establish a resolved timestamp.
		if ConsistentFollowerReadsEnabled.Get(&r.store.cfg.Settings.SV) {
			if r.tryConsistentFollowerRead(ctx, ba, desc, leaseholderNodeId) {
				// Successfully established resolved timestamp and served the read
				return true
			}
		}

		// We can't actually serve the read based on the closed timestamp.
		// Signal the clients that we want an update so that future requests can succeed.
		log.Eventf(ctx, "can't serve follower read; closed timestamp too low by: %s; maxClosed: %s ts: %s uncertaintyLimit: %s",
			tsDiff, maxClosed, ba.Timestamp, uncertaintyLimitStr)
		return false
	}

	// This replica can serve this read!
	//
	// TODO(tschottdorf): once a read for a timestamp T has been served, the replica may
	// serve reads for that and smaller timestamps forever.
	log.Eventf(ctx, "%s; query timestamp below closed timestamp by %s", redact.Safe(kvbase.FollowerReadServingMsg), -tsDiff)
	r.store.metrics.FollowerReadsCount.Inc(1)
	if sp := tracing.SpanFromContext(ctx); sp.RecordingType() != tracingpb.RecordingOff {
		sp.RecordStructured(&kvpb.UsedFollowerRead{})
	}
	return true
}

// getCurrentClosedTimestampRLocked is like GetCurrentClosedTimestamp, except
// that it requires r.mu to be RLocked. It also optionally takes a hint: if
// sufficient is not empty, getClosedTimestampRLocked might return a timestamp
// that's lower than the maximum closed timestamp that we know about, as long as
// the returned timestamp is still >= sufficient. This is a performance
// optimization because we can avoid consulting the ClosedTimestampReceiver.
func (r *Replica) getCurrentClosedTimestamp(
	ctx context.Context,
	sufficient hlc.Timestamp,
	appliedLAI kvpb.LeaseAppliedIndex,
	leaseholderNodeId roachpb.NodeID,
	raftClosed hlc.Timestamp,
) hlc.Timestamp {
	sideTransportClosed := r.sideTransportClosedTimestamp.get(ctx, leaseholderNodeId,
		appliedLAI, sufficient)
	var maxClosed hlc.Timestamp
	maxClosed.Forward(raftClosed)
	maxClosed.Forward(sideTransportClosed)
	return maxClosed
}

// GetCurrentClosedTimestamp returns the current maximum closed timestamp for
// this range.
func (r *Replica) GetCurrentClosedTimestamp(ctx context.Context) hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getCurrentClosedTimestamp(ctx, hlc.Timestamp{}, /* sufficient */
		r.shMu.state.LeaseAppliedIndex, r.shMu.state.Lease.Replica.NodeID,
		r.shMu.state.RaftClosedTimestamp)
}

// tryConsistentFollowerRead implements the follower-coordinated approach for
// consistent follower reads described in RFC issue #72593. It sends an
// EstablishResolvedTimestamp request to the leaseholder, waits for the required
// lease applied index, and then serves the read from this follower.
func (r *Replica) tryConsistentFollowerRead(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	desc *roachpb.RangeDescriptor,
	leaseholderNodeId roachpb.NodeID,
) bool {
	// Check if we're already processing an EstablishResolvedTimestamp request
	// to prevent infinite recursion
	if len(ba.Requests) > 0 {
		for _, ru := range ba.Requests {
			if _, ok := ru.GetInner().(*kvpb.EstablishResolvedTimestampRequest); ok {
				// log.Infof(ctx, "IBRAHIM FALSE: already processing an EstablishResolvedTimestamp request")
				return false
			}
		}
	}

	// Step 1: Find the leaseholder replica descriptor by iterating through all replicas
	var leaseholderStoreID roachpb.StoreID
	found := false
	for _, replica := range desc.Replicas().Descriptors() {
		if replica.NodeID == leaseholderNodeId {
			leaseholderStoreID = replica.StoreID
			found = true
			break
		}
	}
	if !found {
		log.Eventf(ctx, "leaseholder store not found in range descriptor")
		// log.Infof(ctx, "IBRAHIM FALSE: leaseholder store not found in range descriptor")
		return false
	}

	// Check if we're trying to send to ourselves - this could cause infinite recursion
	if leaseholderStoreID == r.store.StoreID() {
		log.Eventf(ctx, "leaseholder is our own store, skipping EstablishResolvedTimestamp")
		// log.Infof(ctx, "IBRAHIM FALSE: leaseholder is our own store, skipping EstablishResolvedTimestamp")
		return false
	}

	////

	// Create separate EstablishResolvedTimestamp for each request
	var establishReqs []*kvpb.EstablishResolvedTimestampRequest

	for _, ru := range ba.Requests {
		req := ru.GetInner()
		span := req.Header().Span()

		// EstablishResolvedTimestamp requires a range (non-empty EndKey)
		// For point requests, convert to a minimal range [Key, Key.Next())
		establishSpan := span
		if len(span.EndKey) == 0 {
			establishSpan.EndKey = span.Key.Next()
		}

		establishReq := &kvpb.EstablishResolvedTimestampRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    establishSpan.Key,
				EndKey: establishSpan.EndKey,
			},
			Txn:       ba.Txn,
			Timestamp: ba.Timestamp,
			Span:      establishSpan,
		}
		establishReqs = append(establishReqs, establishReq)
	}

	// Add all establish requests to the batch
	establishBa := &kvpb.BatchRequest{
		Header: kvpb.Header{
			RangeID: desc.RangeID,
		},
	}
	for _, req := range establishReqs {
		establishBa.Add(req)
	}

	////

	// // Step 2: Create EstablishResolvedTimestamp request
	// // Calculate the span we need to establish the resolved timestamp for
	// startKey := ba.Requests[0].GetInner().Header().Key
	// endKey := ba.Requests[len(ba.Requests)-1].GetInner().Header().EndKey
	// if len(endKey) == 0 {
	// 	endKey = startKey.Next()
	// }

	// establishReq := &kvpb.EstablishResolvedTimestampRequest{
	// 	RequestHeader: kvpb.RequestHeader{
	// 		Key:    startKey,
	// 		EndKey: endKey,
	// 	},
	// 	Txn:       ba.Txn,
	// 	Timestamp: ba.Timestamp,
	// 	Span: roachpb.Span{
	// 		Key:    startKey,
	// 		EndKey: endKey,
	// 	},
	// }

	// // Step 3: Create a batch request to send to the leaseholder
	// establishBa := &kvpb.BatchRequest{
	// 	Header: kvpb.Header{
	// 		RangeID: desc.RangeID, // Set RangeID to help with routing
	// 	},
	// }

	// Set transaction context if this is a transactional request
	if ba.Txn != nil {
		establishBa.Txn = ba.Txn
	} else {
		// For non-transactional requests, set the timestamp
		establishBa.Header.Timestamp = ba.Timestamp
	}

	// establishBa.Add(establishReq)
	// if ba.Txn != nil {
	// 	establishBa.Txn = ba.Txn
	// }

	log.Eventf(ctx, "sending EstablishResolvedTimestamp request to leaseholder store %d", leaseholderStoreID)
	// log.Infof(ctx, "sending EstablishResolvedTimestamp request to leaseholder store %d", leaseholderStoreID)
	// log.Eventf(ctx, "request span: %s, range: %d", establishReq.Span, desc.RangeID)

	// Step 4: Send the request to the leaseholder via the DB
	// Use the appropriate sender based on whether this is a transactional request
	var br *kvpb.BatchResponse
	var pErr *kvpb.Error

	// Add a timeout to prevent hanging requests
	requestCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if ba.Txn != nil {
		// For transactional requests, use the transactional sender
		err := r.store.db.Txn(requestCtx, func(ctx context.Context, txn *kv.Txn) error {
			// Create a new batch within the transaction
			batch := txn.NewBatch()
			batch.Header = establishBa.Header
			for _, req := range establishReqs {
				batch.AddRawRequest(req)
			}

			// Run the batch
			err := txn.Run(ctx, batch)
			if err != nil {
				return err
			}

			// Extract the response
			br = batch.RawResponse()
			return nil
		})
		if err != nil {
			pErr = kvpb.NewError(err)
		}
	} else {
		// For non-transactional requests, use the non-transactional sender
		br, pErr = r.store.db.NonTransactionalSender().Send(requestCtx, establishBa)
	}

	if pErr != nil {
		log.Eventf(ctx, "EstablishResolvedTimestamp request failed: %v", pErr)
		// log.Infof(ctx, "EstablishResolvedTimestamp request failed: %v", pErr)
		// log.Infof(ctx, "IBRAHIM FALSE: EstablishResolvedTimestamp request failed: %v", pErr)
		return false
	}

	// Iterate through all responses and record the max lease applied index
	maxLeaseAppliedIndex := kvpb.LeaseAppliedIndex(0)
	for _, resp := range br.Responses {
		establishResp := resp.GetInner().(*kvpb.EstablishResolvedTimestampResponse)
		if establishResp.LeaseAppliedIndex > maxLeaseAppliedIndex {
			maxLeaseAppliedIndex = establishResp.LeaseAppliedIndex
		}
	}

	log.Eventf(ctx, "received lease applied index %d, waiting for our log to catch up",
		maxLeaseAppliedIndex)

	// log.Infof(ctx, "received lease applied index %d, waiting for our log to catch up",
	// 	maxLeaseAppliedIndex)
	// Step 5: Wait for our lease applied index to catch up to the required index
	const maxWaitTime = 500 * time.Millisecond
	startTime := time.Now()

	for {
		// Check our current lease applied index
		currentLAI := r.GetLeaseAppliedIndex()
		if currentLAI >= maxLeaseAppliedIndex {
			// We've caught up! We can now safely serve the read
			break
		}

		// Check for timeout
		if time.Since(startTime) > maxWaitTime {
			log.Eventf(ctx, "timeout waiting for lease applied index %d (current: %d)",
				maxLeaseAppliedIndex, currentLAI)
			// log.Infof(ctx, "IBRAHIM FALSE: timeout waiting for lease applied index %d (current: %d)",
			// maxLeaseAppliedIndex, currentLAI)
			// log.Infof(ctx, "timeout waiting for lease applied index %d (current: %d)",
			// 	maxLeaseAppliedIndex, currentLAI)
			return false
		}

		// Check if context was cancelled
		select {
		case <-ctx.Done():
			log.Eventf(ctx, "context cancelled while waiting for lease applied index")
			// log.Infof(ctx, "IBRAHIM FALSE: context cancelled while waiting for lease applied index")
			// log.Infof(ctx, "context cancelled while waiting for lease applied index")
			return false
		case <-time.After(1 * time.Millisecond):
			// Brief sleep before checking again
		}
	}

	// Step 6: We've successfully coordinated with the leaseholder and our log has
	// caught up. We can now serve the read from this follower with confidence
	// that it will be consistent.
	log.Eventf(ctx, "lease applied index caught up, serving consistent follower read")
	// log.Infof(ctx, "lease applied index caught up, serving consistent follower read")

	// Record that we successfully served a follower read
	r.store.metrics.FollowerReadsCount.Inc(1)
	if sp := tracing.SpanFromContext(ctx); sp.RecordingType() != tracingpb.RecordingOff {
		sp.RecordStructured(&kvpb.UsedFollowerRead{})
	}

	return true
}
