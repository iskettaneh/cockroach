package kvserver_test

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"testing"
)

// testSetup returns a test cluster with a scratch range split three-ways:
// [-, d), [d, g), [g, +).
func testSetup(t *testing.T, ctx context.Context) (
	*testcluster.TestCluster,
	roachpb.Key,
	*roachpb.RangeDescriptor, /* desc1 */
	*roachpb.RangeDescriptor, /* desc2 */
	*roachpb.RangeDescriptor  /* desc3 */) {
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableMergeQueue:         true,
					DisableLoadBasedSplitting: true,
					//DisableCanAckBeforeApplication: true,
				},
			},
		},
	})
	// Create three ranges: [-, d), [d, g), [g, +)
	scratchKey := tc.ScratchRange(t)

	store := tc.GetFirstStoreFromServer(t, 0)
	args := adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.Key("d")))
	_, err := kv.SendWrapped(ctx, store.TestSender(), args)
	require.NoError(t, err.GoError())

	args = adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.Key("g")))
	_, err = kv.SendWrapped(ctx, store.TestSender(), args)
	require.NoError(t, err.GoError())

	//desc1 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
	//desc2 := store.LookupReplica(testutils.MakeKey(tmpDesc.StartKey, roachpb.RKey("d"))).Desc()
	//desc3 := store.LookupReplica(testutils.MakeKey(tmpDesc.StartKey, roachpb.RKey("g"))).Desc()

	//expKVs := []struct {
	//	Key   string
	//	Value string
	//}{
	//	{Key: "a", Value: "a-val"},
	//	{Key: "d", Value: "d-val"},
	//	{Key: "g", Value: "g-val"},
	//}
	//kvs := make([]interface{}, 0, len(expKVs))
	//for _, expKV := range expKVs {
	//	kvs = append(kvs, storageutils.PointKV(expKV.Key, 1, expKV.Value))
	//}

	desc1 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
	desc2 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("d"))).Desc()
	desc3 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("g"))).Desc()

	tc.AddVotersOrFatal(t, desc1.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
	tc.AddVotersOrFatal(t, desc2.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
	tc.AddVotersOrFatal(t, desc3.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))

	incA := incrementArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a")), 1)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), incA); pErr != nil {
		t.Fatal(pErr)
	}

	incD := incrementArgs(testutils.MakeKey(scratchKey, roachpb.RKey("d")), 1)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), incD); pErr != nil {
		t.Fatal(pErr)
	}

	incG := incrementArgs(testutils.MakeKey(scratchKey, roachpb.RKey("g")), 1)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), incG); pErr != nil {
		t.Fatal(pErr)
	}

	tc.WaitForValues(t, testutils.MakeKey(scratchKey, roachpb.RKey("a")), []int64{1, 1, 1})
	tc.WaitForValues(t, testutils.MakeKey(scratchKey, roachpb.RKey("d")), []int64{1, 1, 1})
	tc.WaitForValues(t, testutils.MakeKey(scratchKey, roachpb.RKey("g")), []int64{1, 1, 1})

	return tc, scratchKey, desc1, desc2, desc3
}

func makeSST(t *testing.T, ctx context.Context, store *kvserver.Store, externURI string, scratchKey roachpb.Key) *cloud.ExternalStorage {
	extStore, err := cloud.EarlyBootExternalStorageFromURI(ctx,
		externURI,
		base.ExternalIODirConfig{},
		store.ClusterSettings(),
		nil, /* limiters */
		cloud.NilMetrics)
	require.NoError(t, err)

	expKVs := []struct {
		Key   string
		Value string
	}{
		{Key: "b", Value: "a-val"},
		{Key: "e", Value: "d-val"},
		{Key: "h", Value: "g-val"},
	}
	//kvs1 := make([]interface{}, 0, 1)
	//kvs1 = append(kvs1, storageutils.PointKV(expKVs[0].Key, 1, expKVs[0].Value))
	//
	//kvs2 := make([]interface{}, 0, 1)
	//kvs2 = append(kvs1, storageutils.PointKV(expKVs[1].Key, 1, expKVs[1].Value))
	//
	//kvs3 := make([]interface{}, 0, 1)
	//kvs3 = append(kvs1, storageutils.PointKV(expKVs[2].Key, 1, expKVs[2].Value))
	//
	//fileName1 := "external-1.sst"
	//fileName2 := "external-2.sst"
	//fileName3 := "external-3.sst"
	//sst1, _, _ := storageutils.MakeSST(t, store.ClusterSettings(), kvs1)
	//sst2, _, _ := storageutils.MakeSST(t, store.ClusterSettings(), kvs2)
	//sst3, _, _ := storageutils.MakeSST(t, store.ClusterSettings(), kvs3)
	//
	//w1, errWriter := extStore.Writer(ctx, fileName1)
	//require.NoError(t, errWriter)
	//_, errWriter = w1.Write(sst1)
	//require.NoError(t, errWriter)
	//require.NoError(t, w1.Close())
	//
	//w2, errWriter := extStore.Writer(ctx, fileName2)
	//require.NoError(t, errWriter)
	//_, errWriter = w2.Write(sst2)
	//require.NoError(t, errWriter)
	//require.NoError(t, w2.Close())
	//
	//w3, errWriter := extStore.Writer(ctx, fileName3)
	//require.NoError(t, errWriter)
	//_, errWriter = w3.Write(sst3)
	//require.NoError(t, errWriter)
	//require.NoError(t, w3.Close())

	kvs := make([]interface{}, 0, len(expKVs))
	for _, expKV := range expKVs {

		val, err := storage.EncodeMVCCValue(storageutils.StringValue(expKV.Value))
		if err != nil {
			panic(err)
		}

		keyVal := storage.MVCCKeyValue{
			Key:   storage.MVCCKey{Key: testutils.MakeKey(scratchKey, roachpb.RKey(expKV.Key)), Timestamp: hlc.Timestamp{WallTime: 1}},
			Value: val,
		}
		kvs = append(kvs, keyVal)
	}
	fileName := "external-1.sst"
	sst, _, _ := storageutils.MakeSST(t, store.ClusterSettings(), kvs)
	w, err := extStore.Writer(ctx, fileName)
	require.NoError(t, err)
	_, err = w.Write(sst)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return &extStore
}

func TestIbrahim(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()
	const externURI = "nodelocal://1/external-files"

	//
	////splitArgs := adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e")))
	////if _, pErr := kv.SendWrapped(ctx, store.TestSender(), splitArgs); pErr != nil {
	////	t.Fatal(pErr)
	////}
	////
	////if _, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(desc1.StartKey.AsRawKey())); pErr != nil {
	////	t.Fatal(pErr)
	////}
	////
	////if _, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(desc1.StartKey.AsRawKey())); pErr != nil {
	////	t.Fatal(pErr)
	////}
	////
	//desc1 = store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
	//desc2 = store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("d"))).Desc()
	//desc3 = store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("g"))).Desc()
	//fmt.Printf("desc1: %+v, desc2: %+v, desc3: %+v\n", desc1, desc2, desc3)
	//
	////pArgs = putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e")), []byte("value"))
	////if _, err := kv.SendWrapped(ctx, store.TestSender(), pArgs); err != nil {
	////	t.Fatal(err)
	////}
	//
	////if _, err := kv.SendWrapped(ctx, store.TestSender(), getArgs(roachpb.Key("b"))); err != nil {
	////	t.Fatal(err)
	////}
	//
	//descTableSpan := roachpb.Span{
	//	Key:    testutils.MakeKey(scratchKey, roachpb.RKey("a")),
	//	EndKey: testutils.MakeKey(scratchKey, roachpb.RKey("z")),
	//}
	//
	//ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)
	//db := tc.Server(0).DB()
	//startTS := db.Clock().Now()
	//evChan := make(chan kvcoord.RangeFeedMessage)
	//rangefeedErrChan := make(chan error, 1)
	//ctxToCancel, cancel := context.WithCancel(ctx)
	//defer cancel()
	//go func() {
	//	rangefeedErrChan <- ds.RangeFeed(ctxToCancel, []kvcoord.SpanTimePair{{Span: descTableSpan, StartAfter: startTS}}, evChan)
	//}()
	//
	//pArgs := putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a")), []byte("value"))
	//if _, err := kv.SendWrapped(ctx, store.TestSender(), pArgs); err != nil {
	//	t.Fatal(err)
	//}
	//
	//pArgs = putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("g")), []byte("value"))
	//if _, err := kv.SendWrapped(ctx, store.TestSender(), pArgs); err != nil {
	//	t.Fatal(err)
	//}
	//
	//for {
	//	ev := <-evChan
	//	fmt.Printf("ev: %+v\n", ev)
	//
	//	if ev.Val != nil {
	//		fmt.Printf("ev.Val: %+v\n", ev.Val)
	//
	//	}
	//}
	//cancel()

	////////////////////

	testCases := []struct {
		name                     string
		deletedExternalSpanStart roachpb.RKey
		deletedExternalSpanEnd   roachpb.RKey
		testFunc                 func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key)
	}{
		{
			name:                     "put and get with deleted span at range boundaries",
			deletedExternalSpanStart: roachpb.RKey("d"),
			deletedExternalSpanEnd:   roachpb.RKey("g"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				b := kv.Batch{}
				b.Scan(testutils.MakeKey(scratchKey, roachpb.RKey("a")), testutils.MakeKey(scratchKey, roachpb.RKey("z")))
				err := store.DB().Run(ctx, &b)
				require.Error(t, err)
				require.Regexp(t, "no such file or directory", err)

				//_, err := kv.SendWrapped(ctx, store.TestSender(),
				//	putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a")), []byte("value")))
				//require.NoError(t, err.GoError())
				//
				//_, err = kv.SendWrapped(ctx, store.TestSender(),
				//	putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("g")), []byte("value")))
				//require.NoError(t, err.GoError())
				//
				//_, err = kv.SendWrapped(ctx, store.TestSender(),
				//	getArgs(testutils.MakeKey(scratchKey, roachpb.RKey("f"))))
				//fmt.Printf("err.GoError(): %+v\n", err.GoError())
				//require.Error(t, err.GoError())
				//require.Regexp(t, "no such file or directory", err)
				//
				//drArgs := &kvpb.DeleteRangeRequest{
				//	UpdateRangeDeleteGCHint: true,
				//	UseRangeTombstone:       true,
				//	RequestHeader: kvpb.RequestHeader{
				//		Key:    testutils.MakeKey(scratchKey, roachpb.RKey("a")),
				//		EndKey: testutils.MakeKey(scratchKey, roachpb.RKey("z")),
				//	},
				//}
				//_, pErr := kv.SendWrapped(ctx, store.TestSender(), drArgs)
				//require.NoError(t, pErr.GoError(), "failed to send delete range request")

				b = kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("d")), testutils.MakeKey(scratchKey, roachpb.RKey("g")))
				require.NoError(t, store.DB().Run(ctx, &b))

				b = kv.Batch{}
				b.Scan(testutils.MakeKey(scratchKey, roachpb.RKey("a")), testutils.MakeKey(scratchKey, roachpb.RKey("z")))
				require.NoError(t, store.DB().Run(ctx, &b))
			},
		},
		{
			name:                     "put and get with deleted span less than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("e"),
			deletedExternalSpanEnd:   roachpb.RKey("f"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				_, err := kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a")), []byte("value")))
				require.NoError(t, err.GoError())

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("g")), []byte("value")))
				require.NoError(t, err.GoError())

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e")), []byte("value")))
				require.Error(t, err.GoError())
				require.Regexp(t, "no such file or directory", err)

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("f")), []byte("value")))
				require.NoError(t, err.GoError())

				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("e")), testutils.MakeKey(scratchKey, roachpb.RKey("f")))
				require.NoError(t, store.DB().Run(ctx, &b))

				b = kv.Batch{}
				b.Scan(testutils.MakeKey(scratchKey, roachpb.RKey("a")), testutils.MakeKey(scratchKey, roachpb.RKey("z")))
				require.NoError(t, store.DB().Run(ctx, &b))

			},
		},
		{
			name:                     "put and get with deleted span more than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("a"),
			deletedExternalSpanEnd:   roachpb.RKey("z"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				_, err := kv.SendWrapped(ctx, store.TestSender(),
					getArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a"))))
				fmt.Printf("!!! IBRAHIM !!! 1\n")
				require.Error(t, err.GoError())
				require.Regexp(t, "no such file or directory", err)

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					getArgs(testutils.MakeKey(scratchKey, roachpb.RKey("g"))))
				require.Error(t, err.GoError())
				require.Regexp(t, "no such file or directory", err)
				fmt.Printf("!!! IBRAHIM !!! 2\n")

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					getArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.Error(t, err.GoError())
				require.Regexp(t, "no such file or directory", err)

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					getArgs(testutils.MakeKey(scratchKey, roachpb.RKey("z"))))
				require.NoError(t, err.GoError())
				fmt.Printf("!!! IBRAHIM !!! 3\n")

				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("a")), testutils.MakeKey(scratchKey, roachpb.RKey("z")))
				require.NoError(t, store.DB().Run(ctx, &b))
				fmt.Printf("!!! IBRAHIM !!! 4\n")

				b = kv.Batch{}
				b.Scan(testutils.MakeKey(scratchKey, roachpb.RKey("a")), testutils.MakeKey(scratchKey, roachpb.RKey("z")))
				require.NoError(t, store.DB().Run(ctx, &b))
				fmt.Printf("!!! IBRAHIM !!! 5\n")

			},
		},
		{
			name:                     "split with deleted span at range boundaries",
			deletedExternalSpanStart: roachpb.RKey("d"),
			deletedExternalSpanEnd:   roachpb.RKey("g"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.Error(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("b"))))
				require.NoError(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("z"))))
				require.NoError(t, pErr.GoError())

				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("d")), testutils.MakeKey(scratchKey, roachpb.RKey("g")))
				require.NoError(t, store.DB().Run(ctx, &b))

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.NoError(t, pErr.GoError())
			},
		},
		{
			name:                     "split with deleted span at more than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("b"),
			deletedExternalSpanEnd:   roachpb.RKey("y"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.Error(t, pErr.GoError())

				//_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a"))))
				//require.Error(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("z"))))
				require.Error(t, pErr.GoError())

				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("b")), testutils.MakeKey(scratchKey, roachpb.RKey("y")))
				require.NoError(t, store.DB().Run(ctx, &b))

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.NoError(t, pErr.GoError())
			},
		},
		{
			name:                     "split with deleted span at less than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("e"),
			deletedExternalSpanEnd:   roachpb.RKey("f"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.Error(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("b"))))
				require.NoError(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("z"))))
				require.NoError(t, pErr.GoError())

				desc1 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
				desc2 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("d"))).Desc()
				desc3 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("g"))).Desc()
				fmt.Printf("desc1: %+v, desc2: %+v, desc3: %+v\n", desc1, desc2, desc3)

				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("e")), testutils.MakeKey(scratchKey, roachpb.RKey("f")))
				require.NoError(t, store.DB().Run(ctx, &b))

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.NoError(t, pErr.GoError())
			},
		},
		{
			name:                     "merge with deleted span at range boundaries",
			deletedExternalSpanStart: roachpb.RKey("d"),
			deletedExternalSpanEnd:   roachpb.RKey("g"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.NoError(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a"))))
				require.NoError(t, pErr.GoError())

				desc1 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
				desc2 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("d"))).Desc()
				desc3 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("g"))).Desc()
				require.Equal(t, desc1, desc2)
				require.Equal(t, desc2, desc3)

				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("d")), testutils.MakeKey(scratchKey, roachpb.RKey("g")))
				require.NoError(t, store.DB().Run(ctx, &b))
			},
		},
		{
			name:                     "merge with excised span at range boundaries",
			deletedExternalSpanStart: roachpb.RKey("d"),
			deletedExternalSpanEnd:   roachpb.RKey("g"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("d")), testutils.MakeKey(scratchKey, roachpb.RKey("g")))
				require.NoError(t, store.DB().Run(ctx, &b))

				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.NoError(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a"))))
				require.NoError(t, pErr.GoError())

				desc1 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
				desc2 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("d"))).Desc()
				desc3 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("g"))).Desc()
				require.Equal(t, desc1, desc2)
				require.Equal(t, desc2, desc3)
			},
		},
		{
			name:                     "merge with deleted span at more than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("b"),
			deletedExternalSpanEnd:   roachpb.RKey("y"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.NoError(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a"))))
				require.NoError(t, pErr.GoError())

				desc1 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
				desc2 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("d"))).Desc()
				desc3 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("g"))).Desc()
				require.Equal(t, desc1, desc2)
				require.Equal(t, desc2, desc3)

				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("b")), testutils.MakeKey(scratchKey, roachpb.RKey("y")))
				require.NoError(t, store.DB().Run(ctx, &b))
			},
		},
		{
			name:                     "merge with excised span at more than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("b"),
			deletedExternalSpanEnd:   roachpb.RKey("y"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("b")), testutils.MakeKey(scratchKey, roachpb.RKey("y")))
				require.NoError(t, store.DB().Run(ctx, &b))

				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.NoError(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a"))))
				require.NoError(t, pErr.GoError())

				desc1 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
				desc2 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("d"))).Desc()
				desc3 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("g"))).Desc()
				require.Equal(t, desc1, desc2)
				require.Equal(t, desc2, desc3)
			},
		},
		{
			name:                     "merge with deleted span at less than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("e"),
			deletedExternalSpanEnd:   roachpb.RKey("f"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.NoError(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a"))))
				require.NoError(t, pErr.GoError())

				desc1 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
				desc2 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("d"))).Desc()
				desc3 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("g"))).Desc()
				require.Equal(t, desc1, desc2)
				require.Equal(t, desc2, desc3)

				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("e")), testutils.MakeKey(scratchKey, roachpb.RKey("f")))
				require.NoError(t, store.DB().Run(ctx, &b))
			},
		},
		{
			name:                     "merge with excised span at less than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("e"),
			deletedExternalSpanEnd:   roachpb.RKey("f"),
			testFunc: func(t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store, scratchKey roachpb.Key) {
				b := kv.Batch{}
				b.GCAndExciseRange(testutils.MakeKey(scratchKey, roachpb.RKey("e")), testutils.MakeKey(scratchKey, roachpb.RKey("f")))
				require.NoError(t, store.DB().Run(ctx, &b))
				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.NoError(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a"))))
				require.NoError(t, pErr.GoError())

				desc1 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("a"))).Desc()
				desc2 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("d"))).Desc()
				desc3 := store.LookupReplica(testutils.MakeKey(scratchKey, roachpb.RKey("g"))).Desc()
				require.Equal(t, desc1, desc2)
				require.Equal(t, desc2, desc3)
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()

			tc, scratchKey, desc1, desc2, desc3 := testSetup(t, ctx)
			defer tc.Stopper().Stop(ctx)
			fmt.Printf("desc1: %+v, desc2: %+v, desc3: %+v\n", desc1, desc2, desc3)

			store := tc.GetFirstStoreFromServer(t, 0)
			extStore := makeSST(t, ctx, store, externURI, scratchKey)

			ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)
			db := tc.Server(0).DB()
			startTS := db.Clock().Now()
			evChan := make(chan kvcoord.RangeFeedMessage)
			rangefeedErrChan := make(chan error, 1)
			ctxToCancel, cancel := context.WithCancel(ctx)
			defer cancel()
			descTableSpan := roachpb.Span{
				Key:    testutils.MakeKey(scratchKey, roachpb.RKey("za")),
				EndKey: testutils.MakeKey(scratchKey, roachpb.RKey("zz")),
			}
			go func() {
				rangefeedErrChan <- ds.RangeFeed(ctxToCancel, []kvcoord.SpanTimePair{{Span: descTableSpan, StartAfter: startTS}}, evChan)
			}()

			fileName1 := "external-1.sst"

			size1, err := (*extStore).Size(ctx, fileName1)
			require.NoError(t, err)
			fmt.Printf("size1: %d\n", size1)

			fmt.Printf("StartKey: %+v\n", testutils.MakeKey(scratchKey, testCase.deletedExternalSpanStart))
			errLink := store.DB().LinkExternalSSTable(ctx, roachpb.Span{
				Key:    testutils.MakeKey(scratchKey, testCase.deletedExternalSpanStart),
				EndKey: testutils.MakeKey(scratchKey, testCase.deletedExternalSpanEnd),
			}, kvpb.LinkExternalSSTableRequest_ExternalFile{
				Locator:                 externURI,
				Path:                    "Asd",
				ApproximatePhysicalSize: uint64(size1),
				BackingFileSize:         uint64(size1),
				MVCCStats: &enginepb.MVCCStats{
					ContainsEstimates: 1,
					KeyBytes:          2,
					ValBytes:          10,
					KeyCount:          2,
					LiveCount:         2,
				},
			}, store.DB().Clock().Now())
			require.NoError(t, errLink)

			errDelete := (*extStore).Delete(ctx, fileName1)
			require.NoError(t, errDelete)

			//gArgs := getArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e")))
			//reply, pErr := kv.SendWrapped(ctx, store.TestSender(), gArgs)
			//if pErr.GoError() == nil {
			//	replyBytes, err := reply.(*kvpb.GetResponse).Value.GetBytes()
			//	require.NoError(t, err)
			//	fmt.Printf("replyBytes: %s\n", replyBytes)
			//}
			//require.Error(t, pErr.GoError())

			testCase.testFunc(t, ctx, tc, store, scratchKey)

			runConsistencyCheck := func() *kvpb.CheckConsistencyResponse {
				req := kvpb.CheckConsistencyRequest{
					RequestHeader: kvpb.RequestHeader{ // keys span that includes "a" & "c"
						Key:    testutils.MakeKey(scratchKey, roachpb.RKey("a")),
						EndKey: testutils.MakeKey(scratchKey, roachpb.RKey("z")),
					},
					Mode: kvpb.ChecksumMode_CHECK_FULL,
				}
				resp, err := kv.SendWrapped(context.Background(), store.DB().NonTransactionalSender(), &req)
				require.NoError(t, err.GoError())
				return resp.(*kvpb.CheckConsistencyResponse)
			}

			constResp := runConsistencyCheck()
			for i := range len(constResp.Result) {
				if constResp.Result[i].Status != kvpb.CheckConsistencyResponse_RANGE_CONSISTENT &&
					constResp.Result[i].Status != kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_ESTIMATED {
					t.Fatalf("expected range to be consistent, but found: %+v", constResp.Result[i])
				}
			}

			pArgs := putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("zd")), []byte("value"))
			if _, err := kv.SendWrapped(ctx, store.TestSender(), pArgs); err != nil {
				t.Fatal(err)
			}
			for {
				ev := <-evChan
				fmt.Printf("ev: %+v\n", ev)
				if ev.Val != nil && ev.Val.Key.Equal(testutils.MakeKey(scratchKey, roachpb.RKey("zd"))) {
					fmt.Printf("ev.Val: %+v\n", ev.Val)
					break
				}
			}
		})
	}

}
