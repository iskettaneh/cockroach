package kvserver_test

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
	*roachpb.RangeDescriptor /* desc3 */) {
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
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

	return tc, scratchKey, desc1, desc2, desc3
}

func makeSST(t *testing.T, ctx context.Context, store *kvserver.Store, externURI string) *cloud.ExternalStorage {
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
		{Key: "a", Value: "a-val"},
		{Key: "d", Value: "d-val"},
		{Key: "g", Value: "g-val"},
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
		kvs = append(kvs, storageutils.PointKV(expKV.Key, 1, expKV.Value))
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
		testFunc                 func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key)
	}{
		{
			name:                     "put and get with deleted span at range boundaries",
			deletedExternalSpanStart: roachpb.RKey("d"),
			deletedExternalSpanEnd:   roachpb.RKey("g"),
			testFunc: func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key) {
				_, err := kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a")), []byte("value")))
				require.NoError(t, err.GoError())

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("g")), []byte("value")))
				require.NoError(t, err.GoError())

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("d")), []byte("value")))
				require.True(t, errors.Is(err.GoError(), cloud.ErrFileDoesNotExist))

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("f")), []byte("value")))
				require.True(t, errors.Is(err.GoError(), cloud.ErrFileDoesNotExist))
			},
		},
		{
			name:                     "put and get with deleted span less than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("e"),
			deletedExternalSpanEnd:   roachpb.RKey("f"),
			testFunc: func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key) {
				_, err := kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a")), []byte("value")))
				require.NoError(t, err.GoError())

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("g")), []byte("value")))
				require.NoError(t, err.GoError())

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e")), []byte("value")))
				require.True(t, errors.Is(err.GoError(), cloud.ErrFileDoesNotExist))

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("f")), []byte("value")))
				require.NoError(t, err.GoError())
			},
		},
		{
			name:                     "put and get with deleted span more than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("a"),
			deletedExternalSpanEnd:   roachpb.RKey("z"),
			testFunc: func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key) {
				_, err := kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a")), []byte("value")))
				require.True(t, errors.Is(err.GoError(), cloud.ErrFileDoesNotExist))

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("g")), []byte("value")))
				require.True(t, errors.Is(err.GoError(), cloud.ErrFileDoesNotExist))

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e")), []byte("value")))
				require.True(t, errors.Is(err.GoError(), cloud.ErrFileDoesNotExist))

				_, err = kv.SendWrapped(ctx, store.TestSender(),
					putArgs(testutils.MakeKey(scratchKey, roachpb.RKey("z")), []byte("value")))
				require.NoError(t, err.GoError())
			},
		},
		{
			name:                     "split with deleted span at range boundaries",
			deletedExternalSpanStart: roachpb.RKey("d"),
			deletedExternalSpanEnd:   roachpb.RKey("g"),
			testFunc: func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key) {
				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.Error(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("b"))))
				require.NoError(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("z"))))
				require.NoError(t, pErr.GoError())
			},
		},
		{
			name:                     "split with deleted span at more than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("b"),
			deletedExternalSpanEnd:   roachpb.RKey("y"),
			testFunc: func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key) {
				_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("e"))))
				require.Error(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("a"))))
				require.Error(t, pErr.GoError())

				_, pErr = kv.SendWrapped(ctx, store.TestSender(), adminSplitArgs(testutils.MakeKey(scratchKey, roachpb.RKey("z"))))
				require.Error(t, pErr.GoError())
			},
		},
		{
			name:                     "split with deleted span at less than range boundaries",
			deletedExternalSpanStart: roachpb.RKey("e"),
			deletedExternalSpanEnd:   roachpb.RKey("f"),
			testFunc: func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key) {
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
			},
		},
		{
			name:                     "merge with deleted span at range boundaries",
			deletedExternalSpanStart: roachpb.RKey("d"),
			deletedExternalSpanEnd:   roachpb.RKey("g"),
			testFunc: func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key) {
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
			testFunc: func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key) {
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
			testFunc: func(t *testing.T, ctx context.Context, store *kvserver.Store, scratchKey roachpb.Key) {
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
			extStore := makeSST(t, ctx, store, externURI)

			fileName1 := "external-1.sst"

			size1, err := (*extStore).Size(ctx, fileName1)
			require.NoError(t, err)
			fmt.Printf("size1: %d\n", size1)

			errLink := store.DB().LinkExternalSSTable(ctx, roachpb.Span{
				Key:    testutils.MakeKey(scratchKey, testCase.deletedExternalSpanStart),
				EndKey: testutils.MakeKey(scratchKey, testCase.deletedExternalSpanEnd),
			}, kvpb.LinkExternalSSTableRequest_ExternalFile{
				Locator:                 externURI,
				Path:                    fileName1,
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

			testCase.testFunc(t, ctx, store, scratchKey)
		})
	}

}
