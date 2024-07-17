// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDeadlockDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(
		t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs:      base.TestServerArgs{},
		},
	)
	defer tc.Stopper().Stop(ctx)
	for _, server := range tc.Servers {
		st := server.ApplicationLayer().ClusterSettings()
		st.Manual.Store(true)
		// Let transactions push immediately to detect deadlocks. The test creates a
		// large amount of contention and dependency cycles, and could take a long
		// time to complete without this.
		concurrency.LockTableDeadlockOrLivenessDetectionPushDelay.Override(ctx,
			&server.SystemLayer().ClusterSettings().SV, time.Millisecond*50000)
	}

	s0 := tc.Server(0).ApplicationLayer()
	//s1 := tc.Server(1).ApplicationLayer()

	// Start a SQL session as admin on node 1.
	sql0 := sqlutils.MakeSQLRunner(s0.SQLConn(t))
	results := sql0.QueryStr(t, "SELECT session_id FROM [SHOW SESSIONS]")
	fmt.Println("num sessions:", len(results))
	fmt.Println("sessionID:", results[0])

	//srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	//defer srv.Stopper().Stop(ctx)
	//s := srv.ApplicationLayer()

	//concurrency.LockTableDeadlockOrLivenessDetectionPushDelay.Override(ctx,
	//	&srv.SystemLayer().ClusterSettings().SV, 100*time.Millisecond)

	//_, err := tc.ServerConn(0).Exec("SET deadlock_timeout = 11")
	//require.NoError(t, err)

	//rootSQLRunner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	//conn1 := sqlutils.MakeSQLRunner(tc.ApplicationLayer(0).SQLConn(t))
	//conn1.Exec(t, "SET deadlock_timeout = 123")
	//_, err := tc.ServerConn(0).Exec("SET SESSION deadlock_timeout = 99999")
	//require.NoError(t, err)

	tx := sql0.Begin(t)
	var name string
	err := tx.QueryRow("SHOW deadlock_timeout").Scan(&name)
	require.NoError(t, err)

	fmt.Println("deadlock_timeout:", name)
	require.NoError(t, tx.Commit())

	//tx0 := rootSQLRunner.Begin(t)
	tx0 := sql0.Begin(t)

	require.NoError(t, err)

	//_, err := tx0.Exec("SET CLUSTER SETTING server.max_open_transactions_per_gateway = 4")
	//require.NoError(t, err)

	_, err = tx0.Exec("CREATE TABLE test1 (id int, age int)")

	require.NoError(t, err)

	_, err = tx0.Exec("CREATE TABLE test2 (id int, age int)")
	require.NoError(t, err)

	_, err = tx0.Exec("INSERT INTO test1 (id, age) VALUES (1, 10)")
	require.NoError(t, err)

	_, err = tx0.Exec("INSERT INTO test2 (id, age) VALUES (1, 10)")
	require.NoError(t, err)

	require.NoError(t, tx0.Commit())

	tx1 := sql0.Begin(t)
	require.NoError(t, err)
	fmt.Println("TX1 started")
	_, err = tx1.Exec("SET local deadlock_timeout = 123")
	require.NoError(t, err)

	results = sql0.QueryStr(t, "SELECT session_id FROM [SHOW SESSIONS]")
	fmt.Println("num sessions:", len(results))
	fmt.Println("sessionID:", results)

	//_, err = tx1.Exec("SET local deadlock_timeout = 11")
	//require.NoError(t, err)

	//_, err = tx2.Exec("SET local deadlock_timeout = 11")
	//require.NoError(t, err)

	//_, err = tx1.Exec("SET LOCAL deadlock_timeout = 12345")
	//require.NoError(t, err)

	//_, err = tx1.Exec("SET sql.defaults.lock_timeout = '1s'")
	//require.NoError(t, err)
	//
	//_, err = tx1.Exec("SET lock_timeout = '12s'")
	//require.NoError(t, err)

	_, err = tx1.Exec("UPDATE test1 SET age = 100 WHERE id = 1")
	require.NoError(t, err)
	fmt.Println("tx1 k1")

	tx2 := sql0.Begin(t)

	_, err = tx2.Exec("SET TRANSACTION PRIORITY HIGH")
	require.NoError(t, err)
	_, err = tx2.Exec("SET local deadlock_timeout = 123")
	require.NoError(t, err)

	fmt.Println("TX2 started")
	_, err = tx2.Exec("UPDATE test2 SET age = 200 WHERE id = 1")
	require.NoError(t, err)
	fmt.Println("tx2 k2")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, err = tx2.Exec("UPDATE test1 SET age = 300 WHERE id = 1")
		require.NoError(t, err)
		fmt.Println("tx2 k1")
		wg.Done()
	}()

	time.Sleep(time.Millisecond * 1000)
	_, err = tx1.Exec("UPDATE test2 SET age = 400 WHERE id = 1")
	require.Contains(t, err.Error(), "TransactionRetryWithProtoRefreshError")
	//require.NoError(t, err)
	fmt.Println("tx1 k2")
	wg.Wait()

	require.NoError(t, tx1.Rollback())
	require.NoError(t, tx2.Commit())
}

// Test the server.max_open_transactions_per_gateway cluster setting. Only
// non-admins are subject to the limit.
func TestMaxOpenTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	rootSQLRunner := sqlutils.MakeSQLRunner(db)
	rootSQLRunner.Exec(t, "SET CLUSTER SETTING server.max_open_transactions_per_gateway = 4")
	rootSQLRunner.Exec(t, "CREATE USER testuser")

	testUserDB := s.SQLConn(t, serverutils.User("testuser"))

	tx1 := rootSQLRunner.Begin(t)
	_, err := testUserDB.Exec("SELECT 1")
	require.NoError(t, err)

	tx2 := rootSQLRunner.Begin(t)
	_, err = testUserDB.Exec("SELECT 1")
	require.NoError(t, err)

	tx3 := rootSQLRunner.Begin(t)
	_, err = testUserDB.Exec("SELECT 1")
	require.NoError(t, err)

	// After four transactions have been opened, testuser cannot run a query.
	tx4 := rootSQLRunner.Begin(t)
	_, err = testUserDB.Exec("SELECT 1")
	require.ErrorContains(t, err, "cannot execute operation due to server.max_open_transactions_per_gateway cluster setting")

	// testuser also cannot run anything in an explicit transaction. Starting the
	// transaction is allowed though.
	testuserTx, err := testUserDB.Begin()
	require.NoError(t, err)
	_, err = testuserTx.Exec("SELECT 1")
	require.ErrorContains(t, err, "cannot execute operation due to server.max_open_transactions_per_gateway cluster setting")
	require.NoError(t, testuserTx.Rollback())

	// Increase the limit, allowing testuser to run queries in a transaction.
	rootSQLRunner.Exec(t, "SET CLUSTER SETTING server.max_open_transactions_per_gateway = 5")
	testuserTx, err = testUserDB.Begin()
	require.NoError(t, err)
	_, err = testuserTx.Exec("SELECT 1")
	require.NoError(t, err)

	// Lower the limit again, and verify that the setting applies to transactions
	// that were already open at the time the setting changed.
	rootSQLRunner.Exec(t, "SET CLUSTER SETTING server.max_open_transactions_per_gateway = 4")
	_, err = testuserTx.Exec("SELECT 2")
	require.ErrorContains(t, err, "cannot execute operation due to server.max_open_transactions_per_gateway cluster setting")
	require.NoError(t, testuserTx.Rollback())

	// Making testuser admin should allow it to run queries.
	rootSQLRunner.Exec(t, "GRANT admin TO testuser")
	_, err = testUserDB.Exec("SELECT 1")
	require.NoError(t, err)

	testuserTx, err = testUserDB.Begin()
	require.NoError(t, err)
	_, err = testuserTx.Exec("SELECT 1")
	require.NoError(t, err)
	require.NoError(t, testuserTx.Rollback())

	require.NoError(t, tx1.Commit())
	require.NoError(t, tx2.Commit())
	require.NoError(t, tx3.Commit())
	require.NoError(t, tx4.Commit())
}
