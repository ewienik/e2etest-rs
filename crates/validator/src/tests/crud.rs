/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use crate::tests::*;
use std::time::Duration;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "simple_create_drop_index",
            timeout,
            simple_create_drop_index,
        )
        .with_test(
            "simple_create_drop_multiple_indexes",
            timeout,
            simple_create_drop_multiple_indexes,
        )
}

async fn simple_create_drop_index(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    session.query_unpaged(
        "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        (),
    ).await.expect("failed to create a keyspace");

    session
        .use_keyspace("ks", false)
        .await
        .expect("failed to use a keyspace");

    session
        .query_unpaged(
            "
            CREATE TABLE tbl (id BIGINT PRIMARY KEY, embedding VECTOR<FLOAT, 3>)
            WITH cdc = {'enabled': true }
            ",
            (),
        )
        .await
        .expect("failed to create a table");

    session
        .query_unpaged(
            "CREATE INDEX idx ON tbl(embedding) USING 'vector_index'",
            (),
        )
        .await
        .expect("failed to create an index");

    while client.indexes().await.is_empty() {}
    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].keyspace.as_ref(), "ks");
    assert_eq!(indexes[0].index.as_ref(), "idx");

    session
        .query_unpaged("DROP INDEX idx", ())
        .await
        .expect("failed to drop an index");

    while !client.indexes().await.is_empty() {}

    session
        .query_unpaged("DROP KEYSPACE ks", ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn simple_create_drop_multiple_indexes(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    // Create keyspace
    // Different keyspace name have to be used until the issue VECTOR-213 is fixed.
    // When fixed please remove the comment and change the keyspace back to "ks"
    session.query_unpaged(
        "CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        (),
    ).await.expect("failed to create a keyspace");

    // Use keyspace
    session
        .use_keyspace("ks2", false)
        .await
        .expect("failed to use a keyspace");

    // Create table
    session
        .query_unpaged(
            "
            CREATE TABLE tbl (pk INT PRIMARY KEY, v1 VECTOR<FLOAT, 3>, v2 VECTOR<FLOAT, 3>)
            ",
            (),
        )
        .await
        .expect("failed to create a table");

    // Create index on column v1
    session
        .query_unpaged("CREATE INDEX idx1 ON tbl(v1) USING 'vector_index'", ())
        .await
        .expect("failed to create an index");

    // Wait for the index to be created
    wait_for(
        || async { client.indexes().await.len() == 1 },
        "Waiting for the first index to be created",
        Duration::from_secs(5),
    )
    .await;

    // Wait for the full scan to complete and check if ANN query succeeds on v1
    wait_for(
        || async {
            session
                .query_unpaged(
                    "SELECT * FROM tbl ORDER BY v1 ANN OF [1.0, 2.0, 3.0] LIMIT 5",
                    (),
                )
                .await
                .is_ok()
        },
        "Waiting for full scan to complete. ANN query should succeed",
        Duration::from_secs(5),
    )
    .await;

    // ANN query on v2 should not succeed without the index
    session
        .query_unpaged(
            "SELECT * FROM tbl ORDER BY v2 ANN OF [1.0, 2.0, 3.0] LIMIT 5",
            (),
        )
        .await
        .expect_err("ANN query should fail when index does not exist");

    // Create index on column v2
    session
        .query_unpaged("CREATE INDEX idx2 ON tbl(v2) USING 'vector_index'", ())
        .await
        .expect("failed to create an index");

    info!("waiting for the second index to be created");

    // Wait for the second index to be created
    wait_for(
        || async { client.indexes().await.len() == 2 },
        "Waiting for 2 indexes to be created",
        Duration::from_secs(5),
    )
    .await;

    // Check if ANN query on v1 still succeeds
    session
        .query_unpaged(
            "SELECT * FROM tbl ORDER BY v1 ANN OF [1.0, 2.0, 3.0] LIMIT 5",
            (),
        )
        .await
        .expect("failed to run ANN query");

    // Wait for the full scan to complete and check if ANN query succeeds on v2
    wait_for(
        || async {
            session
                .query_unpaged(
                    "SELECT * FROM tbl ORDER BY v2 ANN OF [1.0, 2.0, 3.0] LIMIT 5",
                    (),
                )
                .await
                .is_ok()
        },
        "Waiting for full scan to complete. ANN query should succeed",
        Duration::from_secs(5),
    )
    .await;

    // Drop index on column v1
    session
        .query_unpaged("DROP INDEX idx1", ())
        .await
        .expect("failed to drop an index");

    info!("waiting for the first index to be dropped");

    // Wait for the first index to be dropped
    wait_for(
        || async { client.indexes().await.len() == 1 },
        "Waiting for the first index to be dropped",
        Duration::from_secs(5),
    )
    .await;

    // ANN query on v1 should not succeed after dropping the index
    session
        .query_unpaged(
            "SELECT * FROM tbl ORDER BY v1 ANN OF [1.0, 2.0, 3.0] LIMIT 5",
            (),
        )
        .await
        .expect_err("ANN query should fail when index does not exist");

    // Check if ANN query on v2 still succeeds
    session
        .query_unpaged(
            "SELECT * FROM tbl ORDER BY v2 ANN OF [1.0, 2.0, 3.0] LIMIT 5",
            (),
        )
        .await
        .expect("failed to run ANN query");

    // Drop index on column v2
    session
        .query_unpaged("DROP INDEX idx2", ())
        .await
        .expect("failed to drop an index");

    // Wait for the second index to be dropped
    wait_for(
        || async { client.indexes().await.is_empty() },
        "Waiting for all indexes to be dropped",
        Duration::from_secs(5),
    )
    .await;

    // Drop keyspace
    session
        .query_unpaged("DROP KEYSPACE ks2", ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
