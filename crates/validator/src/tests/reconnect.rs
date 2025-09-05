/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use crate::scylla_cluster::ScyllaClusterExt;
use crate::tests::*;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "reconnect_doesnt_break_fullscan",
            timeout,
            reconnect_doesnt_break_fullscan,
        )
}

async fn reconnect_doesnt_break_fullscan(actors: TestActors) {
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
            CREATE TABLE tbl (id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>)
            WITH cdc = {'enabled': true }
            ",
            (),
        )
        .await
        .expect("failed to create a table");

    let stmt = session
        .prepare("INSERT INTO tbl (id, embedding) VALUES (?, [1.0, 2.0, 3.0])")
        .await
        .expect("failed to prepare a statement");

    for id in 0..1000 {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

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
    let index = &indexes[0];
    assert_eq!(index.keyspace.as_ref(), "ks");
    assert_eq!(index.index.as_ref(), "idx");

    let result = session
        .query_unpaged(
            "SELECT * FROM tbl ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 1",
            (),
        )
        .await;

    match &result {
        Err(e) if format!("{e:?}").contains("503 Service Unavailable") => {}
        _ => panic!("Expected SERVICE_UNAVAILABLE error, got: {result:?}"),
    }

    actors.db.down().await;

    sleep(Duration::from_secs(1)).await;
    let count = client.count(&index.keyspace, &index.index).await;
    assert!(count.is_some() && count.unwrap() < 1000);
    actors.db.up(get_default_vs_url(&actors).await, None).await;

    assert!(actors.db.wait_for_ready().await);

    wait_for(
        || async {
            session
                .query_unpaged(
                    "SELECT * FROM tbl ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 1",
                    (),
                )
                .await
                .is_ok()
        },
        "Waiting for index build",
        Duration::from_secs(10),
    )
    .await;

    session
        .query_unpaged("DROP KEYSPACE ks", ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
