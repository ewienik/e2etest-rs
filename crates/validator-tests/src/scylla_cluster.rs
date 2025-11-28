/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::net::Ipv4Addr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub enum ScyllaCluster {
    Version {
        tx: oneshot::Sender<String>,
    },
    Start {
        vs_uri: String,
        db_ips: Vec<Ipv4Addr>,
        conf: Option<Vec<u8>>,
    },
    WaitForReady {
        tx: oneshot::Sender<bool>,
    },
    Stop {
        tx: oneshot::Sender<()>,
    },
    Up {
        vs_uri: String,
        conf: Option<Vec<u8>>,
    },
    UpNode {
        vs_uri: String,
        db_ip: Ipv4Addr,
        conf: Option<Vec<u8>>,
    },
    Down {
        tx: oneshot::Sender<()>,
    },
    DownNode {
        db_ip: Ipv4Addr,
        tx: oneshot::Sender<()>,
    },
    Flush {
        tx: oneshot::Sender<()>,
    },
}

pub trait ScyllaClusterExt {
    /// Returns the version of the ScyllaDB executable.
    fn version(&self) -> impl Future<Output = String>;

    /// Starts the ScyllaDB cluster with the given vector store URI and database IPs.
    fn start(
        &self,
        vs_uri: String,
        db_ips: Vec<Ipv4Addr>,
        conf: Option<Vec<u8>>,
    ) -> impl Future<Output = ()>;

    /// Stops the ScyllaDB cluster.
    fn stop(&self) -> impl Future<Output = ()>;

    /// Waits for the ScyllaDB cluster to be ready.
    fn wait_for_ready(&self) -> impl Future<Output = bool>;

    /// Starts a paused cluster back again.
    fn up(&self, vs_uri: String, conf: Option<Vec<u8>>) -> impl Future<Output = ()>;

    /// Pauses a cluster.
    fn down(&self) -> impl Future<Output = ()>;

    /// Starts a single paused ScyllaDB instance back again.
    fn up_node(
        &self,
        vs_uri: String,
        db_ip: Ipv4Addr,
        conf: Option<Vec<u8>>,
    ) -> impl Future<Output = ()>;

    /// Pauses a single ScyllaDB instance.
    fn down_node(&self, db_ip: Ipv4Addr) -> impl Future<Output = ()>;

    /// Restarts a single ScyllaDB instance.
    fn restart(&self, vs_uri: String, db_ip: Ipv4Addr) -> impl Future<Output = ()>;

    /// Flushes all memtables to disk on all nodes.
    fn flush(&self) -> impl Future<Output = ()>;
}

impl ScyllaClusterExt for mpsc::Sender<ScyllaCluster> {
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Version { tx })
            .await
            .expect("ScyllaClusterExt::version: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::version: internal actor should send response")
    }

    async fn start(&self, vs_uri: String, db_ips: Vec<Ipv4Addr>, conf: Option<Vec<u8>>) {
        self.send(ScyllaCluster::Start {
            vs_uri,
            db_ips,
            conf,
        })
        .await
        .expect("ScyllaClusterExt::start: internal actor should receive request");
    }

    async fn stop(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Stop { tx })
            .await
            .expect("ScyllaClusterExt::stop: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::stop: internal actor should send response");
    }

    async fn wait_for_ready(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::WaitForReady { tx })
            .await
            .expect("ScyllaClusterExt::wait_for_ready: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::wait_for_ready: internal actor should send response")
    }

    async fn up(&self, vs_uri: String, conf: Option<Vec<u8>>) {
        self.send(ScyllaCluster::Up { vs_uri, conf })
            .await
            .expect("ScyllaClusterExt::up: internal actor should receive request")
    }

    async fn up_node(&self, vs_uri: String, db_ip: Ipv4Addr, conf: Option<Vec<u8>>) {
        self.send(ScyllaCluster::UpNode {
            vs_uri,
            db_ip,
            conf,
        })
        .await
        .expect("ScyllaClusterExt::up_node: internal actor should receive request")
    }

    async fn down(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Down { tx })
            .await
            .expect("ScyllaClusterExt::down: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::down: internal actor should send response");
    }

    async fn down_node(&self, db_ip: Ipv4Addr) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::DownNode { db_ip, tx })
            .await
            .expect("ScyllaClusterExt::down_node: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::down_node: internal actor should send response");
    }

    async fn restart(&self, vs_uri: String, db_ip: Ipv4Addr) {
        self.down_node(db_ip).await;
        self.up_node(vs_uri, db_ip, None).await;
        assert!(self.wait_for_ready().await);
    }

    async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Flush { tx })
            .await
            .expect("ScyllaClusterExt::flush: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::flush: internal actor should send response");
    }
}
