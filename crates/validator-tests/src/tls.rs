/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use rustls::ClientConfig;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub enum Tls {
    /// Returns the filesystem path to the certificate file.
    CertPath { tx: oneshot::Sender<PathBuf> },
    /// Returns the filesystem path to the key file.
    KeyPath { tx: oneshot::Sender<PathBuf> },
    /// Returns the pre-built TLS client configuration.
    ClientTlsConfig {
        tx: oneshot::Sender<Arc<ClientConfig>>,
    },
}

pub trait TlsExt {
    /// Returns the filesystem path to the certificate file.
    fn cert_path(&self) -> impl Future<Output = PathBuf>;

    /// Returns the filesystem path to the key file.
    fn key_path(&self) -> impl Future<Output = PathBuf>;

    /// Returns the pre-built TLS client configuration.
    fn client_tls_config(&self) -> impl Future<Output = Arc<ClientConfig>>;
}

impl TlsExt for mpsc::Sender<Tls> {
    #[framed]
    async fn cert_path(&self) -> PathBuf {
        let (tx, rx) = oneshot::channel();
        self.send(Tls::CertPath { tx })
            .await
            .expect("TlsExt::cert_path: internal actor should receive request");
        rx.await
            .expect("TlsExt::cert_path: internal actor should send response")
    }

    #[framed]
    async fn key_path(&self) -> PathBuf {
        let (tx, rx) = oneshot::channel();
        self.send(Tls::KeyPath { tx })
            .await
            .expect("TlsExt::key_path: internal actor should receive request");
        rx.await
            .expect("TlsExt::key_path: internal actor should send response")
    }

    #[framed]
    async fn client_tls_config(&self) -> Arc<ClientConfig> {
        let (tx, rx) = oneshot::channel();
        self.send(Tls::ClientTlsConfig { tx })
            .await
            .expect("TlsExt::client_tls_config: internal actor should receive request");
        rx.await
            .expect("TlsExt::client_tls_config: internal actor should send response")
    }
}
