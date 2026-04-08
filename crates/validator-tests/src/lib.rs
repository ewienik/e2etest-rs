/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

pub mod common;
mod dns;
mod firewall;
mod scylla_cluster;
mod scylla_proxy_cluster;
mod tls;
mod vector_store_cluster;

use async_backtrace::frame;
use async_backtrace::framed;
pub use dns::Dns;
pub use dns::DnsExt;
pub use firewall::Firewall;
pub use firewall::FirewallExt;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::StreamExt;
pub use scylla_cluster::ScyllaCluster;
pub use scylla_cluster::ScyllaClusterExt;
pub use scylla_cluster::ScyllaNodeConfig;
pub use scylla_cluster::default_scylla_args;
pub use scylla_cluster::set_default_scylla_args;
pub use scylla_proxy_cluster::ScyllaProxyCluster;
pub use scylla_proxy_cluster::ScyllaProxyClusterExt;
pub use scylla_proxy_cluster::ScyllaProxyNodeConfig;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future;
use std::net::Ipv4Addr;
use std::panic;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
pub use tls::Tls;
pub use tls::TlsExt;
use tokio::sync::mpsc;
use tokio::time;
use tracing::Instrument;
use tracing::Span;
use tracing::error;
use tracing::error_span;
use tracing::info;
pub use vector_store_cluster::VectorStoreCluster;
pub use vector_store_cluster::VectorStoreClusterExt;
pub use vector_store_cluster::VectorStoreNodeConfig;

/// Represents a subnet for services, derived from a base IP address.
pub struct ServicesSubnet([u8; 3]);

impl ServicesSubnet {
    pub fn new(ip: Ipv4Addr) -> Self {
        assert!(
            ip.is_loopback(),
            "Base IP for services must be a loopback address"
        );

        let octets = ip.octets();
        assert!(
            octets[3] == 1,
            "Base IP for services must have the last octet set to 1"
        );

        Self([octets[0], octets[1], octets[2]])
    }

    /// Returns an IP address in the subnet with the specified last octet.
    pub fn ip(&self, octet: u8) -> Ipv4Addr {
        [self.0[0], self.0[1], self.0[2], octet].into()
    }
}

#[derive(Clone)]
pub struct TestActors {
    pub services_subnet: Arc<ServicesSubnet>,
    pub tls: mpsc::Sender<Tls>,
    pub dns: mpsc::Sender<Dns>,
    pub firewall: mpsc::Sender<Firewall>,
    pub db: mpsc::Sender<ScyllaCluster>,
    pub vs: mpsc::Sender<VectorStoreCluster>,
    pub db_proxy: mpsc::Sender<ScyllaProxyCluster>,
}

type TestFuture = BoxFuture<'static, ()>;

type TestFn = Box<dyn Fn(TestActors) -> TestFuture>;

#[derive(Debug, Default)]
pub struct RunReport {
    failed_tests: Vec<String>,
}

impl RunReport {
    pub fn failed_tests(&self) -> &[String] {
        &self.failed_tests
    }

    pub fn failed_tests_summary(&self) -> Option<String> {
        format_failed_tests(&self.failed_tests)
    }

    pub fn is_success(&self) -> bool {
        self.failed_tests.is_empty()
    }
}

impl From<Statistics> for RunReport {
    fn from(stats: Statistics) -> Self {
        Self {
            failed_tests: stats.failed_tests,
        }
    }
}

pub fn format_failed_tests(failed_tests: &[String]) -> Option<String> {
    if failed_tests.is_empty() {
        return None;
    }

    let failed_tests = failed_tests
        .iter()
        .map(|failed_test| format!("- {failed_test}"))
        .collect::<Vec<_>>()
        .join("\n");
    Some(format!("Failed tests:\n{failed_tests}"))
}

#[derive(Debug)]
/// Statistics for a test run, including total tests, launched, successful, and failed.
pub(crate) struct Statistics {
    total: usize,
    launched: usize,
    ok: usize,
    failed: usize,
    failed_tests: Vec<String>,
}

impl Statistics {
    fn new(total: usize) -> Self {
        Self {
            total,
            launched: 0,
            ok: 0,
            failed: 0,
            failed_tests: vec![],
        }
    }

    fn append(&mut self, other: &Self) {
        self.total += other.total;
        self.launched += other.launched;
        self.ok += other.ok;
        self.failed += other.failed;
        self.failed_tests.extend(other.failed_tests.iter().cloned());
    }

    fn record_failure(&mut self, failed_test: impl Into<String>) {
        self.failed += 1;
        self.failed_tests.push(failed_test.into());
    }
}

/// Represents a single test case, which can include initialization, multiple tests, and cleanup.
pub struct TestCase {
    init: Option<(Duration, TestFn)>,
    tests: Vec<(String, Duration, TestFn)>,
    cleanup: Option<(Duration, TestFn)>,
}

impl TestCase {
    /// Creates a new empty test case.
    pub fn empty() -> Self {
        Self {
            init: None,
            tests: vec![],
            cleanup: None,
        }
    }

    /// Returns a reference to the tests in this test case.
    pub fn tests(&self) -> &Vec<(String, Duration, TestFn)> {
        &self.tests
    }

    /// Add an initialization function to the test case.
    pub fn with_init<F, R>(mut self, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.init = Some((timeout, wrap_test_fn(test_fn)));
        self
    }

    /// Add a test to the test case.
    pub fn with_test<F, R>(mut self, name: impl ToString, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.tests
            .push((name.to_string(), timeout, wrap_test_fn(test_fn)));
        self
    }

    /// Add a cleanup function to the test case.
    pub fn with_cleanup<F, R>(mut self, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.cleanup = Some((timeout, wrap_test_fn(test_fn)));
        self
    }

    #[framed]
    /// Run initialization, all tests, and cleanup functions in the test case.
    async fn run(
        &self,
        actors: TestActors,
        test_case_name: &str,
        test_cases: &HashSet<String>,
        backtrace: Backtrace,
    ) -> Statistics {
        let total = if test_cases.is_empty() {
            // Run all tests
            self.tests.len()
        } else {
            test_cases.len()
        } + (self.init.is_some() as usize)
            + (self.cleanup.is_some() as usize);
        let mut stats = Statistics::new(total);

        if let Some((timeout, init)) = &self.init {
            stats.launched += 1;
            if !run_single(
                error_span!("init"),
                *timeout,
                init(actors.clone()),
                backtrace.clone(),
            )
            .await
            {
                stats.record_failure(format!("{test_case_name}::init"));
                return stats;
            }
            stats.ok += 1;
        }

        stream::iter(self.tests.iter())
            .filter(|(name, _, _)| {
                future::ready(test_cases.is_empty() || test_cases.contains(name))
            })
            .then(|(name, timeout, test)| {
                let actors = actors.clone();
                let backtrace = backtrace.clone();
                async move {
                    let ok =
                        run_single(error_span!("test", name), *timeout, test(actors), backtrace)
                            .await;
                    (name, ok)
                }
            })
            .for_each(|(name, ok)| {
                stats.launched += 1;
                if ok {
                    stats.ok += 1;
                } else {
                    stats.record_failure(format!("{test_case_name}::{name}"));
                }
                future::ready(())
            })
            .await;

        if let Some((timeout, cleanup)) = &self.cleanup {
            stats.launched += 1;
            if !run_single(
                error_span!("cleanup"),
                *timeout,
                cleanup(actors.clone()),
                backtrace.clone(),
            )
            .await
            {
                stats.record_failure(format!("{test_case_name}::cleanup"));
            } else {
                stats.ok += 1;
            }
        }

        stats
    }
}

/// Wraps a test function into a `TestFn` type, which is a boxed future that can be stored in a
/// container.
fn wrap_test_fn<F, R>(test_fn: F) -> TestFn
where
    F: Fn(TestActors) -> R + 'static,
    R: Future<Output = ()> + Send + 'static,
{
    Box::new(move |actors: TestActors| {
        let future = test_fn(actors);
        future.boxed()
    })
}

#[framed]
/// Runs a single test with a timeout, logging the result in the provided span.
async fn run_single(
    span: Span,
    timeout: Duration,
    future: TestFuture,
    backtrace: Backtrace,
) -> bool {
    let task = tokio::spawn(frame!(
        async move {
            time::timeout(timeout, future)
                .await
                .expect("test timed out");
        }
        .instrument(span.clone())
    ));
    if let Err(err) = task.await {
        let backtrace = backtrace.get();
        error!(parent: &span, "test failed: {err}\n{backtrace}");
        false
    } else {
        info!(parent: &span, "test ok");
        true
    }
}

#[framed]
/// Runs all test cases, filtering them based on the provided filter map.
pub async fn run(
    actors: TestActors,
    test_cases: Vec<(String, TestCase)>,
    filter_map: Arc<HashMap<String, HashSet<String>>>,
) -> RunReport {
    let backtrace = setup_panic_hook();

    let stats = stream::iter(test_cases.into_iter())
        .filter(|(file_name, _)| {
            let process = filter_map.is_empty() || filter_map.contains_key(file_name);
            async move { process }
        })
        .then(|(name, test_case)| {
            let actors = actors.clone();
            let filter = filter_map.clone();
            let file_name = name.clone();
            let backtrace = backtrace.clone();
            async move {
                let stats = test_case
                    .run(
                        actors,
                        &file_name,
                        filter.get(&file_name).unwrap_or(&HashSet::new()),
                        backtrace,
                    )
                    .instrument(error_span!("test-case", name))
                    .await;
                if stats.failed > 0 {
                    error!("test case failed: {stats:?}");
                } else {
                    info!("test case ok: {stats:?}");
                }
                stats
            }
        })
        .fold(Statistics::new(0), |mut acc, stats| async move {
            acc.append(&stats);
            acc
        })
        .await;

    clear_panic_hook();

    if stats.failed > 0 {
        error!("test run failed: {stats:?}");
        return stats.into();
    }
    info!("test run ok: {stats:?}");
    stats.into()
}

#[derive(Clone, Debug)]
struct Backtrace(Arc<RwLock<String>>);

impl Backtrace {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(String::new())))
    }

    fn get(&self) -> String {
        self.0.read().unwrap().clone()
    }
}

fn setup_panic_hook() -> Backtrace {
    let backtrace = Backtrace::new();
    let old_hook = panic::take_hook();
    panic::set_hook(Box::new({
        let backtrace = backtrace.clone();
        move |info| {
            *backtrace.0.write().unwrap() = async_backtrace::taskdump_tree(true);
            old_hook(info);
        }
    }));
    backtrace
}

fn clear_panic_hook() {
    _ = panic::take_hook();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::net::Ipv4Addr;
    use tokio::sync::mpsc;

    fn dummy_test_actors() -> TestActors {
        let services_subnet = Arc::new(ServicesSubnet::new(Ipv4Addr::new(127, 0, 2, 1)));
        let (tls, _) = mpsc::channel(1);
        let (dns, _) = mpsc::channel(1);
        let (firewall, _) = mpsc::channel(1);
        let (db, _) = mpsc::channel(1);
        let (vs, _) = mpsc::channel(1);
        let (db_proxy, _) = mpsc::channel(1);

        TestActors {
            services_subnet,
            tls,
            dns,
            firewall,
            db,
            vs,
            db_proxy,
        }
    }

    #[test]
    fn formats_failed_tests_as_bulleted_list() {
        let failed_tests = vec!["crud::boom".to_string(), "crud::cleanup".to_string()];

        assert_eq!(
            format_failed_tests(&failed_tests).as_deref(),
            Some("Failed tests:\n- crud::boom\n- crud::cleanup")
        );
    }

    #[tokio::test]
    async fn collects_failed_test_names() {
        let timeout = Duration::from_secs(1);
        let test_case = TestCase::empty()
            .with_test("ok", timeout, |_actors| async {})
            .with_test("boom", timeout, |_actors| async {
                panic!("boom");
            })
            .with_cleanup(timeout, |_actors| async {
                panic!("cleanup");
            });

        let stats = test_case
            .run(
                dummy_test_actors(),
                "crud",
                &HashSet::new(),
                Backtrace::new(),
            )
            .await;

        assert_eq!(stats.failed, 2);
        assert_eq!(
            stats.failed_tests,
            &["crud::boom".to_string(), "crud::cleanup".to_string()]
        );
    }
}
