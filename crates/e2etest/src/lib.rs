/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! This library provides a framework for defining and running End-to-End tests on network service
//! for Rust. It allows users to define test cases with multiple tests, and provides a global
//! fixture for all of them.
//!
//! ## Usage
//!
//! See this simple example:
//!
//! ```rust
//! mod sample {
//!
//! use std::net::Ipv4Addr;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! #[derive(Clone, Copy)]
//! pub struct FixtureCfg {
//!     pub dns_ip: Ipv4Addr,
//! }
//!
//! #[derive(Clone, Copy)]
//! pub struct FixtureOne {
//!     dns_ip: Ipv4Addr,
//! }
//!
//! impl e2etest::Fixture for FixtureOne {
//!     async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
//!         let cfg = setup.get::<FixtureCfg>().await.unwrap();
//!         Some(Self { dns_ip: cfg.dns_ip })
//!     }
//!
//!     async fn teardown(self) { }
//! }
//!
//! #[derive(Clone, Copy)]
//! pub struct FixtureTwo {
//!     octet: u8,
//! }
//!
//! impl e2etest::Fixture for FixtureTwo {
//!     async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
//!         let one = setup.setup::<FixtureOne>().await?;
//!         Some(Self { octet: one.dns_ip.octets()[2] })
//!     }
//!
//!     async fn teardown(self) { }
//! }
//!
//! #[derive(Clone, Copy)]
//! pub struct FixtureThree {
//!     number: usize,
//! }
//!
//! impl e2etest::Fixture for FixtureThree {
//!     async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
//!         let two = setup.setup::<FixtureTwo>().await?;
//!         Some(Self { number: two.octet as usize * 1024 })
//!     }
//!
//!     async fn teardown(self) { }
//! }
//!
//! e2etest::group!(name = group, fixtures = (FixtureTwo));
//!
//! #[e2etest::test(group = group, timeout = Duration::from_secs(5))]
//! async fn dns_ip_100(one: Arc<FixtureOne>, two: Arc<FixtureTwo>) {
//!     assert_eq!(one.dns_ip, Ipv4Addr::new(127, 0, 100, 1));
//!     assert_eq!(two.octet, 100);
//! }
//!
//! #[e2etest::test(group = group)]
//! async fn dns_ip_200(one: Arc<FixtureOne>, _: Arc<e2etest::Skip>) {
//!     assert_eq!(one.dns_ip, Ipv4Addr::new(127, 0, 200, 1));
//! }
//!
//! #[e2etest::test()]
//! async fn number_and_octet(two: Arc<FixtureTwo>, three: Arc<FixtureThree>) {
//!     assert_eq!(two.octet, 100);
//!     assert_eq!(three.number, 100 * 1024);
//! }
//!
//! }
//!
//! use std::net::Ipv4Addr;
//! use std::time::Duration;
//!
//! tracing_subscriber::fmt::init();
//!
//! if let Some(unshare_info) = e2etest::unshare_info() {
//!     let config = e2etest::ConfigUnshare::new(unshare_info)
//!             .with_permanent_fixture(sample::FixtureCfg { dns_ip: Ipv4Addr::new(127, 0, 100, 1) })
//!             .with_default_timeout(Duration::from_secs(10))
//!             .with_concurrency(10);
//!     e2etest::run_in_unshare(config);
//!     return;
//! }
//!
//! let config = e2etest::Config::default()
//!     .with_concurrency(10);
//!
//! let stats = e2etest::run(config);
//!
//! assert!(stats.is_success());
//! assert_eq!(stats.tests_defined(), 3);
//! assert_eq!(stats.tests_included(), 3);
//! assert_eq!(stats.tests_launched(), 2);
//! assert_eq!(stats.tests_passed(), 2);
//! assert_eq!(stats.tests_skipped(), 1);
//! ```

mod backtrace;
mod filter;
mod fixture;
mod group;
mod run;
mod statistics;
mod task;
mod test;
mod unshare;

use crate::filter::Filter;
pub use crate::fixture::Fixture;
use crate::fixture::Fixtures;
pub use crate::fixture::Setup;
pub use crate::fixture::Skip;
pub use crate::group::Group;
pub use crate::group::RunGroup;
pub use crate::statistics::Statistics;
pub use crate::test::RunTest;
pub use crate::test::Test;
pub use crate::unshare::UnshareInfo;
pub use crate::unshare::unshare_info;
#[doc(hidden)]
pub use async_backtrace as __async_backtrace;
use async_backtrace::framed;
pub use e2etest_macros::group;
pub use e2etest_macros::test;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::IpcSender;
#[doc(hidden)]
pub use linkme as __linkme;
use std::any::Any;
use std::collections::BTreeSet;
use std::panic;
use std::process::Child;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::LazyLock;
use std::thread;
use std::time::Duration;
use tokio::runtime::Builder;
use tracing::error;
use tracing::info;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

#[linkme::distributed_slice]
pub static E2ETEST_TESTS: [fn() -> Box<dyn RunTest>];

#[linkme::distributed_slice]
pub static E2ETEST_GROUPS: [fn() -> Box<dyn RunGroup>];

/// Configuration for running tests.
pub struct Config {
    filters: Vec<String>,
    concurrency: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            filters: Vec::new(),
            concurrency: 1,
        }
    }
}

impl Config {
    /// Add a filter to select which tests to run.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }

    /// Set the maximum number of namespaces to run concurrently.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

/// Configuration for running tests.
pub struct ConfigUnshare {
    permanent_fixtures: Vec<Arc<dyn Any + Send + Sync>>,
    info: UnshareInfo,
    default_timeout: Duration,
    concurrency: usize,
}

impl ConfigUnshare {
    /// Create a new `ConfigUnshare` with the given `UnshareInfo`.
    pub fn new(info: UnshareInfo) -> Self {
        Self {
            permanent_fixtures: Vec::new(),
            info,
            default_timeout: DEFAULT_TIMEOUT,
            concurrency: 1,
        }
    }

    /// Add a permanent fixture that will be available for all tests.
    pub fn with_permanent_fixture(mut self, fixture: impl Any + Send + Sync) -> Self {
        self.permanent_fixtures.push(Arc::new(fixture));
        self
    }

    /// Set the default timeout for tests that don't specify one.
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// Set the maximum number of tests to run concurrently.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Bad exit code")]
    BadExitCode,
    #[error("Bad log directory: {0}")]
    BadLogDir(String, #[source] Option<std::io::Error>),
    #[error("Expected to be run in unshare, but was not")]
    NotInUnshare,
    #[error("Expected to be run outside of unshare, but was in unshare")]
    InUnshare,
}

pub enum ExitUnshare {
    Passed = 0,
    Skipped = 1,
    Failed = 2,
}

impl From<ExitUnshare> for ExitCode {
    fn from(exit: ExitUnshare) -> Self {
        ExitCode::from(exit as u8)
    }
}

impl TryFrom<ExitCode> for ExitUnshare {
    type Error = Error;
    fn try_from(exit_code: ExitCode) -> Result<Self, Self::Error> {
        match exit_code {
            code if code == ExitUnshare::Passed.into() => Ok(ExitUnshare::Passed),
            code if code == ExitUnshare::Skipped.into() => Ok(ExitUnshare::Skipped),
            code if code == ExitUnshare::Failed.into() => Ok(ExitUnshare::Failed),
            _ => Err(Error::BadExitCode),
        }
    }
}

struct Root;

impl Group for Root {
    type Fixture = ();

    fn name(&self) -> &str {
        Self::NAME
    }

    fn tests(&self) -> &[Box<dyn RunTest>] {
        static TESTS: LazyLock<Vec<Box<dyn RunTest>>> =
            LazyLock::new(|| E2ETEST_TESTS.iter().map(|test_fn| test_fn()).collect());
        TESTS.as_slice()
    }
}

trait RootGroup: Group {
    fn groups(&self) -> &[Box<dyn RunGroup>];

    /// Returns an iterator over the (group_name, test_name) of all tests in this group and its subgroups.
    fn group_test_names(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tests()
            .iter()
            .map(|test| ("", test.name()))
            .chain(self.groups().iter().flat_map(|subgroup| {
                subgroup
                    .test_names()
                    .map(|test_name| (subgroup.name(), test_name))
            }))
    }
}

impl RootGroup for Root {
    fn groups(&self) -> &[Box<dyn RunGroup>] {
        static GROUPS: LazyLock<Vec<Box<dyn RunGroup>>> =
            LazyLock::new(|| E2ETEST_GROUPS.iter().map(|group_fn| group_fn()).collect());
        GROUPS.as_slice()
    }
}

impl Root {
    const NAME: &'static str = "";

    fn groups_tests_for_namespaces(&self, filter: &Filter) -> Vec<String> {
        self.group_test_names()
            .filter(|(group_name, test_name)| filter.consider_test(group_name, test_name))
            .map(|(group_name, test_name)| {
                if group_name == Self::NAME {
                    test_name
                } else {
                    group_name
                }
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(String::from)
            .collect()
    }
}

/// Main entry point for running tests.
///
/// It takes `Config` argument. Returns `Statistics` about the test run.
#[framed]
pub fn run(config: Config) -> Statistics {
    panic::set_hook(Box::new(|info| {
        error!("{info}");
    }));

    if unshare_info().is_some() {
        panic!("run() should be called outside of unshare, but was called inside unshare");
    }

    let root = Root;
    let filter = Filter::new(&config.filters, &root);
    let groups_tests = root.groups_tests_for_namespaces(&filter);

    let mut in_progress: Vec<(Child, IpcOneShotServer<Statistics>)> = vec![];
    let mut final_stats = Statistics::new();

    let mut handle_done_work = |in_progress: &mut Vec<(Child, IpcOneShotServer<Statistics>)>| {
        in_progress
            .extract_if(.., |(child, _)| child.try_wait().unwrap().is_some())
            .map(|(_, ipc_server)| {
                let (_, stats) = ipc_server.accept().unwrap();
                stats
            })
            .for_each(|stats| {
                final_stats += stats;
            });
    };

    const SLEEP_DURATION: Duration = Duration::from_millis(100);

    groups_tests.into_iter().for_each(|group_name| {
        loop {
            handle_done_work(&mut in_progress);
            if in_progress.len() < config.concurrency {
                break;
            }
            thread::sleep(SLEEP_DURATION);
        }
        in_progress.push(unshare::spawn(group_name));
    });

    while !in_progress.is_empty() {
        handle_done_work(&mut in_progress);
        thread::sleep(SLEEP_DURATION);
    }

    if final_stats.is_success() {
        info!("test run ok: {final_stats:?}");
    } else {
        error!("test run failed: {final_stats:?}");
    }
    final_stats
}

/// Main entry point for running single test.
///
/// It takes `Config` argument and a root group. Returns `Statistics` about the test run.
#[framed]
pub fn run_in_unshare(config: ConfigUnshare) {
    panic::set_hook(Box::new(|info| {
        error!("{info}");
    }));

    unshare::init_namespaces();

    let root = Root;
    let filter = Filter::new(&[config.info.filter], &root);

    let fixtures = Fixtures::with_permanent(config.permanent_fixtures.into_iter());

    let stats = Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run::run(
            fixtures,
            &root,
            filter,
            config.default_timeout,
            config.concurrency,
        ));

    let tx = IpcSender::connect(config.info.ipc_channel).unwrap();
    tx.send(stats.clone()).unwrap();
}

/// Returns a list of all group names defined in the test suite.
pub fn group_names() -> Vec<String> {
    Root.group_test_names()
        .filter(|&(group_name, _)| group_name != Group::name(&Root))
        .map(|(group_name, _)| group_name.into())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Returns a list of all test names defined in the test suite.
pub fn test_names() -> Vec<String> {
    Root.group_test_names()
        .map(|(_, test_name)| test_name.into())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadMe;
