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
//!     async fn setup(setup: &mut impl e2etest::Setup) -> Self {
//!         let cfg = setup.get::<FixtureCfg>().await.unwrap();
//!         Self { dns_ip: cfg.dns_ip }
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
//!     async fn setup(setup: &mut impl e2etest::Setup) -> Self {
//!         let one = setup.setup::<FixtureOne>().await;
//!         Self { octet: one.dns_ip.octets()[2] }
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
//!     async fn setup(setup: &mut impl e2etest::Setup) -> Self {
//!         let two = setup.setup::<FixtureTwo>().await;
//!         Self { number: two.octet as usize * 1024 }
//!     }
//!
//!     async fn teardown(self) { }
//! }
//!
//! e2etest::group!(name = root, fixtures = (FixtureOne));
//!
//! e2etest::group!(name = group, fixtures = (FixtureTwo), parent = root);
//!
//! #[e2etest::test(group = group, timeout = Duration::from_secs(5))]
//! async fn dns_ip_100(one: Arc<FixtureOne>, two: Arc<FixtureTwo>) {
//!     assert_eq!(one.dns_ip, Ipv4Addr::new(127, 0, 100, 1));
//!     assert_eq!(two.octet, 100);
//! }
//!
//! #[e2etest::test(group = group, skip = true)]
//! async fn dns_ip_200(one: Arc<FixtureOne>) {
//!     assert_eq!(one.dns_ip, Ipv4Addr::new(127, 0, 200, 1));
//! }
//!
//! #[e2etest::test(group = group)]
//! async fn number_and_octet(two: Arc<FixtureTwo>, three: Arc<FixtureThree>) {
//!     assert_eq!(two.octet, 100);
//!     assert_eq!(three.number, 100 * 1024);
//! }
//!
//! }
//!
//! tokio::runtime::Runtime::new().unwrap().block_on(async move {
//!     use std::net::Ipv4Addr;
//!     use std::time::Duration;
//!
//!     let config = e2etest::Config::default()
//!         .with_permanent_fixture(sample::FixtureCfg { dns_ip: Ipv4Addr::new(127, 0, 100, 1) })
//!         .with_default_timeout(Duration::from_secs(10));
//!     let stats = e2etest::run(config, sample::root()).await;
//!     assert!(stats.is_success());
//!     assert_eq!(stats.total(), 3);
//!     assert_eq!(stats.launched(), 2);
//!     assert_eq!(stats.ok(), 2);
//!     assert_eq!(stats.skipped(), 1);
//! });
//! ```

mod backtrace;
mod filter;
mod fixture;
mod group;
mod run;
mod statistics;
mod task;
mod test;

use crate::filter::Filter;
pub use crate::fixture::Fixture;
use crate::fixture::Fixtures;
pub use crate::fixture::Setup;
pub use crate::group::Group;
pub use crate::group::RunGroup;
pub use crate::statistics::Statistics;
pub use crate::test::RunTest;
pub use crate::test::Test;
use async_backtrace::framed;
pub use e2etest_macros::group;
pub use e2etest_macros::test;
use std::any::Any;
use std::panic;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Configuration for running tests.
pub struct Config {
    permanent_fixtures: Vec<Arc<dyn Any + Send + Sync>>,
    filters: Vec<String>,
    default_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            permanent_fixtures: Vec::new(),
            filters: Vec::new(),
            default_timeout: Duration::from_secs(60),
        }
    }
}

impl Config {
    /// Add a permanent fixture that will be available for all tests.
    pub fn with_permanent_fixture(mut self, fixture: impl Any + Send + Sync) -> Self {
        self.permanent_fixtures.push(Arc::new(fixture));
        self
    }

    /// Add a filter to select which tests to run.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }

    /// Set the default timeout for tests that don't specify one.
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }
}

/// Main entry point for running tests.
///
/// It takes command line arguments, an initialization function,
/// a test registration function, and a fixture creation function.
#[framed]
pub async fn run(config: Config, group: Box<dyn RunGroup>) -> Statistics {
    panic::set_hook(Box::new(|info| {
        error!("{info}");
    }));

    let fixtures = Fixtures::with_permanent(config.permanent_fixtures.into_iter());
    let filter = Filter::new(&config.filters, group.as_ref());

    run::run(fixtures, group, filter, config.default_timeout).await
}
