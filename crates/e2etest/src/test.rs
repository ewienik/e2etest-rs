/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use crate::fixture::Fixture;
use crate::run::RunContext;
use crate::task;
use async_backtrace::framed;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;
use tracing::error_span;

/// A test that can be run by the test runner.
pub trait Test: Send + Sync + 'static {
    /// The fixture type that this test uses.
    type Fixture: Fixture;

    /// The timeout for the test. If `None`, the default timeout will be used.
    fn timeout(&self) -> Option<Duration> {
        None
    }

    /// Whether to skip this test. If `true`, the test will be skipped and not run.
    fn skip(&self) -> bool {
        false
    }

    /// The name of the test.
    fn name(&self) -> &str;

    /// Run the test with the given fixture.
    fn run(&self, fixture: Arc<Self::Fixture>) -> impl Future<Output = ()> + Send + 'static;
}

/// A supporting trait to collecting Test trait objects and running them.
///
/// This is used to run test with its fixture and collect statistics.
pub trait RunTest: Send + Sync + 'static {
    /// The name of the test.
    fn name(&self) -> &str;

    /// Run the test with the given fixture and collect statistics.
    fn run_test(&self, group_name: &str, ctx: RunContext) -> BoxFuture<'_, ()>;
}

impl<F, T> RunTest for T
where
    F: Fixture,
    T: Test<Fixture = F>,
    T: Send + Sync + 'static,
{
    fn name(&self) -> &str {
        self.name()
    }

    #[framed]
    fn run_test(&self, group_name: &str, ctx: RunContext) -> BoxFuture<'_, ()> {
        let name = format!("{group_name}::{name}", name = self.name());
        Box::pin(
            async move {
                if self.skip() {
                    ctx.statistics.increment_skipped();
                    return;
                }

                ctx.statistics.increment_launched();

                // Setup the fixture. If it fails, we skip the test and teardown.
                let Ok(fixture) = task::setup(
                    ctx.fixtures.setup::<F>(),
                    F::timeout_setup().unwrap_or(ctx.default_timeout),
                    ctx.clone(),
                )
                .await
                else {
                    ctx.statistics.record_failure(name);
                    return;
                };

                let test_result = task::test(
                    self.run(fixture.clone()),
                    self.timeout().unwrap_or(ctx.default_timeout),
                    ctx.clone(),
                )
                .await;

                // Run the teardown
                drop(fixture);
                let teardown_result = task::teardown(
                    ctx.fixtures.teardown(),
                    F::timeout_teardown().unwrap_or(ctx.default_timeout),
                    ctx.clone(),
                )
                .await;

                match (test_result, teardown_result) {
                    (Ok(_), Ok(_)) => ctx.statistics.increment_ok(),
                    _ => ctx.statistics.record_failure(name),
                }
            }
            .instrument(error_span!("test", "{}", self.name())),
        )
    }
}
