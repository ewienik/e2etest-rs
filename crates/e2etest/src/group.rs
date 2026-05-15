/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use crate::fixture::Fixture;
use crate::run::RunContext;
use crate::task;
use crate::test::RunTest;
use async_backtrace::framed;
use futures::future::BoxFuture;
use tracing::Instrument;
use tracing::error_span;

/// A group of tests.
///
/// Groups can contain other groups and tests. Each group has a fixture that is
/// setup before any tests are run and torn down after all tests are run. Groups are useful
/// for grouping related tests together and sharing setup and teardown logic.
pub trait Group {
    /// The fixture type for this group.
    type Fixture: Fixture;

    /// The name of the group.
    fn name(&self) -> &str;

    /// The tests in this group.
    fn tests(&self) -> &[Box<dyn RunTest>] {
        &[]
    }

    /// The subgroups in this group.
    fn groups(&self) -> &[Box<dyn RunGroup>] {
        &[]
    }
}

/// A supporting trait to collecting Group trait objects and running them.
///
/// This is used to run tests or groups recursively and collect statistics.
pub trait RunGroup: Send + Sync + 'static {
    /// The name of the group.
    fn name(&self) -> &str;

    /// The names of all tests in this group and its subgroups.
    fn test_names(&self) -> Vec<String>;

    /// Run the group and return statistics about the run.
    fn run_group(&self, parent_names: Vec<String>, ctx: RunContext) -> BoxFuture<'_, ()>;
}

impl<F, G> RunGroup for G
where
    F: Fixture,
    G: Group<Fixture = F>,
    G: Send + Sync + 'static,
{
    fn name(&self) -> &str {
        self.name()
    }

    fn test_names(&self) -> Vec<String> {
        self.groups()
            .iter()
            .flat_map(|group| {
                group
                    .test_names()
                    .into_iter()
                    .map(|test| format!("{group}::{test}", group = group.name()))
            })
            .chain(self.tests().iter().map(|test| test.name().into()))
            .collect()
    }

    #[framed]
    fn run_group(&self, mut parent_names: Vec<String>, ctx: RunContext) -> BoxFuture<'_, ()> {
        Box::pin(
            async move {
                parent_names.push(self.name().to_string());

                // Setup the fixture. If it fails, we skip the tests
                let Ok(fixture) = task::setup(
                    ctx.fixtures.setup::<F>(),
                    F::timeout_setup().unwrap_or(ctx.default_timeout),
                    ctx.clone(),
                )
                .await
                else {
                    ctx.statistics.record_failure(parent_names.join("::"));
                    return;
                };

                // Run groups
                for group in self.groups().iter().filter(|group| {
                    ctx.filter
                        .consider_group(parent_names.iter().map(|v| v.as_str()), group.name())
                }) {
                    group.run_group(parent_names.clone(), ctx.clone()).await;
                }

                // Run tests
                for test in self
                    .tests()
                    .iter()
                    .filter(|test| ctx.filter.consider_test(&parent_names, test.name()))
                {
                    _ = test.run_test(self.name(), ctx.clone()).await;
                }

                // Teardown group fixture
                drop(fixture);
                if task::teardown(
                    ctx.fixtures.teardown(),
                    F::timeout_teardown().unwrap_or(ctx.default_timeout),
                    ctx.clone(),
                )
                .await
                .is_err()
                {
                    ctx.statistics.record_failure(parent_names.join("::"));
                }
            }
            .instrument(error_span!("group", "{}", self.name())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Filter;
    use crate::fixture::Setup;
    use crate::test::Test;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone)]
    struct GroupFixture;

    impl From<&()> for GroupFixture {
        fn from(_: &()) -> Self {
            Self
        }
    }

    impl Fixture for GroupFixture {
        async fn setup(_: &mut impl Setup) -> Self {
            Self
        }
        async fn teardown(self) {
            panic!("cleanup")
        }
    }

    #[derive(Clone)]
    struct TestFixture;

    impl From<&GroupFixture> for TestFixture {
        fn from(_: &GroupFixture) -> Self {
            Self
        }
    }

    impl Fixture for TestFixture {
        async fn setup(_: &mut impl Setup) -> Self {
            Self
        }
        async fn teardown(self) {}
    }

    struct TestImpl(String);

    impl Test for TestImpl {
        type Fixture = TestFixture;

        fn name(&self) -> &str {
            &self.0
        }

        fn run(&self, _: Arc<Self::Fixture>) -> impl Future<Output = ()> + Send + 'static {
            let name = self.0.clone();
            async move {
                if name == "boom" {
                    panic!("boom");
                }
            }
        }
    }

    struct GroupImpl {
        name: String,
        tests: Vec<Box<dyn RunTest>>,
    }

    impl Group for GroupImpl {
        type Fixture = GroupFixture;

        fn name(&self) -> &str {
            &self.name
        }

        fn tests(&self) -> &[Box<dyn RunTest>] {
            &self.tests
        }
    }

    #[tokio::test]
    async fn collects_failed_test_names() {
        let group = Box::new(GroupImpl {
            name: "crud".to_string(),
            tests: vec![
                Box::new(TestImpl("ok".to_string())),
                Box::new(TestImpl("boom".to_string())),
            ],
        }) as Box<dyn RunGroup>;
        let ctx = RunContext::new()
            .with_filter(Filter::new(&[], group.as_ref()))
            .with_default_timeout(Duration::from_secs(1));
        group.run_group(vec![], ctx.clone()).await;

        assert!(!ctx.statistics.is_success());
        assert_eq!(ctx.statistics.failed(), 2);
        assert_eq!(
            ctx.statistics.failed_tests(),
            &["crud::boom".to_string(), "crud".to_string()]
        );
    }
}
