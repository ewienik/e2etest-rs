/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use e2etest::Config;
use e2etest::Fixture;
use e2etest::Setup;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

struct Counter(Arc<AtomicUsize>);

#[derive(Clone)]
struct FixtureRoot(Arc<Counter>);

impl Fixture for FixtureRoot {
    async fn setup(setup: &mut impl Setup) -> Self {
        let counter = setup.get::<Counter>().await.unwrap();
        counter.0.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
    async fn teardown(self) {
        self.0.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct FixtureGroup(Arc<Counter>);

impl Fixture for FixtureGroup {
    async fn setup(setup: &mut impl Setup) -> Self {
        let counter = setup.get::<Counter>().await.unwrap();
        counter.0.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
    async fn teardown(self) {
        self.0.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct FixtureTest(Arc<Counter>);

impl Fixture for FixtureTest {
    async fn setup(setup: &mut impl Setup) -> Self {
        let counter = setup.get::<Counter>().await.unwrap();
        counter.0.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
    async fn teardown(self) {
        self.0.0.fetch_add(1, Ordering::Relaxed);
    }
}

e2etest::group!(name = hierarchy_root1, fixtures = (FixtureRoot));

e2etest::group!(
    name = hierarchy_group,
    fixtures = (FixtureGroup),
    parent = hierarchy_root1
);

#[e2etest::test(group = hierarchy_group)]
async fn first1(fixture: Arc<FixtureTest>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[tokio::test]
async fn run_root1() {
    let counter = Arc::new(AtomicUsize::new(0));

    let stats = e2etest::run(
        Config::default()
            .with_permanent_fixture(Counter(Arc::clone(&counter)))
            .with_default_timeout(Duration::from_secs(1)),
        hierarchy_root1(),
    )
    .await;

    // 1 tests * 3 + 2 groups * 2 = 7
    assert_eq!(counter.load(Ordering::Relaxed), 7);

    assert!(stats.is_success());
    assert_eq!(stats.total(), 1);
    assert_eq!(stats.launched(), 1);
    assert_eq!(stats.ok(), 1);
    assert_eq!(stats.failed(), 0);
}

mod root {
    use super::*;

    e2etest::group!(name = hierarchy_root2, fixtures = (FixtureRoot));
}

mod first {
    use super::*;

    e2etest::group!(
        name = hierarchy_first,
        fixtures = (FixtureGroup),
        parent = super::root::hierarchy_root2
    );

    #[e2etest::test(group = hierarchy_first)]
    async fn first1(fixture: Arc<FixtureTest>, _deep: Arc<deep::FixtureDeep>) {
        fixture.0.0.fetch_add(1, Ordering::Relaxed);
    }

    #[e2etest::test(group = hierarchy_first)]
    async fn first2(fixture: Arc<FixtureTest>, _deep: Arc<deep::FixtureDeep>) {
        fixture.0.0.fetch_add(1, Ordering::Relaxed);
    }

    mod deep {
        use super::*;

        #[derive(Clone)]
        pub(crate) struct FixtureDeep(Arc<Counter>);

        impl Fixture for FixtureDeep {
            async fn setup(setup: &mut impl Setup) -> Self {
                let counter = setup.get::<Counter>().await.unwrap();
                counter.0.fetch_add(1, Ordering::Relaxed);
                Self(counter)
            }
            async fn teardown(self) {
                self.0.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        e2etest::group!(
            name = hierarchy_deep,
            fixtures = (FixtureGroup),
            parent = super::hierarchy_first
        );

        #[e2etest::test(group = hierarchy_deep)]
        async fn deep1(fixture: Arc<FixtureTest>, _deep: Arc<FixtureDeep>) {
            fixture.0.0.fetch_add(1, Ordering::Relaxed);
        }

        #[e2etest::test(group = hierarchy_deep)]
        async fn deep2(fixture: Arc<FixtureTest>, _deep: Arc<FixtureDeep>) {
            fixture.0.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

mod second {
    use super::*;

    e2etest::group!(
        name = hierarchy_second,
        fixtures = (FixtureGroup),
        parent = super::root::hierarchy_root2
    );

    #[e2etest::test(group = hierarchy_second)]
    async fn second1(fixture: Arc<FixtureTest>) {
        fixture.0.0.fetch_add(1, Ordering::Relaxed);
    }

    #[e2etest::test(group = hierarchy_second)]
    async fn second2(fixture: Arc<FixtureTest>) {
        fixture.0.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn names_hierarchy_root2() {
    let received: HashSet<_> = root::hierarchy_root2().test_names().into_iter().collect();
    let expected: HashSet<_> = [
        "hierarchy_first::first1".to_string(),
        "hierarchy_first::first2".to_string(),
        "hierarchy_first::hierarchy_deep::deep1".to_string(),
        "hierarchy_first::hierarchy_deep::deep2".to_string(),
        "hierarchy_second::second1".to_string(),
        "hierarchy_second::second2".to_string(),
    ]
    .into_iter()
    .collect();
    assert_eq!(received, expected);
}

#[tokio::test]
async fn run_hierarchy_root2() {
    let counter = Arc::new(AtomicUsize::new(0));

    let stats = e2etest::run(
        Config::default()
            .with_permanent_fixture(Counter(Arc::clone(&counter)))
            .with_default_timeout(Duration::from_secs(1)),
        root::hierarchy_root2(),
    )
    .await;

    // 6 tests * 3 + 7 groups * 2
    assert_eq!(counter.load(Ordering::Relaxed), 32);

    assert!(stats.is_success());
    assert_eq!(stats.total(), 6);
    assert_eq!(stats.launched(), 6);
    assert_eq!(stats.ok(), 6);
    assert_eq!(stats.failed(), 0);
}
