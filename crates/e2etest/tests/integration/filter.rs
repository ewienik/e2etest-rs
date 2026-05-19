/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use e2etest::Config;
use e2etest::Fixture;
use e2etest::Setup;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

struct Counter(Arc<AtomicUsize>);

#[derive(Clone)]
struct FixtureCount(Arc<Counter>);

impl Fixture for FixtureCount {
    async fn setup(setup: &mut impl Setup) -> Self {
        let counter = setup.get::<Counter>().await.unwrap();
        Self(counter)
    }
    async fn teardown(self) {}
}

e2etest::group!(name = filter_root);
e2etest::group!(name = filter_group1, parent = filter_root);
e2etest::group!(name = filter_group1_1, parent = filter_group1);
e2etest::group!(name = filter_group1_2, parent = filter_group1);
e2etest::group!(name = filter_group2, parent = filter_root);
e2etest::group!(name = filter_group2_1, parent = filter_group2);
e2etest::group!(name = filter_group2_2, parent = filter_group2);

#[e2etest::test(group = filter_group1)]
async fn filter_test1_1(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group1)]
async fn filter_test1_2(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group1_1)]
async fn filter_test1_1_1(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group1_1)]
async fn filter_test1_1_2(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group1_2)]
async fn filter_test1_2_1(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group1_2)]
async fn filter_test1_2_2(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group2)]
async fn filter_test2_1(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group2)]
async fn filter_test2_2(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group2_1)]
async fn filter_test2_1_1(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group2_1)]
async fn filter_test2_1_2(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group2_2)]
async fn filter_test2_2_1(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = filter_group2_2)]
async fn filter_test2_2_2(fixture: Arc<FixtureCount>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[tokio::test]
async fn filter_by_group() {
    let counter = Arc::new(AtomicUsize::new(0));

    let stats = e2etest::run(
        Config::default()
            .with_permanent_fixture(Counter(Arc::clone(&counter)))
            .with_filter("group1::")
            .with_default_timeout(Duration::from_secs(1)),
        filter_root(),
    )
    .await;

    // 6 tests
    assert_eq!(counter.load(Ordering::Relaxed), 6);

    assert!(stats.is_success());
    assert_eq!(stats.total(), 12);
    assert_eq!(stats.filtered(), 6);
    assert_eq!(stats.launched(), 6);
    assert_eq!(stats.ok(), 6);
}

#[tokio::test]
async fn filter_by_test() {
    let counter = Arc::new(AtomicUsize::new(0));

    let stats = e2etest::run(
        Config::default()
            .with_permanent_fixture(Counter(Arc::clone(&counter)))
            .with_filter("::test2_2")
            .with_default_timeout(Duration::from_secs(1)),
        filter_root(),
    )
    .await;

    // 3 tests
    assert_eq!(counter.load(Ordering::Relaxed), 3);

    assert!(stats.is_success());
    assert_eq!(stats.total(), 12);
    assert_eq!(stats.filtered(), 3);
    assert_eq!(stats.launched(), 3);
    assert_eq!(stats.ok(), 3);
}
