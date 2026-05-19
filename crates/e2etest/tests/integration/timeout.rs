/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use e2etest::Config;
use e2etest::Setup;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time;

struct Counter(Arc<AtomicUsize>);

#[derive(Clone)]
struct Fixture(Arc<Counter>);

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl Setup) -> Self {
        let counter = setup.get::<Counter>().await.unwrap();
        Self(counter)
    }
    async fn teardown(self) {}
}

e2etest::group!(name = timeout_root, fixtures = ());

#[e2etest::test(group = timeout_root)]
async fn first(fixture: Arc<Fixture>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = timeout_root, timeout = Duration::from_millis(1000))]
async fn second(fixture: Arc<Fixture>) {
    time::sleep(Duration::from_millis(100)).await;
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = timeout_root, timeout = Duration::from_millis(10))]
async fn timeouted(fixture: Arc<Fixture>) {
    time::sleep(Duration::from_millis(100)).await;
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[tokio::test]
async fn timeout() {
    let counter = Arc::new(AtomicUsize::new(0));

    let stats = e2etest::run(
        Config::default()
            .with_permanent_fixture(Counter(Arc::clone(&counter)))
            .with_default_timeout(Duration::from_secs(10)),
        timeout_root(),
    )
    .await;

    // 3 tests - 1 timeout-test
    assert_eq!(counter.load(Ordering::Relaxed), 2);

    assert!(!stats.is_success());
    assert_eq!(stats.total(), 3);
    assert_eq!(stats.launched(), 3);
    assert_eq!(stats.ok(), 2);
    assert_eq!(stats.failed(), 1);
    assert_eq!(
        stats.failed_tests(),
        vec!["timeout_root::timeouted".to_string()]
    ); // 1 timeouted test
}
