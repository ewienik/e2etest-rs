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

e2etest::group!(name = skip_root);

#[e2etest::test(group = skip_root)]
async fn first(fixture: Arc<Fixture>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = skip_root, skip = false)]
async fn second(fixture: Arc<Fixture>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[e2etest::test(group = skip_root, skip = true)]
async fn skipped(fixture: Arc<Fixture>) {
    fixture.0.0.fetch_add(1, Ordering::Relaxed);
}

#[tokio::test]
async fn skip() {
    let counter = Arc::new(AtomicUsize::new(0));

    let stats = e2etest::run(
        Config::default()
            .with_permanent_fixture(Counter(Arc::clone(&counter)))
            .with_default_timeout(Duration::from_secs(1)),
        skip_root(),
    )
    .await;

    // 3 tests - 1 test-skipped
    assert_eq!(counter.load(Ordering::Relaxed), 2);

    assert!(stats.is_success());
    assert_eq!(stats.total(), 3);
    assert_eq!(stats.launched(), 2);
    assert_eq!(stats.ok(), 2);
    assert_eq!(stats.failed(), 0);
    assert_eq!(stats.skipped(), 1);
}
