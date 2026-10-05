/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use e2etest::Config;

e2etest::group!(name = empty_root, fixtures = ());

#[tokio::test]
async fn empty() {
    let stats = e2etest::run(Config::default(), empty_root()).await;

    assert!(!stats.is_success());
    assert_eq!(stats.tests_defined(), 0);
}
