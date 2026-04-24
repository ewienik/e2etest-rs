/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use vector_search_validator_tests::TestCase;

#[framed]
/// Returns a vector of all known test cases to be run. Each test case is registered with a name
async fn register() -> Vec<(String, TestCase)> {
    vector_search_validator::test_cases().await.collect()
}

fn main() -> Result<(), &'static str> {
    vector_search_validator_engine::run(register)
}
