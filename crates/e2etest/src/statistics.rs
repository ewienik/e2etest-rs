/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::fmt::Debug;
use std::ops::Add;
use std::ops::AddAssign;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone)]
/// Statistics for a test run, including total tests, launched, successful, and failed.
pub struct Statistics(Arc<Mutex<Inner>>);

impl Debug for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statistics")
            .field("total", &self.total())
            .field("filtered", &self.filtered())
            .field("launched", &self.launched())
            .field("ok", &self.ok())
            .field("failed", &self.failed())
            .field("skipped", &self.skipped())
            .finish()
    }
}

struct Inner {
    total: usize,
    filtered: usize,
    launched: usize,
    ok: usize,
    failed: usize,
    skipped: usize,
    failed_tests: Vec<String>,
}

impl Inner {
    fn new() -> Self {
        Self {
            total: 0,
            filtered: 0,
            launched: 0,
            ok: 0,
            failed: 0,
            skipped: 0,
            failed_tests: Vec::new(),
        }
    }

    pub(crate) fn append(&mut self, other: &Self) {
        self.total += other.total;
        self.filtered += other.filtered;
        self.launched += other.launched;
        self.ok += other.ok;
        self.failed += other.failed;
        self.skipped += other.skipped;
        self.failed_tests.extend(other.failed_tests.iter().cloned());
    }
}

impl Add for Statistics {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        self + &other
    }
}

impl Add<&Statistics> for Statistics {
    type Output = Self;

    fn add(mut self, other: &Self) -> Self {
        self += other;
        self
    }
}

impl AddAssign for Statistics {
    fn add_assign(&mut self, other: Self) {
        *self += &other;
    }
}

impl AddAssign<&Statistics> for Statistics {
    fn add_assign(&mut self, other: &Self) {
        let mut inner = self.0.lock().unwrap();
        let other_inner = other.0.lock().unwrap();
        inner.append(&other_inner);
    }
}

impl Statistics {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(Inner::new())))
    }

    pub(crate) fn increment_total(&self, count: usize) {
        let mut inner = self.0.lock().unwrap();
        inner.total += count;
    }

    pub(crate) fn increment_filtered(&self, count: usize) {
        let mut inner = self.0.lock().unwrap();
        inner.filtered += count;
    }

    pub(crate) fn increment_launched(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.launched += 1;
    }

    pub(crate) fn increment_ok(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.ok += 1;
    }

    pub(crate) fn record_failure(&self, failed_test: impl Into<String>) {
        let mut inner = self.0.lock().unwrap();
        inner.failed += 1;
        inner.failed_tests.push(failed_test.into());
    }

    pub(crate) fn increment_skipped(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.skipped += 1;
    }

    pub fn is_success(&self) -> bool {
        let inner = self.0.lock().unwrap();
        inner.failed_tests.is_empty()
    }

    pub fn total(&self) -> usize {
        let inner = self.0.lock().unwrap();
        inner.total
    }

    pub fn filtered(&self) -> usize {
        let inner = self.0.lock().unwrap();
        inner.filtered
    }

    pub fn launched(&self) -> usize {
        let inner = self.0.lock().unwrap();
        inner.launched
    }

    pub fn ok(&self) -> usize {
        let inner = self.0.lock().unwrap();
        inner.ok
    }

    pub fn failed(&self) -> usize {
        let inner = self.0.lock().unwrap();
        inner.failed
    }

    pub fn skipped(&self) -> usize {
        let inner = self.0.lock().unwrap();
        inner.skipped
    }

    pub fn failed_tests(&self) -> Vec<String> {
        let inner = self.0.lock().unwrap();
        inner.failed_tests.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add() {
        let stats_base = Statistics::new();
        stats_base.increment_total(2);
        stats_base.increment_launched();
        stats_base.increment_ok();
        stats_base.record_failure("crud::boom");
        stats_base.increment_skipped();

        let stats = Statistics::new();
        stats.increment_total(20);
        stats.increment_launched();
        stats.increment_ok();
        stats.record_failure("crud::cleanup");
        stats.increment_skipped();

        let stats = stats_base + stats;

        assert_eq!(stats.total(), 22);
        assert_eq!(stats.launched(), 2);
        assert_eq!(stats.ok(), 2);
        assert_eq!(stats.failed(), 2);
        assert_eq!(stats.skipped(), 2);
        assert_eq!(
            stats.failed_tests(),
            vec!["crud::boom".to_string(), "crud::cleanup".to_string()]
        );
    }
}
