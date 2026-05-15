/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use crate::DEFAULT_TIMEOUT;
use crate::backtrace::Backtrace;
use crate::filter::Filter;
use crate::fixture::Fixtures;
use crate::statistics::Statistics;
use std::fmt::Debug;
use std::time::Duration;

#[derive(Clone)]
pub struct RunContext {
    pub(crate) fixtures: Fixtures,
    pub(crate) statistics: Statistics,
    pub(crate) backtrace: Backtrace,
    pub(crate) filter: Filter,
    pub(crate) default_timeout: Duration,
}

impl RunContext {
    pub(crate) fn new() -> Self {
        Self {
            fixtures: Fixtures::new(),
            statistics: Statistics::new(),
            backtrace: Backtrace::new(),
            filter: Filter::empty(),
            default_timeout: DEFAULT_TIMEOUT,
        }
    }

    pub(crate) fn with_fixtures(mut self, fixtures: Fixtures) -> Self {
        self.fixtures = fixtures;
        self
    }

    pub(crate) fn with_backtrace(mut self, backtrace: Backtrace) -> Self {
        self.backtrace = backtrace;
        self
    }

    pub(crate) fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = filter;
        self
    }

    pub(crate) fn with_default_timeout(mut self, default_timeout: Duration) -> Self {
        self.default_timeout = default_timeout;
        self
    }
}

impl Debug for RunContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Run").finish()
    }
}
