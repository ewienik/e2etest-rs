/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::panic;
use std::sync::Arc;
use std::sync::RwLock;

/// A wrapper around a backtrace string that can be shared across threads.
#[derive(Clone, Debug)]
pub(crate) struct Backtrace(Arc<RwLock<String>>);

impl Backtrace {
    pub(crate) fn new() -> Self {
        Self(Arc::new(RwLock::new(String::new())))
    }

    pub(crate) fn get(&self) -> String {
        self.0.read().unwrap().clone()
    }
}

pub(crate) fn setup_panic_hook() -> Backtrace {
    let backtrace = Backtrace::new();
    let old_hook = panic::take_hook();
    panic::set_hook(Box::new({
        let backtrace = backtrace.clone();
        move |info| {
            *backtrace.0.write().unwrap() = async_backtrace::taskdump_tree(true);
            old_hook(info);
        }
    }));
    backtrace
}

pub(crate) fn clear_panic_hook() {
    _ = panic::take_hook();
}
