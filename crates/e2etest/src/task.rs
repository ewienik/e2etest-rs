/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use crate::run::RunContext;
use async_backtrace::frame;
use async_backtrace::framed;
use std::time::Duration;
use tokio::time;
use tracing::Instrument;
use tracing::Span;
use tracing::error;
use tracing::error_span;
use tracing::info;

#[framed]
pub(crate) async fn setup<T: Send + Sync + 'static>(
    setup: impl Future<Output = T> + Send + 'static,
    timeout: Duration,
    ctx: RunContext,
) -> Result<T, ()> {
    single(error_span!("setup"), setup, timeout, ctx).await
}

#[framed]
pub(crate) async fn teardown(
    teardown: impl Future<Output = ()> + Send + 'static,
    timeout: Duration,
    ctx: RunContext,
) -> Result<(), ()> {
    single::<()>(error_span!("teardown"), teardown, timeout, ctx).await
}

#[framed]
pub(crate) async fn test(
    run: impl Future<Output = ()> + Send + 'static,
    timeout: Duration,
    ctx: RunContext,
) -> Result<(), ()> {
    single::<()>(error_span!("run"), run, timeout, ctx).await
}

#[framed]
pub(crate) async fn single<T: Send + Sync + 'static>(
    span: Span,
    fut: impl Future<Output = T> + Send + 'static,
    timeout: Duration,
    ctx: RunContext,
) -> Result<T, ()> {
    let task_result = tokio::spawn(frame!(
        async move { time::timeout(timeout, fut).await.expect("test timed out") }
            .instrument(span.clone())
    ))
    .await;

    match task_result {
        Err(err) => {
            let backtrace = ctx.backtrace.get();
            error!(parent: &span, "test failed: {err}\n{backtrace}");
            Err(())
        }
        Ok(t) => {
            info!(parent: &span, "test ok");
            Ok(t)
        }
    }
}
