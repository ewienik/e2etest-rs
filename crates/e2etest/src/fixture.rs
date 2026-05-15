/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use futures::FutureExt;
use futures::future::BoxFuture;
use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A fixture manager. It allows you to set up and tear down fixtures for tests.
#[derive(Clone)]
pub(crate) struct Fixtures(Arc<Mutex<Inner>>);

impl std::fmt::Debug for Fixtures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Fixtures").finish()
    }
}

impl Fixtures {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(Inner::new())))
    }

    pub(crate) fn with_permanent(
        fixtures: impl Iterator<Item = Arc<dyn Any + Send + Sync>>,
    ) -> Self {
        Self(Arc::new(Mutex::new(Inner::with_permanent(fixtures))))
    }

    pub(crate) fn setup<F: Fixture>(&self) -> impl Future<Output = Arc<F>> + Send + use<F> {
        let inner = Arc::clone(&self.0);
        async move { inner.lock().await.setup::<F>().await }
    }

    pub fn teardown(&self) -> impl Future<Output = ()> + Send + use<> {
        let inner = Arc::clone(&self.0);
        async move {
            inner.lock().await.teardown().await;
        }
    }
}

/// A fixture.
///
/// It is a type that can be set up and torn down by the fixture manager.
pub trait Fixture: Any + Send + Sync {
    /// The timeout for setting up this fixture. If the setup takes longer than this, the test will
    /// fail. If this returns `None`, there is default timeout for setting up this fixture.
    fn timeout_setup() -> Option<std::time::Duration> {
        None
    }

    /// The timeout for tearing down this fixture. If the teardown takes longer than this, the test
    /// will fail. If this returns `None`, there is default timeout for tearing down this fixture.
    fn timeout_teardown() -> Option<std::time::Duration> {
        None
    }

    /// Set up this fixture. This will be called by the fixture manager when setting up this
    /// fixture.
    fn setup(setup: &mut impl Setup) -> impl Future<Output = Self> + Send;

    /// Tear down this fixture. This will be called by the fixture manager when tearing down this
    /// fixture.
    fn teardown(self) -> impl Future<Output = ()> + Send;
}

trait Teardown: Any + Send + Sync {
    fn teardown(self: Arc<Self>) -> BoxFuture<'static, ()>;
}

impl<F: Fixture> Teardown for F {
    fn teardown(self: Arc<Self>) -> BoxFuture<'static, ()> {
        async move {
            let fixture = Arc::try_unwrap(self).ok().unwrap();
            fixture.teardown().await
        }
        .boxed()
    }
}

/// A fixture setup.
///
/// It allows you to access to the other fixtures while setting up a fixture. It
/// is passed to the `setup` method of a fixture.
pub trait Setup: Send {
    /// Set up a new fixture. If the fixture is already set up, this will do nothing.
    fn setup<F: Fixture>(&mut self) -> impl Future<Output = Arc<F>> + Send;

    /// Get a fixture. If the fixture is not set up, this will return `None`.
    fn get<F: Send + Sync + 'static>(&self) -> impl Future<Output = Option<Arc<F>>> + Send;
}

struct Inner {
    permanent: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    cache: HashMap<TypeId, Arc<dyn Teardown>>,
}

impl Setup for Inner {
    async fn setup<F: Fixture>(&mut self) -> Arc<F> {
        if self.permanent.contains_key(&TypeId::of::<F>())
            || self.cache.contains_key(&TypeId::of::<F>())
        {
            return self.get::<F>().await.unwrap();
        }
        let fixture = Arc::new(F::setup(self).await);
        self.cache
            .insert(TypeId::of::<F>(), Arc::clone(&fixture) as Arc<dyn Teardown>);
        fixture
    }

    async fn get<F: Send + Sync + 'static>(&self) -> Option<Arc<F>> {
        if let Some(fixture) = self.permanent.get(&TypeId::of::<F>()) {
            return Some(Arc::clone(fixture).downcast::<F>().unwrap());
        }
        if let Some(fixture) = self.cache.get(&TypeId::of::<F>()) {
            return Some(
                (Arc::clone(fixture) as Arc<dyn Any + Send + Sync>)
                    .downcast::<F>()
                    .unwrap(),
            );
        }
        None
    }
}

impl Inner {
    fn new() -> Self {
        Self {
            permanent: HashMap::new(),
            cache: HashMap::new(),
        }
    }

    fn with_permanent(fixtures: impl Iterator<Item = Arc<dyn Any + Send + Sync>>) -> Self {
        let permanent = fixtures
            .map(|any| {
                let type_id = (*any).type_id();
                (type_id, any)
            })
            .collect();
        Self {
            permanent,
            cache: HashMap::new(),
        }
    }

    async fn teardown(&mut self) {
        let mut search = true;
        while search {
            search = false;
            for fixture in self
                .cache
                .extract_if(|_, fixture| Arc::strong_count(fixture) == 1)
                .map(|(_, fixture)| fixture)
            {
                search = true;
                fixture.teardown().await;
            }
        }
    }
}
