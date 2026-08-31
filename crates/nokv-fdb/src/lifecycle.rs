/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::{Arc, Mutex, Weak};

pub(crate) struct RuntimeRegistry<T> {
    state: Mutex<RuntimeState<T>>,
}

enum RuntimeState<T> {
    Fresh,
    Running(Weak<RuntimeCore<T>>),
    Stopped,
}

pub(crate) struct RuntimeCore<T> {
    registry: Weak<RuntimeRegistry<T>>,
    _resource: T,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RuntimeAcquireError<E> {
    Start(E),
    Stopped,
}

impl<T> RuntimeRegistry<T> {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(RuntimeState::Fresh),
        }
    }

    pub(crate) fn acquire<E>(
        self: &Arc<Self>,
        start: impl FnOnce() -> Result<T, E>,
    ) -> Result<Arc<RuntimeCore<T>>, RuntimeAcquireError<E>> {
        let mut state = self.state.lock().expect("FDB runtime state lock poisoned");
        match &*state {
            RuntimeState::Running(core) => {
                if let Some(core) = core.upgrade() {
                    return Ok(core);
                }
                *state = RuntimeState::Stopped;
                return Err(RuntimeAcquireError::Stopped);
            }
            RuntimeState::Stopped => return Err(RuntimeAcquireError::Stopped),
            RuntimeState::Fresh => {}
        }
        let resource = match start() {
            Ok(resource) => resource,
            Err(error) => {
                *state = RuntimeState::Stopped;
                return Err(RuntimeAcquireError::Start(error));
            }
        };
        let core = Arc::new(RuntimeCore {
            registry: Arc::downgrade(self),
            _resource: resource,
        });
        *state = RuntimeState::Running(Arc::downgrade(&core));
        Ok(core)
    }
}

impl<T> Drop for RuntimeCore<T> {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut state = registry
            .state
            .lock()
            .expect("FDB runtime state lock poisoned during shutdown");
        *state = RuntimeState::Stopped;
    }
}
