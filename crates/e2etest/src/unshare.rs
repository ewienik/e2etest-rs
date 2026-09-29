/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use crate::Restricted;
use crate::Statistics;
use ipc_channel::ipc::IpcOneShotServer;
use std::env;
use std::process::Child;
const UNSHARE_INFO_ENV: &str = "E2ETEST_UNSHARE_INFO";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UnshareInfo {
    pub(crate) restricted: Restricted,
    pub(crate) filters: Vec<String>,
    pub(crate) ipc_channel: String,
}

impl UnshareInfo {
    /// Returns the name of the test if the process is restricted to a specific test.
    pub fn test_name(&self) -> Option<&str> {
        match &self.restricted {
            Restricted::None | Restricted::Group(_) => None,
            Restricted::Test(name) => Some(name),
        }
    }

    /// Returns the name of the group if the process is restricted to a specific group.
    pub fn group_name(&self) -> Option<&str> {
        match &self.restricted {
            Restricted::None | Restricted::Test(_) => None,
            Restricted::Group(name) => Some(name),
        }
    }
}

pub fn unshare_info() -> Option<UnshareInfo> {
    env::var(UNSHARE_INFO_ENV)
        .ok()
        .map(|s| serde_json::from_str(&s).expect("Failed to deserialize unshare info"))
}

pub(crate) fn spawn(
    restricted: Restricted,
    filters: Vec<String>,
) -> (Child, IpcOneShotServer<Statistics>) {
    todo!("Implement spawn logic for unshare process");
}

pub(crate) fn init_namespaces() {
    // TODO: Implement namespace initialization logic here
}
