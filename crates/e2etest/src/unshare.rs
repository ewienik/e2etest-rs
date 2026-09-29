/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use crate::Statistics;
use ipc_channel::ipc::IpcOneShotServer;
use std::env;
use std::process::Child;
const UNSHARE_INFO_ENV: &str = "E2ETEST_UNSHARE_INFO";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UnshareInfo {
    pub filter: String,
    pub ipc_channel: String,
}

pub fn unshare_info() -> Option<UnshareInfo> {
    env::var(UNSHARE_INFO_ENV)
        .ok()
        .map(|s| serde_json::from_str(&s).expect("Failed to deserialize unshare info"))
}

pub(crate) fn spawn(name: String) -> (Child, IpcOneShotServer<Statistics>) {
    todo!("Implement spawn logic for unshare process");
}

pub(crate) fn init_namespaces() {
    // TODO: Implement namespace initialization logic here
}
