/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use e2etest::ConfigUnshare;
use e2etest::Statistics;
use e2etest::UnshareInfo;
use ipc_channel::ipc::IpcOneShotServer;

e2etest::group!(name = empty_group);

#[test]
fn empty() {
    let (rx, ipc_channel) = IpcOneShotServer::<Statistics>::new().unwrap();
    e2etest::run_in_unshare(ConfigUnshare::new(UnshareInfo {
        filter: "".to_string(),
        ipc_channel,
    }));
    let (_, stats) = rx.accept().unwrap();

    assert!(!stats.is_success());
    assert_eq!(stats.tests_defined(), 0);
}
