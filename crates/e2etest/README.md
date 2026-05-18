# e2etest

The main crate for E2E test framework for Rust.

[![crates.io](https://img.shields.io/crates/v/e2etest.svg)](https://crates.io/crates/e2etest)
[![docs.rs](https://img.shields.io/docsrs/e2etest/latest)](https://docs.rs/e2etest)

## Getting Started

The user needs to define global fixture for all tests, initialization and
callback for test cases registration. The user needs to create a binary crate -
`e2etest` doesn't build directly into a binary.

The user can use the actors provided by `e2etest-rs` sub-crates, or create
their own actors. Most likely, the user wants to run dns server, firewall and
other testing actors - so the provided binary ought to be run in the unshared
environment.  In the future `e2etest` will provide the unshared environment
directly, without additional setup.

**Sample code for using `e2etest`**

```rust
use e2etest::TestCase;
use std::net::Ipv4Addr;
use std::time::Duration;

#[derive(clap::Args)]
struct Args {
    #[arg(short, long, default_value = "127.0.100.1")]
    dns_ip: Ipv4Addr,
}

fn init(args: &Args) {
}

#[derive(Clone)]
struct Fixture {
    dns_ip: Ipv4Addr,
}

async fn fixture(args: &Args) -> Fixture {
    Fixture {
        dns_ip: args.dns_ip,
    }
}

async fn init_testcase(fixture: Fixture) {
}

async fn cleanup_testcase(fixture: Fixture) {
}

async fn dns_ip(fixture: Fixture) {
    assert_eq!(fixture.dns_ip, Ipv4Addr::new(127, 0, 100, 1));
}

async fn register() -> Vec<(String, TestCase<Fixture>)> {
    let timeout = Duration::from_secs(10);
    let testcase = TestCase::empty()
        .with_init(timeout, init_testcase)
        .with_cleanup(timeout, cleanup_testcase)
        .with_test("dns_ip", timeout, dns_ip);
    vec![("simple".to_string(), testcase)]
}

e2etest::run(["validator", "run"], init, register, fixture);
```

**Sample code for script to run in the unshared environment:**

```bash
#!/bin/bash

set -e

base_ip=127.0.1
dns_ip=127.0.1.1

tmp_resolv_conf=$(mktemp /tmp/resolv.conf.XXXXXX)
echo "nameserver $dns_ip" > $tmp_resolv_conf

sudo unshare -n -m /bin/bash <<EOF
mount --bind $tmp_resolv_conf /etc/resolv.conf
ip link set lo up
ip addr add $dns_ip/32 dev lo
for i in {1..10}; do
    ip addr add $base_ip.\$i/32 dev lo
done
cat /etc/resolv.conf
$e2e_validator run --dns-ip $dns_ip --base-ip $args
EOF

rm $tmp_resolv_conf
```

**Sample code for running tests in a docker container:**

```bash
#!/bin/bash

set -e

dns_ip=127.0.1.1

docker run --rm \
    --cap-add NET_ADMIN \
    --user root \
    --security-opt seccomp=unconfined \
    --dns=$dns_ip \
    --dns-search=. \
    --volume="$e2e_validator:/e2e-validator" \
    --network=none \
    --entrypoint=/e2e-validator \
    $docker_image \
    run --dns-ip $dns_ip $args "$@"
```

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
