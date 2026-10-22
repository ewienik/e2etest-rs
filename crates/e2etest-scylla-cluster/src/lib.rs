/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

//! This crate provides a ScyllaDB cluster manager for e2etest tests. It provides an actor with
//! handler using `tokio::sync::mpsc::Sender` over `enum ScyllaCluster` message. It provides also a
//! `trait ScyllaClusterExt` with helper methods to send messages to the actor.

use async_backtrace::frame;
use async_backtrace::framed;
use nix::errno::Errno;
use nix::fcntl;
use nix::fcntl::OFlag;
use nix::mount;
use nix::mount::MsFlags;
use nix::sched;
use nix::sys::stat::Mode;
use nix::unistd;
use std::ffi::CString;
use std::ffi::OsStr;
use std::io;
use std::iter;
use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::process::Output;
use std::process::Stdio;
use std::time::Duration;
use tap::Tap;
use tempfile::TempDir;
use testcontainers::ContainerAsync;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::bollard::plugin::Mount;
use testcontainers::bollard::plugin::MountBindOptions;
use testcontainers::bollard::plugin::MountBindOptionsPropagationEnum;
use testcontainers::bollard::plugin::MountType;
use testcontainers::runners::AsyncRunner;
use tokio::fs;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::info;

const DEFAULT_SCYLLA_CQL_PORT: u16 = 9042;

/// Configuration for a single ScyllaDB node in the test cluster.
#[derive(Clone)]
pub struct ScyllaNodeConfig {
    /// The IP address of this ScyllaDB node.
    pub db_ip: Ipv4Addr,
    /// Primary Vector Store URIs (--vector-store-primary-uri).
    pub primary_vs_uris: Vec<String>,
    /// Secondary Vector Store URIs (--vector-store-secondary-uri).
    pub secondary_vs_uris: Vec<String>,
    /// Args to pass to the ScyllaDB process.
    pub args: Vec<String>,
    /// Optional path to the TLS certificate file for client encryption.
    pub cert_path: Option<PathBuf>,
    /// Optional path to the TLS key file for client encryption.
    pub key_path: Option<PathBuf>,
    /// Optional extra YAML config to append to the ScyllaDB options file
    /// (e.g. authentication settings).
    pub extra_config: Option<Vec<u8>>,
}

/// Messages for the ScyllaDB cluster manager actor.
pub enum ScyllaCluster {
    Version {
        tx: oneshot::Sender<String>,
    },
    Start {
        node_configs: Vec<ScyllaNodeConfig>,
    },
    WaitForReady {
        tx: oneshot::Sender<bool>,
    },
    Stop {
        tx: oneshot::Sender<()>,
    },
    Up {
        node_configs: Vec<ScyllaNodeConfig>,
    },
    UpNode {
        node_config: ScyllaNodeConfig,
    },
    Down {
        tx: oneshot::Sender<()>,
    },
    DownNode {
        db_ip: Ipv4Addr,
        tx: oneshot::Sender<()>,
    },
    Flush {
        tx: oneshot::Sender<()>,
    },
}

/// Extension trait for `mpsc::Sender<ScyllaCluster>` to provide convenient async methods for
/// interacting with the ScyllaDB cluster manager actor.
pub trait ScyllaClusterExt {
    /// Returns the version of the ScyllaDB executable.
    fn version(&self) -> impl Future<Output = String>;

    /// Starts the ScyllaDB cluster with the given node configurations.
    fn start(&self, node_configs: Vec<ScyllaNodeConfig>) -> impl Future<Output = ()>;

    /// Stops the ScyllaDB cluster.
    fn stop(&self) -> impl Future<Output = ()>;

    /// Waits for the ScyllaDB cluster to be ready.
    fn wait_for_ready(&self) -> impl Future<Output = bool>;

    /// Starts a paused cluster back again.
    fn up(&self, node_configs: Vec<ScyllaNodeConfig>) -> impl Future<Output = ()>;

    /// Pauses a cluster.
    fn down(&self) -> impl Future<Output = ()>;

    /// Starts a single paused ScyllaDB instance back again.
    fn up_node(&self, node_config: ScyllaNodeConfig) -> impl Future<Output = ()>;

    /// Pauses a single ScyllaDB instance.
    fn down_node(&self, db_ip: Ipv4Addr) -> impl Future<Output = ()>;

    /// Restarts a single ScyllaDB instance.
    fn restart(&self, node_config: &ScyllaNodeConfig) -> impl Future<Output = ()>;

    /// Flushes all memtables to disk on all nodes.
    fn flush(&self) -> impl Future<Output = ()>;
}

impl ScyllaClusterExt for mpsc::Sender<ScyllaCluster> {
    #[framed]
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Version { tx })
            .await
            .expect("ScyllaClusterExt::version: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::version: internal actor should send response")
    }

    #[framed]
    async fn start(&self, node_configs: Vec<ScyllaNodeConfig>) {
        self.send(ScyllaCluster::Start { node_configs })
            .await
            .expect("ScyllaClusterExt::start: internal actor should receive request");
    }

    #[framed]
    async fn stop(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Stop { tx })
            .await
            .expect("ScyllaClusterExt::stop: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::stop: internal actor should send response");
    }

    #[framed]
    async fn wait_for_ready(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::WaitForReady { tx })
            .await
            .expect("ScyllaClusterExt::wait_for_ready: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::wait_for_ready: internal actor should send response")
    }

    #[framed]
    async fn up(&self, node_configs: Vec<ScyllaNodeConfig>) {
        self.send(ScyllaCluster::Up { node_configs })
            .await
            .expect("ScyllaClusterExt::up: internal actor should receive request")
    }

    #[framed]
    async fn up_node(&self, node_config: ScyllaNodeConfig) {
        self.send(ScyllaCluster::UpNode { node_config })
            .await
            .expect("ScyllaClusterExt::up_node: internal actor should receive request")
    }

    #[framed]
    async fn down(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Down { tx })
            .await
            .expect("ScyllaClusterExt::down: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::down: internal actor should send response");
    }

    #[framed]
    async fn down_node(&self, db_ip: Ipv4Addr) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::DownNode { db_ip, tx })
            .await
            .expect("ScyllaClusterExt::down_node: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::down_node: internal actor should send response");
    }

    #[framed]
    async fn restart(&self, node_config: &ScyllaNodeConfig) {
        self.down_node(node_config.db_ip).await;
        self.up_node(node_config.clone()).await;
        assert!(self.wait_for_ready().await);
    }

    #[framed]
    async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Flush { tx })
            .await
            .expect("ScyllaClusterExt::flush: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::flush: internal actor should send response");
    }
}

/// Creates a new ScyllaDB cluster manager actor and returns a sender to send messages to it.
///
/// `target` can be either a path to the ScyllaDB executable or a Docker image name (with optional
/// tag).
#[framed]
pub async fn new(
    target_name: String,
    default_conf: PathBuf,
    tempdir: PathBuf,
    verbose: bool,
) -> mpsc::Sender<ScyllaCluster> {
    let (tx, mut rx) = mpsc::channel(10);

    let (target, docker_export) = prepare_target(target_name, &tempdir).await;
    assert!(
        file_exists(&default_conf).await,
        "scylla config '{default_conf:?}' does not exist"
    );

    let mut state = State::new(target, default_conf, tempdir, verbose).await;

    tokio::spawn(
        frame!(async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &mut state).await;
            }

            info!("Final shutting down the scylla cluster...");
            stop(&mut state).await;

            if let Some(DockerExport {
                dir: _dir,
                container,
            }) = docker_export
            {
                container
                    .stop()
                    .await
                    .expect("failed to stop scylla container");
            }
            debug!("finished");
        })
        .instrument(debug_span!("db")),
    );

    tx
}

enum ChrootError {
    Unshare = 1,
    SetHostname,
    MountRoot,
    MkDir,
    Touch,
    MountBind,
    Chroot,
    Chdir,
}

impl ChrootError {
    const MASK_LEN: u32 = 24;
    const MASK_SYS_ERROR: i32 = (u32::MAX >> (u32::BITS - Self::MASK_LEN)) as i32;
    const MASK_CHROOT_ERROR: i32 = !Self::MASK_SYS_ERROR;

    fn encode(self, err: io::Error) -> io::Error {
        let errno = err.raw_os_error().unwrap_or(0) & Self::MASK_SYS_ERROR;
        io::Error::from_raw_os_error(errno | ((self as i32) << Self::MASK_LEN))
    }

    fn decode(err: io::Error) -> (Option<Self>, io::Error) {
        let Some(errno) = err.raw_os_error() else {
            return (None, err);
        };
        let err_chroot = errno & Self::MASK_CHROOT_ERROR;
        if err_chroot == 0 {
            return (None, err);
        }
        (
            match err_chroot >> Self::MASK_LEN {
                x if x == Self::Unshare as i32 => Some(Self::Unshare),
                x if x == Self::SetHostname as i32 => Some(Self::SetHostname),
                x if x == Self::MountRoot as i32 => Some(Self::MountRoot),
                x if x == Self::MountBind as i32 => Some(Self::MountBind),
                x if x == Self::MkDir as i32 => Some(Self::MkDir),
                x if x == Self::Touch as i32 => Some(Self::Touch),
                x if x == Self::Chroot as i32 => Some(Self::Chroot),
                x if x == Self::Chdir as i32 => Some(Self::Chdir),
                _ => None,
            },
            io::Error::from_raw_os_error(errno & Self::MASK_SYS_ERROR),
        )
    }
}

fn panic_on_command_error(name: &str, err: io::Error) -> ! {
    let (unshare_err, err) = ChrootError::decode(err);
    match unshare_err {
        Some(ChrootError::Unshare) => {
            error!("{name}: Failed to unshare: {err}")
        }
        Some(ChrootError::SetHostname) => {
            error!("{name}: Failed to set hostname: {err}")
        }
        Some(ChrootError::MountRoot) => {
            error!("{name}: Failed to mount private root: {err}")
        }
        Some(ChrootError::MkDir) => {
            error!("{name}: Failed to make directory: {err}")
        }
        Some(ChrootError::Touch) => {
            error!("{name}: Failed to touch file: {err}")
        }
        Some(ChrootError::MountBind) => {
            error!("{name}: Failed to mount bind: {err}")
        }
        Some(ChrootError::Chroot) => {
            error!("{name}: Failed to chroot: {err}")
        }
        Some(ChrootError::Chdir) => {
            error!("{name}: Failed to chdir: {err}")
        }
        None => {
            error!("{name}: {err}")
        }
    }
    panic!("error while executing command");
}

trait WithinChroot {
    fn within_chroot(&mut self, rootfs: &Path, binds: &[(&Path, &Path)]) -> &mut Self;
}

impl WithinChroot for Command {
    fn within_chroot(&mut self, rootfs: &Path, binds: &[(&Path, &Path)]) -> &mut Self {
        let mut dirs_to_create = vec![];
        let mut files_to_create = vec![];
        let mut add_recursive = |path: &Path, file: bool| {
            let parent = if let Some(parent) = path.parent() {
                parent.iter().fold(rootfs.to_path_buf(), |acc, p| {
                    let dir = acc.join(p);
                    dirs_to_create.push(
                        CString::new(dir.to_string_lossy().as_bytes())
                            .expect("within_chroot: failed to create CString for directory"),
                    );
                    dir
                })
            } else {
                rootfs.to_path_buf()
            };
            if file {
                files_to_create.push(
                    CString::new(
                        parent
                            .join(path.file_name().unwrap())
                            .to_string_lossy()
                            .as_bytes(),
                    )
                    .expect("within_chroot: failed to create CString for file"),
                );
            } else {
                dirs_to_create.push(
                    CString::new(
                        parent
                            .join(path.file_name().unwrap())
                            .to_string_lossy()
                            .as_bytes(),
                    )
                    .expect("within_chroot: failed to create CString for directory"),
                );
            }
        };

        add_recursive(Path::new("proc"), false);
        add_recursive(Path::new("sys"), false);
        add_recursive(Path::new("dev"), false);
        add_recursive(Path::new("etc/resolv.conf"), true);

        let binds: Vec<_> = iter::once((
            CString::new("/proc").expect("within_chroot: failed to create CString for /proc"),
            CString::new(rootfs.join("proc").to_string_lossy().as_bytes())
                .expect("within_chroot: failed to create CString for /proc mount point"),
        ))
        .chain(iter::once((
            CString::new("/sys").expect("within_chroot: failed to create CString for /sys"),
            CString::new(rootfs.join("sys").to_string_lossy().as_bytes())
                .expect("within_chroot: failed to create CString for /sys mount point"),
        )))
        .chain(iter::once((
            CString::new("/dev").expect("within_chroot: failed to create CString for /dev"),
            CString::new(rootfs.join("dev").to_string_lossy().as_bytes())
                .expect("within_chroot: failed to create CString for /dev mount point"),
        )))
        .chain(iter::once((
            CString::new("/etc/resolv.conf")
                .expect("within_chroot: failed to create CString for /etc/resolv.conf"),
            CString::new(
                rootfs
                    .join("etc")
                    .join("resolv.conf")
                    .to_string_lossy()
                    .as_bytes(),
            )
            .expect("within_chroot: failed to create CString for /etc/resolv.conf mount point"),
        )))
        .chain(binds.iter().map(|(src, dst)| {
            let dst = dst.strip_prefix("/").unwrap_or(dst);
            add_recursive(dst, src.is_file());
            (
                CString::new(src.to_string_lossy().as_bytes())
                    .expect("within_chroot: failed to create CString for bind source"),
                CString::new(rootfs.join(dst).to_string_lossy().as_bytes())
                    .expect("within_chroot: failed to create CString for bind destination"),
            )
        }))
        .collect();

        // Sort and deduplicate the directories and files to create
        dirs_to_create.sort();
        dirs_to_create.dedup();
        files_to_create.sort();
        files_to_create.dedup();

        let rootfs = CString::new(rootfs.to_string_lossy().as_bytes())
            .expect("within_chroot: failed to create CString for rootfs");
        unsafe {
            // SAFETY: We must not allocate memory in this closure, as it is called after fork()
            // and before exec().
            self.pre_exec(move || {
                sched::unshare(sched::CloneFlags::CLONE_NEWNS | sched::CloneFlags::CLONE_NEWUTS)
                    .map_err(|err| ChrootError::Unshare.encode(err.into()))?;

                unistd::sethostname("e2etest-chroot")
                    .map_err(|e| ChrootError::SetHostname.encode(e.into()))?;

                // Stop mount events from propagating back to the host
                mount::mount(
                    None::<&str>,
                    &b"/"[..],
                    None::<&str>,
                    MsFlags::MS_REC | MsFlags::MS_PRIVATE,
                    None::<&str>,
                )
                .map_err(|err| ChrootError::MountRoot.encode(err.into()))?;

                for dir in dirs_to_create.iter() {
                    if let Err(err) = unistd::mkdir(dir.as_c_str(), Mode::from_bits_truncate(0o755))
                        && err != Errno::EEXIST
                    {
                        return Err(ChrootError::MkDir.encode(err.into()));
                    }
                }
                for file in files_to_create.iter() {
                    fcntl::open(
                        file.as_c_str(),
                        OFlag::O_CREAT | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
                        Mode::from_bits_truncate(0o644),
                    )
                    .map_err(|err| ChrootError::Touch.encode(err.into()))?;
                }

                for (src, dst) in binds.iter() {
                    mount::mount(
                        Some(src.as_c_str()),
                        dst.as_c_str(),
                        None::<&str>,
                        MsFlags::MS_BIND | MsFlags::MS_REC,
                        None::<&str>,
                    )
                    .map_err(|err| ChrootError::MountBind.encode(err.into()))?;
                }

                unistd::chroot(rootfs.as_c_str())
                    .map_err(|err| ChrootError::Chroot.encode(err.into()))?;

                unistd::chdir(&b"/"[..]).map_err(|err| ChrootError::Chdir.encode(err.into()))?;

                Ok(())
            });
        }
        self
    }
}

enum Target {
    Binary(PathBuf),
    ContainerRoot(PathBuf),
}

impl Target {
    const CONTAINER_CERT_PATH: &'static str = "/e2etest/cert.pem";
    const CONTAINER_KEY_PATH: &'static str = "/e2etest/key.pem";

    fn cert_key<'a>(&self, node_config: &'a ScyllaNodeConfig) -> Option<(&'a Path, &'a Path)> {
        match self {
            Target::Binary(_) => {
                if let (Some(cert), Some(key)) = (&node_config.cert_path, &node_config.key_path) {
                    Some((cert, key))
                } else {
                    None
                }
            }
            Target::ContainerRoot(_) => {
                if let (Some(_), Some(_)) = (&node_config.cert_path, &node_config.key_path) {
                    Some((
                        Path::new(Self::CONTAINER_CERT_PATH),
                        Path::new(Self::CONTAINER_KEY_PATH),
                    ))
                } else {
                    None
                }
            }
        }
    }
}

struct DockerExport {
    dir: TempDir,
    container: ContainerAsync<GenericImage>,
}

async fn prepare_target(name: String, tempdir: &Path) -> (Target, Option<DockerExport>) {
    let path = PathBuf::from(&name);
    if executable_exists(&path).await {
        (Target::Binary(path), None)
    } else {
        let dir = TempDir::new_in(tempdir)
            .expect("prepare_target: failed to create temporary directory for docker export");
        let rootfs = dir.path().join("rootfs");
        fs::create_dir_all(&rootfs)
            .await
            .expect("prepare_target: failed to create rootfs directory");
        let image = if let Some((name, tag)) = name.split_once(':') {
            GenericImage::new(name, tag)
        } else {
            GenericImage::new(name.as_str(), "latest")
        };
        let mount = Mount::default().tap_mut(|m| {
            m.source = Some(dir.path().to_string_lossy().to_string());
            m.target = Some("/export".to_string());
            m.typ = Some(MountType::BIND);
            m.bind_options = Some(MountBindOptions::default().tap_mut(|bo| {
                bo.propagation = Some(MountBindOptionsPropagationEnum::RSHARED);
            }));
        });
        let container = image
            .with_entrypoint("sh")
            .with_privileged(true)
            .with_user("root")
            .with_cmd(["-c", "mount --bind / /export/rootfs && exec sleep infinity"])
            .with_host_config_modifier(move |hc| {
                hc.mounts.get_or_insert_with(Vec::new).push(mount.clone());
            })
            .start()
            .await
            .expect("prepare_target: failed to start scylla container");
        (
            Target::ContainerRoot(rootfs),
            Some(DockerExport { dir, container }),
        )
    }
}

impl Target {
    async fn scylla(
        &self,
        args: impl IntoIterator<Item = impl Into<String> + AsRef<OsStr>>,
    ) -> Output {
        let mut cmd = match self {
            Target::Binary(path) => {
                let mut cmd = Command::new(path);
                cmd.args(args);
                cmd
            }
            Target::ContainerRoot(path) => {
                let mut cmd = Command::new("/usr/bin/scylla");
                cmd.args(args).within_chroot(path, &[]);
                cmd
            }
        };

        match cmd.output().await {
            Ok(output) => output,
            Err(err) => {
                panic_on_command_error("scylla", err);
            }
        }
    }

    async fn run(
        &self,
        node_config: &ScyllaNodeConfig,
        workdir: &Path,
        confdir: &Path,
        conffile: &Path,
        seeds: &str,
        verbose: bool,
    ) -> Child {
        let db_ip = node_config.db_ip;
        let mut args = vec![
            "--listen-address".to_string(),
            db_ip.to_string(),
            "--rpc-address".to_string(),
            db_ip.to_string(),
            "--api-address".to_string(),
            db_ip.to_string(),
            "--log-to-stdout".to_string(),
            "true".to_string(),
            "--logger-ostream-type".to_string(),
            "stdout".to_string(),
            "--endpoint-snitch".to_string(),
            "GossipingPropertyFileSnitch".to_string(),
        ]
        .tap_mut(|args| {
            if !node_config.primary_vs_uris.is_empty() {
                args.extend([
                    "--vector-store-primary-uri".to_string(),
                    node_config.primary_vs_uris.join(","),
                ]);
            }
        })
        .tap_mut(|args| {
            if !node_config.secondary_vs_uris.is_empty() {
                args.extend([
                    "--vector-store-secondary-uri".to_string(),
                    node_config.secondary_vs_uris.join(","),
                ]);
            }
        })
        .tap_mut(|args| {
            args.extend(node_config.args.clone());
        });

        let mut cmd = match self {
            Target::Binary(path) => {
                args.extend([
                    "--options-file".to_string(),
                    conffile.to_string_lossy().to_string(),
                    "--workdir".to_string(),
                    workdir.to_string_lossy().to_string(),
                ]);
                if !seeds.is_empty() {
                    args.extend([
                        "--seed-provider-parameters".to_string(),
                        format!("seeds={seeds}"),
                    ]);
                }

                let mut cmd = Command::new(path);
                cmd.env("SCYLLA_CONF", confdir);
                cmd
            }

            Target::ContainerRoot(rootfs) => {
                let mut mounts = vec![
                    (workdir, Path::new("/e2etest/workdir")),
                    (confdir, Path::new("/e2etest/confdir")),
                    (conffile, Path::new("/e2etest/conffile")),
                ];
                args.extend([
                    "--options-file".to_string(),
                    "/e2etest/conffile".to_string(),
                    "--workdir".to_string(),
                    "/e2etest/workdir".to_string(),
                ]);
                if !seeds.is_empty() {
                    args.extend(["--seeds".to_string(), seeds.to_string()]);
                }
                if let Some((container_cert, container_key)) = self.cert_key(node_config)
                    && let (Some(cert), Some(key)) = (&node_config.cert_path, &node_config.key_path)
                {
                    mounts.extend([
                        (cert.as_path(), container_cert),
                        (key.as_path(), container_key),
                    ]);
                }

                let mut cmd = Command::new("/usr/bin/scylla");
                cmd.within_chroot(rootfs, &mounts);
                cmd.env("SCYLLA_CONF", "/e2etest/confdir");
                cmd
            }
        };
        if !verbose {
            cmd.stdout(Stdio::null()).stderr(Stdio::null());
        }
        cmd.args(args)
            .spawn()
            .expect("start: failed to spawn scylladb")
    }
}

struct NodeState {
    db_ip: Ipv4Addr,
    child: Option<Child>,
    workdir: Option<TempDir>,
}

struct State {
    target: Target,
    default_conf: PathBuf,
    tempdir: PathBuf,
    nodes: Vec<NodeState>,
    version: String,
    verbose: bool,
}

impl State {
    #[framed]
    async fn new(target: Target, default_conf: PathBuf, tempdir: PathBuf, verbose: bool) -> Self {
        let version = String::from_utf8_lossy(&target.scylla(["--version"]).await.stdout)
            .trim()
            .to_string();

        Self {
            target,
            default_conf,
            version,
            tempdir,
            nodes: Vec::new(),
            verbose,
        }
    }
}

#[framed]
async fn process(msg: ScyllaCluster, state: &mut State) {
    match msg {
        ScyllaCluster::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process ScyllaCluster::Version: failed to send a response");
        }

        ScyllaCluster::Start { node_configs } => {
            start(node_configs, state).await;
        }

        ScyllaCluster::Stop { tx } => {
            stop(state).await;
            tx.send(())
                .expect("process ScyllaCluster::Stop: failed to send a response");
        }

        ScyllaCluster::WaitForReady { tx } => {
            tx.send(wait_for_ready(state).await)
                .expect("process ScyllaCluster::WaitForReady: failed to send a response");
        }

        ScyllaCluster::Up { node_configs } => {
            up(node_configs, state).await;
        }

        ScyllaCluster::UpNode { node_config } => {
            up_node(node_config, state).await;
        }

        ScyllaCluster::Down { tx } => {
            down(state).await;
            tx.send(())
                .expect("process ScyllaCluster::Down: failed to send a response");
        }

        ScyllaCluster::DownNode { db_ip, tx } => {
            down_node(state, db_ip).await;
            tx.send(())
                .expect("process ScyllaCluster::DownNode: failed to send a response");
        }

        ScyllaCluster::Flush { tx } => {
            flush(state).await;
            tx.send(())
                .expect("process ScyllaCluster::Flush: failed to send a response");
        }
    }
}

#[framed]
async fn run_node(
    node_config: &ScyllaNodeConfig,
    seeds: &str,
    path: &Path,
    rack: &str,
    state: &State,
) -> Child {
    let cert_key = state.target.cert_key(node_config);
    let has_tls = cert_key.is_some();
    let has_extra = node_config.extra_config.is_some();

    let conf = if has_tls || has_extra {
        let mut config = Vec::new();
        if let Some((cert, key)) = cert_key {
            config.extend_from_slice(
                format!(
                    r#"client_encryption_options:
  enabled: true
  certificate: {cert}
  keyfile: {key}"#,
                    cert = cert.display(),
                    key = key.display()
                )
                .as_bytes(),
            );
        }
        if let Some(extra) = &node_config.extra_config {
            if !config.is_empty() {
                config.push(b'\n');
            }
            config.extend_from_slice(extra);
        }
        let conf_path = path.join("scylla.conf");
        fs::write(&conf_path, config)
            .await
            .expect("start: failed to write scylla config");
        conf_path
    } else {
        state.default_conf.clone()
    };

    let conf_dir = path.join("conf");
    fs::create_dir_all(&conf_dir)
        .await
        .expect("start: failed to create conf directory");

    let rack_dc_properties = format!("dc=datacenter1\nrack={rack}\nprefer_local=true\n");
    let properties_path = conf_dir.join("cassandra-rackdc.properties");
    debug!(
        "Creating cassandra-rackdc.properties at {:?}",
        properties_path
    );

    fs::write(&properties_path, &rack_dc_properties)
        .await
        .expect("start: failed to write cassandra-rackdc.properties");

    state
        .target
        .run(node_config, path, &conf_dir, &conf, seeds, state.verbose)
        .await
}

#[framed]
async fn start(node_configs: Vec<ScyllaNodeConfig>, state: &mut State) {
    if node_configs.is_empty() {
        return;
    }

    let db_ips: Vec<Ipv4Addr> = node_configs.iter().map(|c| c.db_ip).collect();
    debug!("scylla_cluster: using DB IPs: {:?}", db_ips);

    // The first node IP will be used as seed for whole cluster
    let seed_ip = db_ips.first().unwrap();

    // Start each node sequentially with proper seed configuration
    for (i, node_config) in node_configs.iter().enumerate() {
        let workdir = TempDir::new_in(&state.tempdir)
            .expect("start: failed to create temporary directory for scylladb");
        let rack = format!("rack{}", i + 1);
        let seeds = seed_ip.to_string();

        info!(
            "Starting Scylla node {} on IP {} in {} (primary: {:?}, secondary: {:?})",
            i + 1,
            node_config.db_ip,
            rack,
            node_config.primary_vs_uris,
            node_config.secondary_vs_uris
        );

        let child = run_node(node_config, &seeds, workdir.path(), &rack, state).await;

        state.nodes.push(NodeState {
            db_ip: node_config.db_ip,
            child: Some(child),
            workdir: Some(workdir),
        });
    }

    debug!(
        "Started {} Scylla nodes in {}-rack cluster configuration, waiting for initialization...",
        state.nodes.len(),
        state.nodes.len()
    );
}

#[framed]
async fn stop(state: &mut State) {
    for node in &mut state.nodes {
        if let Some(mut child) = node.child.take() {
            child
                .start_kill()
                .expect("kill: failed to send SIGTERM to scylladb process");
            child
                .wait()
                .await
                .expect("kill: failed to wait for scylladb process to exit");
        }
        node.workdir = None;
    }
    state.nodes.clear();
}

#[framed]
async fn down(state: &mut State) {
    for node in &mut state.nodes {
        if let Some(mut child) = node.child.take() {
            child
                .start_kill()
                .expect("kill: failed to send SIGTERM to scylladb process");
            child
                .wait()
                .await
                .expect("kill: failed to wait for scylladb process to exit");
        }
    }
}

#[framed]
async fn down_node(state: &mut State, db_ip: Ipv4Addr) {
    if let Some(node) = state.nodes.iter_mut().find(|n| n.db_ip == db_ip)
        && let Some(mut child) = node.child.take()
    {
        child
            .start_kill()
            .expect("kill: failed to send SIGTERM to scylladb process");
        child
            .wait()
            .await
            .expect("kill: failed to wait for scylladb process to exit");
    }
}

#[framed]
async fn wait_for_node(state: &State, ip: Ipv4Addr) -> bool {
    loop {
        if String::from_utf8_lossy(
            &state
                .target
                .scylla(["nodetool", "-h", &ip.to_string(), "status"])
                .await
                .stdout,
        )
        .lines()
        .any(|line| line.starts_with(&format!("UN {ip}")))
        {
            loop {
                if is_cql_port_ready(ip).await {
                    return true;
                }
                time::sleep(Duration::from_millis(100)).await;
            }
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

#[framed]
async fn is_cql_port_ready(ip: Ipv4Addr) -> bool {
    use std::net::SocketAddr;
    use tokio::net::TcpStream;

    let addr = SocketAddr::from((ip, DEFAULT_SCYLLA_CQL_PORT));

    match tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr)).await {
        Ok(Ok(_)) => {
            debug!("CQL port {} is open on {}", DEFAULT_SCYLLA_CQL_PORT, ip);
            true
        }
        _ => {
            debug!(
                "CQL port {} is not yet ready on {}",
                DEFAULT_SCYLLA_CQL_PORT, ip
            );
            false
        }
    }
}

#[framed]
/// Waits for ScyllaDB to be ready by checking the nodetool status.
async fn wait_for_ready(state: &State) -> bool {
    if state.nodes.is_empty() {
        tracing::error!("No ScyllaDB nodes to wait for - nodes list is empty");
        return false;
    }

    for node in &state.nodes {
        tracing::info!(
            "Waiting for ScyllaDB node at IP {} to be ready...",
            &node.db_ip
        );
        wait_for_node(state, node.db_ip).await;
        tracing::info!("ScyllaDB node at IP {} is ready.", &node.db_ip);
    }

    true
}

#[framed]
async fn up(node_configs: Vec<ScyllaNodeConfig>, state: &mut State) {
    if state.nodes.is_empty() {
        return;
    }

    // Use the first node as seed for whole cluster
    let seed_ip = state.nodes.first().unwrap().db_ip.to_string();

    let nodes = state
        .nodes
        .iter()
        .enumerate()
        .map(|(i, node)| (i, node.workdir.as_ref().unwrap().path().to_path_buf()))
        .collect::<Vec<_>>();

    for (i, path) in nodes.into_iter() {
        let rack = format!("rack{}", i + 1);
        let child = run_node(&node_configs[i], &seed_ip, &path, &rack, state).await;
        state.nodes[i].child = Some(child);
    }
}

#[framed]
async fn up_node(node_config: ScyllaNodeConfig, state: &mut State) {
    if state.nodes.is_empty() {
        return;
    }

    // Use the first node as seed
    let seed_ip = state.nodes.first().unwrap().db_ip.to_string();

    if let Some((i, node)) = state
        .nodes
        .iter()
        .enumerate()
        .find(|(_, n)| n.db_ip == node_config.db_ip)
    {
        let rack = format!("rack{}", i + 1);
        let path = node.workdir.as_ref().unwrap().path().to_path_buf();
        let child = run_node(&node_config, &seed_ip, &path, &rack, state).await;
        state.nodes[i].child = Some(child);
    }
}

#[framed]
async fn flush(state: &State) {
    for node in &state.nodes {
        info!("Flushing node {}", node.db_ip);
        let output = state
            .target
            .scylla(["nodetool", "-h", &node.db_ip.to_string(), "flush"])
            .await;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::error!("nodetool flush failed on {}: {}", node.db_ip, stderr);
        } else {
            info!("Flush completed on node {}", node.db_ip);
        }
    }
}

/// Checks if the file exists and is executable.
#[framed]
async fn executable_exists(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path).await else {
        return false;
    };
    metadata.is_file() && (metadata.permissions().mode() & 0o111 != 0)
}

/// Checks if the file exists.
#[framed]
async fn file_exists(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path).await else {
        return false;
    };
    metadata.is_file()
}
