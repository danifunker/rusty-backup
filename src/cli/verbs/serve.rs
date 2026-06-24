//! `rb-cli serve` — the network daemon (Family F) plus its boot-service and
//! interactive-setup subcommands.
//!
//! - `rb-cli serve` (no subcommand) runs the daemon in the foreground. Lets a
//!   remote `rb-cli` browse and read/write files inside disk images this host
//!   holds, via `rb://host:port/img@N` references.
//! - `rb-cli serve service {start|stop|restart|status|install|uninstall}` is the
//!   MiSTer "rb-daemon" lifecycle: detached start/stop, and install/uninstall of
//!   the `user-startup.sh` autostart entry. See [`crate::remote::service`].
//! - `rb-cli serve setup` is the interactive console the Scripts menu launches.
//!
//! See `docs/remote_transfer_plan.md`.

use anyhow::Result;
use clap::{Args, Subcommand, ValueEnum};
use std::path::PathBuf;

use crate::remote::service::{self as svc, ServicePaths};
use crate::remote::{serve, ServeConfig};

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Address to bind, `host:port`. Default binds all interfaces on the
    /// rusty-backup port (7341). Ignored by the `service` / `setup` subcommands
    /// (those read `rb-daemon.ini`).
    #[arg(long, default_value = "0.0.0.0:7341")]
    pub bind: String,

    /// Root directory images are served from. Every `rb://` path a client
    /// opens is sandboxed under this directory.
    #[arg(long, default_value = ".")]
    pub root: PathBuf,

    /// Directory for per-session upload staging blobs (write path). Defaults to
    /// the system temp dir. On a MiSTer point this at a roomy writable mount,
    /// never tmpfs — large uploads would fill RAM.
    #[arg(long = "staging-dir")]
    pub staging_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<ServeCommand>,
}

#[derive(Debug, Subcommand)]
pub enum ServeCommand {
    /// Manage the boot service (start/stop/restart/status/install/uninstall).
    Service(ServiceArgs),
    /// Open the interactive setup console (the MiSTer Scripts-menu screen).
    Setup,
}

#[derive(Debug, Args)]
pub struct ServiceArgs {
    /// What to do with the daemon service.
    #[arg(value_enum)]
    pub action: ServiceAction,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ServiceAction {
    /// Spawn the daemon in the background (records a PID file).
    Start,
    /// Stop the running daemon.
    Stop,
    /// Restart the daemon.
    Restart,
    /// Print whether the daemon is running, enabled on boot, and reachable.
    Status,
    /// Register autostart in user-startup.sh and start now.
    Install,
    /// Remove the autostart entry (leaves a running daemon alone).
    Uninstall,
}

pub fn run(args: ServeArgs) -> Result<()> {
    match args.command {
        None => serve(ServeConfig {
            bind: args.bind,
            root: args.root,
            staging_dir: args.staging_dir,
        }),
        Some(ServeCommand::Setup) => crate::cli::verbs::setup::run(),
        Some(ServeCommand::Service(service)) => run_service(service.action),
    }
}

fn run_service(action: ServiceAction) -> Result<()> {
    let paths = ServicePaths::detect()?;
    match action {
        ServiceAction::Start => {
            svc::start(&paths)?;
            println!("rb-daemon started");
        }
        ServiceAction::Stop => {
            svc::stop(&paths)?;
            println!("rb-daemon stopped");
        }
        ServiceAction::Restart => {
            svc::restart(&paths)?;
            println!("rb-daemon restarted");
        }
        ServiceAction::Install => {
            svc::install(&paths)?;
            println!("rb-daemon installed for autostart and started");
        }
        ServiceAction::Uninstall => {
            svc::uninstall(&paths)?;
            println!("rb-daemon autostart removed");
        }
        ServiceAction::Status => print_status(&svc::status(&paths)),
    }
    Ok(())
}

fn print_status(status: &svc::ServiceStatus) {
    println!(
        "state:     {}",
        if status.active { "ACTIVE" } else { "INACTIVE" }
    );
    if let Some(pid) = status.pid {
        println!("pid:       {pid}");
    }
    println!(
        "autostart: {}",
        if status.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    let port = svc::bind_port(&status.bind);
    if status.addrs.is_empty() {
        println!("connect:   (no network detected)  port {port}");
    } else {
        for addr in &status.addrs {
            let iface = if addr.iface.is_empty() {
                String::new()
            } else {
                format!("  ({})", addr.iface)
            };
            println!("connect:   {}:{}{}", addr.ip, port, iface);
        }
    }
}
