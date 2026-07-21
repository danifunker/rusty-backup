//! `rb-cli update` — check for a newer release and, when built with the
//! optional `tui-update` feature, self-update.
//!
//! The verb always exists so scripts and users get a definitive answer. Without
//! `tui-update` it reports that self-update wasn't compiled in and prints the
//! releases URL, exiting non-zero. With the feature it checks GitHub releases
//! (honoring the `config.json` opt-in) and reports whether a newer version is
//! available, plus the download URL for this platform.

use anyhow::Result;
use clap::Args;

#[derive(Debug, Args)]
pub struct UpdateArgs {
    /// After checking, download the newer release and replace this binary in
    /// place (requires `--features tui-update`). Without it, `update` only
    /// reports what's available. On macOS/Linux this swaps the running `rb-cli`
    /// via a temp-file + rename; on Windows it uses the self-replace path.
    #[arg(long)]
    pub apply: bool,
}

pub fn run(args: UpdateArgs) -> Result<()> {
    #[cfg(feature = "tui-update")]
    {
        run_check_and_report(args.apply)
    }
    #[cfg(not(feature = "tui-update"))]
    {
        let _ = args;
        // The `update` module (config + releases URL) is only compiled with the
        // gui / tui-update features, so this path uses the known default URL.
        crate::cli::logging::log_stderr(
            "rb-cli update: self-update was not compiled into this build \
             (rebuild with `--features tui-update` to enable it).",
        );
        crate::cli::logging::out_stdout(
            "Download the latest release manually: \
             https://github.com/danifunker/rusty-backup/releases",
        );
        std::process::exit(2)
    }
}

#[cfg(feature = "tui-update")]
fn run_check_and_report(apply: bool) -> Result<()> {
    let cfg = crate::update::UpdateConfig::load();
    let current = env!("APP_VERSION");
    crate::cli::logging::out_stdout(format!("Current version: {current}"));
    let info = match crate::update::check_for_updates(&cfg.update_check, current) {
        Ok(info) => info,
        Err(e) => anyhow::bail!("update check failed: {e}"),
    };
    if !info.is_outdated {
        crate::cli::logging::out_stdout("You are on the latest version.");
        return Ok(());
    }
    crate::cli::logging::out_stdout(format!(
        "A newer version is available: {}",
        info.latest_version
    ));
    crate::cli::logging::out_stdout(format!("Release page: {}", info.releases_url));
    if let Some(url) = info.cli_asset_url.clone().or(info.asset_url.clone()) {
        crate::cli::logging::out_stdout(format!("Download this platform's build: {url}"));
    }
    if !apply {
        crate::cli::logging::out_stdout(
            "Re-run with `--apply` to download and replace this binary in place.",
        );
        return Ok(());
    }
    apply_update(&info)
}

/// Perform the in-place self-update after a `--apply`. Unix swaps the running
/// `rb-cli` via temp-file + rename; Windows uses the self-replace path.
#[cfg(feature = "tui-update")]
fn apply_update(info: &crate::update::UpdateInfo) -> Result<()> {
    crate::cli::logging::out_stdout("Downloading and applying update...");
    let progress = |_downloaded: u64, _total: Option<u64>| {};

    #[cfg(unix)]
    {
        match crate::update::download_and_apply_cli_update_unix(info, &progress) {
            Ok(path) => {
                crate::cli::logging::out_stdout(format!(
                    "Updated {} in place. Re-run rb-cli to use the new version.",
                    path.display()
                ));
                Ok(())
            }
            Err(e) => anyhow::bail!(
                "update download/apply failed: {e}\nDownload it manually: {}",
                info.releases_url
            ),
        }
    }
    #[cfg(windows)]
    {
        match crate::update::download_and_apply_update(info, &progress) {
            Ok(()) => {
                crate::cli::logging::out_stdout(
                    "Update applied. Restart rb-cli to use the new version.",
                );
                Ok(())
            }
            Err(e) => anyhow::bail!(
                "update download/apply failed: {e}\nDownload it manually: {}",
                info.releases_url
            ),
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = info;
        anyhow::bail!("in-place self-update is not supported on this platform")
    }
}
