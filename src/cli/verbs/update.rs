//! `rb-cli update` — check for a newer release and, when built with the
//! optional `tui-update` feature, self-update.
//!
//! The verb always exists so scripts and users get a definitive answer. Without
//! `tui-update` it reports that self-update wasn't compiled in and prints the
//! releases URL, exiting non-zero. With the feature it checks GitHub releases
//! (honoring the `config.json` opt-in) and reports whether a newer version is
//! available, plus the download URL for this platform.

use anyhow::Result;

pub fn run() -> Result<()> {
    #[cfg(feature = "tui-update")]
    {
        run_check_and_report()
    }
    #[cfg(not(feature = "tui-update"))]
    {
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
fn run_check_and_report() -> Result<()> {
    let cfg = crate::update::UpdateConfig::load();
    let current = env!("APP_VERSION");
    crate::cli::logging::out_stdout(format!("Current version: {current}"));
    match crate::update::check_for_updates(&cfg.update_check, current) {
        Ok(info) => {
            if info.is_outdated {
                crate::cli::logging::out_stdout(format!(
                    "A newer version is available: {}",
                    info.latest_version
                ));
                crate::cli::logging::out_stdout(format!("Release page: {}", info.releases_url));
                if let Some(url) = info.cli_asset_url.or(info.asset_url) {
                    crate::cli::logging::out_stdout(format!(
                        "Download this platform's build: {url}"
                    ));
                }
                crate::cli::logging::out_stdout(
                    "Automatic in-place replacement is not enabled yet on this platform; \
                     download and replace the binary/bundle from the link above.",
                );
            } else {
                crate::cli::logging::out_stdout("You are on the latest version.");
            }
            Ok(())
        }
        Err(e) => anyhow::bail!("update check failed: {e}"),
    }
}
