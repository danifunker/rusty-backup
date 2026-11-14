//! The update banner: the start-up release check and (Windows, macOS) the in-app download and
//! install, each a job. Linux updates through its AppImage, so it only links the release.

use super::AppController;
use crate::controller::job_outcome;
use crate::model::job::{Job, Progress, Unit};

pub use crate::update::UpdateInfo;

/// Where the in-app install is.
#[derive(Debug, Clone, PartialEq)]
pub enum InstallState {
    /// Downloading; the fraction when the server gave a length.
    Downloading(Option<f32>),
    /// Installed; restart to run it.
    Ready,
    /// Not installed in place: the package was opened for the user, who installs it by hand.
    Manual(String),
    Failed(String),
}

#[derive(Default)]
pub(super) struct UpdateState {
    check: Option<Job<Option<UpdateInfo>>>,
    /// The check was asked for (Check for Updates...), so its answer is said even when up to date.
    asked: bool,
    info: Option<UpdateInfo>,
    dismissed: bool,
    /// `Ok(Some(why))`: opened for the user to install by hand.
    install: Option<Job<Result<Option<String>, String>>>,
    installed: Option<Result<Option<String>, String>>,
}

impl AppController {
    /// Check for a newer release in a job, if config.json enables the check.
    pub fn start_update_check(&mut self) {
        let config = self.config.load_config().update_check;
        if !config.enabled {
            return;
        }
        self.spawn_check(config);
    }

    /// Check now, whatever config.json says (the menu's Check for Updates...); the answer is said.
    pub(super) fn check_updates_now(&mut self) {
        if self.update.check.is_some() {
            return;
        }
        let mut config = self.config.load_config().update_check;
        config.enabled = true;
        self.update.asked = true;
        self.update.dismissed = false;
        self.spawn_check(config);
    }

    fn spawn_check(&mut self, config: crate::update::UpdateCheckConfig) {
        self.update.check = Some(Job::spawn(
            "Checking for updates",
            self.notify(),
            move |_| {
                let current = env!("APP_VERSION");
                Ok(crate::update::check_for_updates(&config, current).ok())
            },
        ));
    }

    /// The release to announce: newer than this build and not dismissed.
    pub fn update_banner(&self) -> Option<&UpdateInfo> {
        self.update
            .info
            .as_ref()
            .filter(|i| i.is_outdated && !self.update.dismissed)
    }

    /// The banner's line for the announced release.
    pub fn update_banner_text(&self) -> Option<String> {
        let info = self.update_banner()?;
        Some(format!(
            "available: v{} -> v{}",
            info.current_version, info.latest_version
        ))
    }

    /// Open the announced release's page in the browser.
    pub(super) fn view_release(&self) -> Vec<crate::controller::Effect> {
        let url = self.update.info.as_ref().map(|i| i.releases_url.clone());
        url.map(crate::controller::Effect::OpenUrl)
            .into_iter()
            .collect()
    }

    pub(super) fn dismiss_update(&mut self) {
        self.update.dismissed = true;
    }

    /// Whether this build installs the announced release itself (Windows, macOS; a matching asset).
    pub fn can_install_update(&self) -> bool {
        cfg!(any(windows, target_os = "macos"))
            && self
                .update
                .info
                .as_ref()
                .is_some_and(|i| i.asset_url.is_some())
    }

    /// The in-app install's state, once it was started.
    pub fn install_state(&self) -> Option<InstallState> {
        if let Some(job) = &self.update.install {
            let fraction = match job.snapshot().progress {
                Progress::Determinate { .. } => job.snapshot().progress.fraction(),
                _ => None,
            };
            return Some(InstallState::Downloading(fraction));
        }
        match &self.update.installed {
            Some(Ok(None)) => Some(InstallState::Ready),
            Some(Ok(Some(why))) => Some(InstallState::Manual(why.clone())),
            Some(Err(e)) => Some(InstallState::Failed(e.clone())),
            None => None,
        }
    }

    pub(super) fn install_update(&mut self) {
        let Some(info) = self.update.info.clone() else {
            return;
        };
        if self.update.install.is_some() || info.asset_url.is_none() {
            return;
        }
        self.update.installed = None;
        self.log.info("Downloading update...");
        self.update.install = Some(Job::spawn(
            "Downloading update",
            self.notify(),
            move |ctx| {
                ctx.set_can_cancel(false);
                let progress = |done: u64, total: Option<u64>| {
                    ctx.set_progress(match total {
                        Some(total) => Progress::Determinate {
                            done,
                            total,
                            unit: Unit::Bytes,
                        },
                        None => Progress::Counting {
                            done,
                            unit: Unit::Bytes,
                        },
                    });
                };
                let done = match crate::update::download_and_apply_update(&info, &progress) {
                    Ok(()) => Ok(None),
                    Err(e) => match e.downcast::<crate::update::ManualInstall>() {
                        Ok(manual) => Ok(Some(manual.0)),
                        Err(e) => Err(e.to_string()),
                    },
                };
                Ok(done)
            },
        ));
    }

    /// "Retry" after a failed install: back to the offer.
    pub(super) fn retry_update(&mut self) {
        self.update.installed = None;
    }

    pub(super) fn poll_update(&mut self) -> Vec<crate::controller::Effect> {
        let mut effects = Vec::new();
        if let Some(done) = self.update.check.as_ref().and_then(job_outcome) {
            self.update.check = None;
            self.update.info = done.ok().flatten();
            if std::mem::take(&mut self.update.asked) {
                let current = env!("APP_VERSION");
                let said = match &self.update.info {
                    Some(i) if i.is_outdated => None,
                    Some(_) => Some(format!("Up to date (current {current}).")),
                    None => {
                        Some("Update check failed: the release feed could not be read.".to_string())
                    }
                };
                effects.extend(said.map(crate::controller::Effect::Message));
            }
        }
        if let Some(done) = self.update.install.as_ref().and_then(job_outcome) {
            self.update.install = None;
            let done = done.and_then(|r| r);
            match &done {
                Ok(None) => self.log.info("Update installed; restart to run it."),
                Ok(Some(why)) => self.log.info(format!("Update: {why}")),
                Err(e) => self.log.error(format!("Update failed: {e}")),
            }
            self.update.installed = Some(done);
        }
        effects
    }

    /// For tests: announce `info` as if the start-up check had found it.
    pub fn set_update_info(&mut self, info: UpdateInfo) {
        self.update.info = Some(info);
    }
}
