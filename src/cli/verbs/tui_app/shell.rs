//! The app shell's parts of the TUI over `controller::app`: the update banner of a `tui-update`
//! build (the start-up release check, F12 hides it; the install is `rb-cli update --apply`) and
//! the backup folder the Restore and Inspect screens share, as the GUI's root does
//! (`AppController::share_backup` / `close_shared_backup`). See docs/phase2_commander_plan.md
//! § "Slice 29".

use super::{App, RestoreState, TabId};

impl App {
    /// The start-up release check, when config.json enables it (`tui-update` builds).
    pub(super) fn start_update_check(&mut self) {
        #[cfg(feature = "tui-update")]
        self.shell.start_update_check();
    }

    /// The release the shell announces: (latest, current) versions.
    pub(super) fn update_banner(&self) -> Option<(String, String)> {
        #[cfg(feature = "tui-update")]
        {
            self.shell
                .update_banner()
                .map(|i| (i.latest_version.clone(), i.current_version.clone()))
        }
        #[cfg(not(feature = "tui-update"))]
        None
    }

    /// The line under the tab bar: the update banner, then the shared backup; `true` warns.
    pub(super) fn shell_line(&self) -> Option<(String, bool)> {
        if let Some((latest, current)) = self.update_banner() {
            return Some((
                format!(
                    "Update available: {latest} (current {current}). \
                     Install: `:` then `update --apply`.  F12 hide"
                ),
                true,
            ));
        }
        let folder = self.shell.shared_backup()?;
        let name = folder
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| folder.display().to_string());
        Some((format!("Backup: {name} (Restore and Inspect)"), false))
    }

    /// Restore and Inspect share one backup folder, as the GUI's tabs do: the showing screen
    /// publishes a backup it opened, or adopts the shared one when it has nothing open.
    pub(super) fn share_backup_tick(&mut self) {
        use crate::controller::app::ShareStep;
        use crate::controller::restore::RestoreAction;
        // A backup the user closed on either screen is closed on both.
        let from_restore = self
            .restore
            .as_mut()
            .is_some_and(|r| std::mem::take(&mut r.closed_backup));
        let from_inspect = self
            .inspect
            .as_mut()
            .is_some_and(|s| std::mem::take(&mut s.closed_backup));
        if from_restore || from_inspect {
            self.shell.close_shared_backup();
        }
        if from_inspect {
            if let Some(r) = self.restore.as_mut().filter(|r| r.ctl.has_backup()) {
                r.act(RestoreAction::ClearBackup);
            }
        }
        if from_restore {
            if let Some(s) = self.inspect.as_mut() {
                if s.ctl.backup_folder().is_some() {
                    s.act(crate::controller::inspect::InspectAction::Unload { quiet: true });
                    s.closed_backup = false;
                }
            }
        }
        match self.current() {
            TabId::Restore => {
                let Some(r) = self.restore.as_mut() else {
                    return;
                };
                let loaded = r.ctl.backup_folder().map(|p| p.to_path_buf());
                let empty = !r.ctl.has_backup() && !r.ctl.has_other_source();
                match self.shell.share_backup(loaded, empty) {
                    ShareStep::Publish(folder) => self.inspect_load_backup(folder),
                    ShareStep::Adopt(folder) => r.act(RestoreAction::OpenBackup(folder)),
                    ShareStep::Nothing => {}
                }
            }
            TabId::Inspect => {
                let Some(s) = self.inspect.as_ref() else {
                    return;
                };
                let loaded = s.ctl.backup_folder().map(|p| p.to_path_buf());
                let empty = loaded.is_none() && !s.ctl.has_source() && !s.ctl.is_preparing();
                match self.shell.share_backup(loaded, empty) {
                    ShareStep::Publish(folder) => {
                        let recent = self.shell.recent_handle();
                        let r = self
                            .restore
                            .get_or_insert_with(|| RestoreState::new(recent));
                        if r.ctl.backup_folder() != Some(folder.as_path()) {
                            r.act(RestoreAction::OpenBackup(folder));
                        }
                    }
                    ShareStep::Adopt(folder) => self.inspect_load_backup(folder),
                    ShareStep::Nothing => {}
                }
            }
            _ => {}
        }
    }
}
