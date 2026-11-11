//! Which Windows GUI is running: the egui app or the native Win32 shell.
//!
//! The two ship as separate packages (their own installer, ZIP, Add/Remove Programs entry and
//! "Open with" ProgId) until the Win32 shell replaces egui (docs/win32_shell_plan.md, slice 9).
//! Everything that differs between them is keyed here, by the running exe's file name.

/// One of the two Windows GUIs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowsGui {
    Egui,
    Win32,
}

impl WindowsGui {
    /// The GUI this process is, from its exe name; anything else (rb-cli, tests) counts as egui.
    pub fn current() -> Self {
        let exe = std::env::current_exe().ok();
        let name = exe
            .as_ref()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str());
        Self::from_exe_name(name.unwrap_or_default())
    }

    pub fn from_exe_name(name: &str) -> Self {
        if name.eq_ignore_ascii_case(Self::Win32.exe_name()) {
            Self::Win32
        } else {
            Self::Egui
        }
    }

    /// The name Explorer's "Open with" shows; the shared version resource can't tell the GUIs apart.
    pub fn app_name(self) -> &'static str {
        match self {
            Self::Egui => "Rusty Backup",
            Self::Win32 => "Rusty Backup (Win32)",
        }
    }

    pub fn exe_name(self) -> &'static str {
        match self {
            Self::Egui => "rusty-backup.exe",
            Self::Win32 => "rusty-backup-win32.exe",
        }
    }

    /// Release asset prefix, before `-<arch>-<version>.zip` (`.github/workflows/release.yml`).
    pub fn zip_prefix(self) -> &'static str {
        match self {
            Self::Egui => "Rusty-Backup-windows",
            Self::Win32 => "Rusty-Backup-Win32-windows",
        }
    }

    /// The Inno `AppId`; must match `installer/rusty-backup.iss` / `rusty-backup-win32.iss`.
    pub fn app_id(self) -> &'static str {
        match self {
            Self::Egui => "{8F3A1C2E-5B7D-4E9A-9C1F-2D6B8A4E7F10}",
            Self::Win32 => "{D9A96B8F-A4DA-49F7-A1BB-9BC1BF5E9CDC}",
        }
    }

    /// The ProgId its file associations register under, and its description.
    pub fn progid(self) -> (&'static str, &'static str) {
        match self {
            Self::Egui => ("RustyBackup.DiskImage", "Rusty Backup Disk Image"),
            Self::Win32 => (
                "RustyBackup.Win32.DiskImage",
                "Rusty Backup (Win32) Disk Image",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WindowsGui;

    #[test]
    fn each_gui_has_its_own_open_with_name() {
        assert_eq!(WindowsGui::Win32.app_name(), "Rusty Backup (Win32)");
        assert_ne!(WindowsGui::Egui.app_name(), WindowsGui::Win32.app_name());
    }

    #[test]
    fn exe_name_picks_the_gui() {
        assert_eq!(
            WindowsGui::from_exe_name("rusty-backup-win32.exe"),
            WindowsGui::Win32
        );
        assert_eq!(
            WindowsGui::from_exe_name("RUSTY-BACKUP-WIN32.EXE"),
            WindowsGui::Win32
        );
        assert_eq!(
            WindowsGui::from_exe_name("rusty-backup.exe"),
            WindowsGui::Egui
        );
        assert_eq!(WindowsGui::from_exe_name("rb-cli.exe"), WindowsGui::Egui);
    }
}
