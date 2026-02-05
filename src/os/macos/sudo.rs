//! Sudo-based privilege elevation for macOS.
//!
//! Uses the standard `sudo -E --askpass` mechanism with a custom osascript dialog
//! to prompt for administrator credentials. This approach is simpler and more
//! reliable than the SMAppService daemon approach.
//!
//! Based on Balena Etcher's implementation.

use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};

/// The osascript (JXA) code for the password prompt dialog.
/// This creates a native macOS dialog asking for the user's password.
const ASKPASS_SCRIPT: &str = r#"#!/usr/bin/env osascript -l JavaScript

ObjC.import('stdlib')

const app = Application.currentApplication()
app.includeStandardAdditions = true

const result = app.displayDialog('Rusty Backup needs administrator access to for low-level disk reads and writes.\n\nType your password to allow this.', {
  defaultAnswer: '',
  withIcon: 'caution',
  buttons: ['Cancel', 'OK'],
  defaultButton: 'OK',
  hiddenAnswer: true,
})

if (result.buttonReturned === 'OK') {
  result.textReturned
} else {
  $.exit(255)
}
"#;

/// Marker printed to stdout when authentication succeeds.
const SUCCESSFUL_AUTH_MARKER: &str = "AUTHENTICATION_SUCCEEDED";

/// Execute a command with sudo privileges, prompting for password if needed.
///
/// Returns `Ok(true)` if the command executed successfully with elevated privileges.
/// Returns `Ok(false)` if the user cancelled the authentication dialog.
/// Returns `Err` if the command failed for other reasons.
///
/// # Arguments
/// * `command` - The command and arguments to execute (e.g., `["dd", "if=/dev/disk2", ...]`)
/// * `preserve_env` - If true, uses `-E` to preserve environment variables
pub fn sudo_execute(command: &[String], preserve_env: bool) -> Result<bool> {
    // Check if already running as root
    if unsafe { libc::geteuid() } == 0 {
        // Already elevated, run directly
        let status = Command::new(&command[0])
            .args(&command[1..])
            .status()
            .context("Failed to execute command")?;
        
        return Ok(status.success());
    }

    // Create temporary askpass script
    let askpass_path = create_askpass_script()?;
    
    // Build the shell command that prints a marker on success
    let shell_cmd = format!(
        "echo {} && {}",
        SUCCESSFUL_AUTH_MARKER,
        command
            .iter()
            .map(|arg| shell_escape(arg))
            .collect::<Vec<_>>()
            .join(" ")
    );

    // Build sudo command
    let mut sudo_cmd = Command::new("sudo");
    
    if preserve_env {
        sudo_cmd.arg("-E");
    }
    
    sudo_cmd
        .arg("--askpass")
        .arg("sh")
        .arg("-c")
        .arg(&shell_cmd)
        .env("SUDO_ASKPASS", &askpass_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    // Execute sudo command
    let output = sudo_cmd.output().context("Failed to execute sudo")?;

    // Clean up askpass script
    let _ = fs::remove_file(&askpass_path);

    // Check if authentication succeeded by looking for the marker
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if stdout.contains(SUCCESSFUL_AUTH_MARKER) {
        // Authentication succeeded, check command result
        if output.status.success() {
            Ok(true)
        } else {
            bail!(
                "Command failed with exit code {}: {}",
                output.status.code().unwrap_or(-1),
                stderr.trim()
            );
        }
    } else {
        // Authentication failed or was cancelled
        if stderr.contains("is not in the sudoers file") {
            bail!(
                "Your user account doesn't have sudo privileges. \
                 Please add your user to the sudoers file or contact your system administrator."
            );
        } else if stderr.contains("incorrect password") {
            bail!("Incorrect password. Please try again.");
        } else if output.status.code() == Some(255) {
            // Exit code 255 means user cancelled in osascript
            Ok(false)
        } else {
            bail!(
                "Failed to authenticate: {}",
                stderr.trim()
            );
        }
    }
}

/// Create the temporary askpass script and return its path.
fn create_askpass_script() -> Result<PathBuf> {
    let pid = std::process::id();
    let path = PathBuf::from(format!("/tmp/rusty-backup-askpass-{}.js", pid));
    
    let mut file = fs::File::create(&path)
        .context("Failed to create temporary askpass script")?;
    
    file.write_all(ASKPASS_SCRIPT.as_bytes())
        .context("Failed to write askpass script")?;
    
    // Make executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms)?;
    }
    
    Ok(path)
}

/// Escape a shell argument for safe inclusion in a shell command.
fn shell_escape(arg: &str) -> String {
    if arg.is_empty() {
        return "''".to_string();
    }
    
    // If the argument contains no special characters, return as-is
    if arg.chars().all(|c| {
        c.is_alphanumeric() || c == '-' || c == '_' || c == '/' || c == '.' || c == ':'
    }) {
        return arg.to_string();
    }
    
    // Otherwise, wrap in single quotes and escape any single quotes
    format!("'{}'", arg.replace('\'', "'\\''"))
}

/// Request elevation for the entire application by relaunching with sudo.
///
/// This should be called at startup if the application detects it needs
/// elevated privileges. If successful, this function will not return
/// (the current process will be replaced by the elevated one).
pub fn request_app_elevation() -> Result<()> {
    // Get the current executable path
    let exe_path = env::current_exe()
        .context("Failed to get current executable path")?;
    
    // Get current arguments
    let args: Vec<String> = env::args().skip(1).collect();
    
    // Build command: [exe_path, arg1, arg2, ...]
    let mut command = vec![exe_path.to_string_lossy().to_string()];
    command.extend(args);
    
    eprintln!("Requesting administrator privileges...");
    
    // Request elevation
    match sudo_execute(&command, true) {
        Ok(true) => {
            // The elevated process has completed, we can exit
            std::process::exit(0);
        }
        Ok(false) => {
            bail!("User cancelled the administrator authentication request");
        }
        Err(e) => {
            bail!("Failed to elevate application: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shell_escape() {
        assert_eq!(shell_escape("simple"), "simple");
        assert_eq!(shell_escape("/dev/disk2"), "/dev/disk2");
        assert_eq!(shell_escape("hello world"), "'hello world'");
        assert_eq!(shell_escape("it's"), "'it'\\''s'");
        assert_eq!(shell_escape(""), "''");
    }
}
