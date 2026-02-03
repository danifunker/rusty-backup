# macOS Privileged Helper Setup Guide

This guide explains how to set up Rusty Backup's privileged helper daemon on macOS to enable disk access.

## Overview

Rusty Backup uses a privileged helper daemon (`com.rustybackup.helper`) to access disk devices on macOS. This is required because:
- macOS restricts raw disk device access to root processes
- The helper runs as root via launchd
- It communicates with the main app via a Unix socket

## Installation Steps

### 1. Install the Helper Daemon

1. Launch Rusty Backup
2. Click the **"Install Helper"** button in the top bar
3. Enter your administrator password when prompted
4. Wait for installation to complete

The helper will be installed to:
- Binary: `/Library/PrivilegedHelperTools/com.rustybackup.helper`
- Configuration: `/Library/LaunchDaemons/com.rustybackup.helper.plist`
- Log: `/var/log/rustybackup-helper.log`

### 2. Approve in Login Items (if needed)

If the app shows "Helper Needs Approval":

1. Open **System Settings**
2. Go to **General** → **Login Items**
3. Look for **com.rustybackup.helper** in the list
4. Ensure it's **enabled** (toggle should be ON)
5. Restart Rusty Backup

### 3. Grant Full Disk Access (REQUIRED)

**This is the most important step!** The helper needs Full Disk Access to read/write disk devices.

1. Open **System Settings**
2. Go to **Privacy & Security** → **Full Disk Access**
3. Click the **lock** icon and authenticate
4. Click the **+** button
5. Navigate to: `/Library/PrivilegedHelperTools/`
6. Select **`com.rustybackup.helper`**
7. Click **Open**
8. Ensure the helper is **checked** in the list

**Note:** You may need to press `Command + Shift + G` in the file picker to manually enter the path `/Library/PrivilegedHelperTools/`

## Verification

After completing all steps:

1. Restart Rusty Backup
2. The top bar should show no warnings
3. Settings → Privileged Helper Daemon should show: **"✓ Daemon is installed and running"**

## Troubleshooting

### "Helper Needs Approval" after enabling in Login Items

**Solution:** Restart your Mac. Sometimes macOS doesn't recognize the approval until after a reboot.

### "Operation not permitted" when accessing disks

**Cause:** Full Disk Access not granted

**Solution:**
1. Verify the helper is in Full Disk Access list
2. Make sure it's **checked** (enabled)
3. Try removing and re-adding it
4. Restart Rusty Backup

### Check helper status manually

```bash
# Check if helper is installed
ls -l /Library/PrivilegedHelperTools/com.rustybackup.helper

# Check if daemon is loaded
sudo launchctl list | grep rustybackup

# Check helper log
tail -50 /var/log/rustybackup-helper.log

# Check socket
ls -l /var/run/rustybackup.sock
```

### Reinstall the helper

If something goes wrong, you can reinstall:

1. Open Rusty Backup Settings
2. Click **"Uninstall Daemon"**
3. Enter your password
4. Click **"Install Helper"** again
5. Repeat the Full Disk Access steps above

## Uninstalling

To completely remove the helper:

1. Open Rusty Backup Settings
2. Click **"Uninstall Daemon"**
3. Optionally remove from Full Disk Access list in System Settings

## Why is this necessary?

macOS security model requires explicit user approval for:
- **Privileged helper installation** - via administrator password
- **Running background services** - via Login Items approval
- **Accessing disk devices** - via Full Disk Access

This multi-step process ensures that only authorized software can access your disk devices, protecting your data from malicious applications.

## For Advanced Users

### Manual Installation

```bash
# Copy files (requires sudo)
sudo cp /path/to/Rusty\ Backup.app/Contents/Library/LaunchDaemons/com.rustybackup.helper \
    /Library/PrivilegedHelperTools/

sudo cp /path/to/Rusty\ Backup.app/Contents/Resources/com.rustybackup.helper.plist \
    /Library/LaunchDaemons/

# Set permissions
sudo chmod 755 /Library/PrivilegedHelperTools/com.rustybackup.helper
sudo chown root:wheel /Library/PrivilegedHelperTools/com.rustybackup.helper
sudo chmod 644 /Library/LaunchDaemons/com.rustybackup.helper.plist
sudo chown root:wheel /Library/LaunchDaemons/com.rustybackup.helper.plist

# Load daemon
sudo launchctl load -w /Library/LaunchDaemons/com.rustybackup.helper.plist
```

### Manual Uninstallation

```bash
# Unload daemon
sudo launchctl unload -w /Library/LaunchDaemons/com.rustybackup.helper.plist

# Remove files
sudo rm /Library/PrivilegedHelperTools/com.rustybackup.helper
sudo rm /Library/LaunchDaemons/com.rustybackup.helper.plist
sudo rm /var/log/rustybackup-helper.log
sudo rm /var/run/rustybackup.sock
```
