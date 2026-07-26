//! Which operating-system *version* we are running on, at runtime.
//!
//! `std` tells you the OS *family* at compile time (`cfg!(target_os = "macos")`)
//! but never the release. That distinction is the whole story on vintage Mac OS
//! X: 10.4 and 10.5 are the same `target_os`, the same `libSystem`, and the same
//! binary format, yet they differ in ways that decide whether a program even
//! launches. Tiger exports no `$INODE64` symbols at all, has no libdispatch, no
//! `fdopendir`, and no `_Unwind_GetIPInfo` in its `libgcc_s`. A build that works
//! on Leopard can fail on Tiger before `main`.
//!
//! So when something platform-shaped is unavailable, saying *which* OS we are on
//! is far more use than "unsupported platform" — both in an error message a user
//! pastes into an issue, and as the single place to hang a 10.4-vs-10.5 decision.
//!
//! **This module takes no dependencies.** It is `std`-only on purpose: its main
//! consumer is [`crate::os`]'s stub layer, which exists precisely to keep `libc`
//! and `objc2` out of a build, so reaching for either here would defeat it.
//! Detection is a file read and a substring scan.

use std::fmt;

/// The running OS, as far as we can determine it at runtime.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HostVersion {
    /// Marketing name, e.g. `"Mac OS X"`, `"macOS"`, `"Ubuntu"`. Empty if unknown.
    pub name: String,
    /// Dotted release, e.g. `"10.4.12"`. Empty if unknown.
    pub release: String,
    /// OS build identifier where one exists, e.g. `"8SX170"`. Empty if unknown.
    pub build: String,
    /// Parsed `release` as `(major, minor, patch)`; `None` if unparseable.
    pub version: Option<(u32, u32, u32)>,
}

impl HostVersion {
    /// Detect the running OS. Never fails - unknown fields are left empty, so a
    /// caller can always render *something*.
    pub fn detect() -> Self {
        #[cfg(target_vendor = "apple")]
        {
            if let Some(v) = Self::from_darwin_plist() {
                return v;
            }
        }
        #[cfg(target_os = "linux")]
        {
            if let Some(v) = Self::from_os_release() {
                return v;
            }
        }
        Self {
            name: std::env::consts::OS.to_string(),
            ..Self::default()
        }
    }

    /// True when the running OS is at least `major.minor`.
    ///
    /// Returns `false` when the version could not be determined, so callers get
    /// the conservative answer (assume the older OS, avoid the newer facility)
    /// rather than an optimistic one.
    pub fn at_least(&self, major: u32, minor: u32) -> bool {
        match self.version {
            Some((maj, min, _)) => (maj, min) >= (major, minor),
            None => false,
        }
    }

    /// The architecture, from compile-time cfg rather than
    /// [`std::env::consts::ARCH`].
    ///
    /// `env::consts::ARCH` is baked in by libstd's build script via
    /// `STD_ENV_ARCH`, and mrustc's build takes that from the *host* triple - so
    /// a PowerPC binary built through mrustc reports `x86_64`. `cfg!` is
    /// evaluated against the real target and is correct.
    pub fn arch() -> &'static str {
        if cfg!(target_arch = "powerpc") {
            "powerpc"
        } else if cfg!(target_arch = "powerpc64") {
            "powerpc64"
        } else if cfg!(target_arch = "x86") {
            "x86"
        } else if cfg!(target_arch = "x86_64") {
            "x86_64"
        } else if cfg!(target_arch = "aarch64") {
            "aarch64"
        } else if cfg!(target_arch = "arm") {
            "arm"
        } else {
            std::env::consts::ARCH
        }
    }

    /// Mac OS X 10.2 onward keeps the release in an XML plist. Parsing it needs
    /// no framework call, which is what makes it usable from the stub layer.
    #[cfg(target_vendor = "apple")]
    fn from_darwin_plist() -> Option<Self> {
        const PLIST: &str = "/System/Library/CoreServices/SystemVersion.plist";
        let text = std::fs::read_to_string(PLIST).ok()?;
        let release = plist_string(&text, "ProductVersion").unwrap_or_default();
        Some(Self {
            name: plist_string(&text, "ProductName").unwrap_or_default(),
            build: plist_string(&text, "ProductBuildVersion").unwrap_or_default(),
            version: parse_release(&release),
            release,
        })
    }

    #[cfg(target_os = "linux")]
    fn from_os_release() -> Option<Self> {
        let text = std::fs::read_to_string("/etc/os-release").ok()?;
        let field = |key: &str| -> String {
            for line in text.lines() {
                if let Some(rest) = line.strip_prefix(key) {
                    if let Some(val) = rest.strip_prefix('=') {
                        return val.trim().trim_matches('"').to_string();
                    }
                }
            }
            String::new()
        };
        let release = field("VERSION_ID");
        Some(Self {
            name: field("NAME"),
            build: String::new(),
            version: parse_release(&release),
            release,
        })
    }
}

impl fmt::Display for HostVersion {
    /// e.g. `Mac OS X 10.4.12 (build 8SX170, powerpc)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = if self.name.is_empty() {
            std::env::consts::OS
        } else {
            &self.name
        };
        write!(f, "{name}")?;
        if !self.release.is_empty() {
            write!(f, " {}", self.release)?;
        }
        if self.build.is_empty() {
            write!(f, " ({})", Self::arch())
        } else {
            write!(f, " (build {}, {})", self.build, Self::arch())
        }
    }
}

/// Pull `<key>NAME</key><string>VALUE</string>` out of an XML plist.
///
/// Deliberately a scan rather than an XML parse: the file is machine-generated
/// with a fixed shape, and the alternative is a dependency this module cannot
/// take. Whitespace and newlines between the tags are tolerated.
// Only the Apple path reads a plist; `test` keeps the parser exercised on any
// host, which matters because the fixture is copied from a real Tiger install.
#[cfg(any(target_vendor = "apple", test))]
fn plist_string(text: &str, key: &str) -> Option<String> {
    let key_tag = format!("<key>{key}</key>");
    let after_key = &text[text.find(&key_tag)? + key_tag.len()..];
    let open = after_key.find("<string>")? + "<string>".len();
    let close = after_key[open..].find("</string>")?;
    Some(after_key[open..open + close].trim().to_string())
}

/// `"10.4.12"` -> `(10, 4, 12)`. A missing patch counts as 0.
#[cfg(any(target_vendor = "apple", target_os = "linux", test))]
fn parse_release(s: &str) -> Option<(u32, u32, u32)> {
    let mut it = s.split('.');
    let major = it.next()?.trim().parse().ok()?;
    let minor = it.next().unwrap_or("0").trim().parse().unwrap_or(0);
    let patch = it.next().unwrap_or("0").trim().parse().unwrap_or(0);
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim from `/System/Library/CoreServices/SystemVersion.plist` on a
    /// Tiger install (10.4.12, build 8SX170).
    const TIGER_PLIST: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>ProductBuildVersion</key>
	<string>8SX170</string>
	<key>ProductCopyright</key>
	<string>1983-2007 Apple Inc.</string>
	<key>ProductName</key>
	<string>Mac OS X</string>
	<key>ProductUserVisibleVersion</key>
	<string>10.4.12</string>
	<key>ProductVersion</key>
	<string>10.4.12</string>
</dict>
</plist>
"#;

    #[test]
    fn parses_tiger_plist() {
        assert_eq!(
            plist_string(TIGER_PLIST, "ProductVersion").as_deref(),
            Some("10.4.12")
        );
        assert_eq!(
            plist_string(TIGER_PLIST, "ProductBuildVersion").as_deref(),
            Some("8SX170")
        );
        assert_eq!(
            plist_string(TIGER_PLIST, "ProductName").as_deref(),
            Some("Mac OS X")
        );
        assert_eq!(plist_string(TIGER_PLIST, "NoSuchKey"), None);
    }

    #[test]
    fn release_parsing_handles_short_and_long_forms() {
        assert_eq!(parse_release("10.4.12"), Some((10, 4, 12)));
        assert_eq!(parse_release("10.5"), Some((10, 5, 0)));
        assert_eq!(parse_release("11"), Some((11, 0, 0)));
        assert_eq!(parse_release(""), None);
        assert_eq!(parse_release("not a version"), None);
    }

    #[test]
    fn at_least_compares_major_and_minor() {
        let tiger = HostVersion {
            version: parse_release("10.4.12"),
            ..HostVersion::default()
        };
        assert!(tiger.at_least(10, 4));
        assert!(tiger.at_least(10, 3));
        assert!(!tiger.at_least(10, 5));
        assert!(!tiger.at_least(11, 0));
    }

    /// An undetermined version must answer "no" to every capability question,
    /// so callers fall back to the older-OS path instead of assuming a facility
    /// that may not exist.
    #[test]
    fn at_least_is_false_when_version_is_unknown() {
        let unknown = HostVersion::default();
        assert!(!unknown.at_least(10, 4));
        assert!(!unknown.at_least(1, 0));
    }

    #[test]
    fn display_renders_name_release_build_and_arch() {
        let tiger = HostVersion {
            name: "Mac OS X".to_string(),
            release: "10.4.12".to_string(),
            build: "8SX170".to_string(),
            version: parse_release("10.4.12"),
        };
        let s = tiger.to_string();
        assert!(s.starts_with("Mac OS X 10.4.12 (build 8SX170, "), "{s}");
        assert!(s.ends_with(')'), "{s}");
    }

    #[test]
    fn display_survives_a_completely_unknown_host() {
        // Must still render something rather than panicking or printing empties.
        let s = HostVersion::default().to_string();
        assert!(!s.is_empty());
        assert!(s.contains(std::env::consts::OS));
    }

    #[test]
    fn detect_never_panics_and_names_something() {
        let h = HostVersion::detect();
        assert!(!h.to_string().is_empty());
    }
}
