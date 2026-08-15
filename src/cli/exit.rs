//! Exit-code table for `rb-cli`. The numbers are part of the public CLI
//! contract — scripts may switch on them. Keep this list synchronized
//! with `docs/cli-todo.md` § "Exit codes".

/// Operation completed successfully.
pub const SUCCESS: i32 = 0;

/// Generic operation failure (I/O, parse error, fsck-checkonly found
/// issues, partial backup kept on Ctrl-C, etc.). Used as the catch-all
/// when no more specific code applies.
pub const GENERIC_FAILURE: i32 = 1;

/// Usage / syntax error (bad flag, unknown verb). Returned by clap when
/// argument parsing fails; reserved here for handler-side
/// usage-bad-input branches.
pub const USAGE_ERROR: i32 = 2;

/// Resource not found — image file missing, partition index out of
/// range, path inside the filesystem doesn't exist, etc.
pub const NOT_FOUND: i32 = 3;

/// Permission denied / needs elevation. Examples: opening a raw block
/// device without root, hitting a read-only filesystem on a write path.
pub const PERMISSION_DENIED: i32 = 4;

/// User declined an interactive prompt, or a prompt timed out to No
/// without a tty.
pub const USER_DECLINED: i32 = 5;

/// SIGINT (Ctrl-C). Shell convention is 128 + signal number.
pub const SIGINT: i32 = 130;

/// An error that names the exit code it should produce.
///
/// Every handler error used to arrive at `main` as a bare `anyhow::Error` and
/// leave as [`GENERIC_FAILURE`], so a message could say "usage error" while the
/// process said 1 — which is what scripts actually switch on (R-004). Wrapping
/// the message in this type carries the code the whole way out.
#[derive(Debug)]
pub struct CodedError {
    pub code: i32,
    pub message: String,
}

impl std::fmt::Display for CodedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CodedError {}

/// A usage rejection: bad input a handler recognises as the caller's mistake.
pub fn usage(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(CodedError {
        code: USAGE_ERROR,
        message: message.into(),
    })
}

/// A named thing does not exist: an image file, a partition index, a path
/// inside a filesystem.
pub fn not_found(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(CodedError {
        code: NOT_FOUND,
        message: message.into(),
    })
}

/// A refusal for lack of permission, including writing to a read-only target.
pub fn permission_denied(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(CodedError {
        code: PERMISSION_DENIED,
        message: message.into(),
    })
}

/// The exit code an error asks for, or [`GENERIC_FAILURE`].
///
/// Walks the whole `anyhow` chain, so adding `.context(..)` to a coded error
/// does not silently downgrade it back to 1.
pub fn code_for(err: &anyhow::Error) -> i32 {
    err.chain()
        .find_map(|e| e.downcast_ref::<CodedError>())
        .map(|c| c.code)
        .unwrap_or(GENERIC_FAILURE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_error_is_a_generic_failure() {
        assert_eq!(code_for(&anyhow::anyhow!("boom")), GENERIC_FAILURE);
    }

    #[test]
    fn a_usage_error_keeps_its_code_under_context() {
        let e = usage("bad flag");
        assert_eq!(code_for(&e), USAGE_ERROR);
        // The chain walk is the point: context is added all over the CLI.
        let wrapped = e.context("while doing the thing");
        assert_eq!(code_for(&wrapped), USAGE_ERROR);
        assert!(format!("{wrapped:#}").contains("bad flag"));
    }

    #[test]
    fn permission_denied_is_four() {
        assert_eq!(code_for(&permission_denied("read-only")), PERMISSION_DENIED);
    }
}
