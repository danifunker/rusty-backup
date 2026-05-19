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
