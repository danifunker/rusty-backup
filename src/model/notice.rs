//! User-facing progress and advisory notes from model code. The model never prints: a shell
//! installs a sink (the CLI sends notes to stderr, honouring `--quiet`); with none they are logged.

use std::sync::OnceLock;

static SINK: OnceLock<fn(&str)> = OnceLock::new();

/// Route every note to `sink`; the first call wins.
pub fn set_sink(sink: fn(&str)) {
    let _ = SINK.set(sink);
}

/// Report `msg` to whichever shell is listening.
pub fn emit(msg: impl AsRef<str>) {
    match SINK.get() {
        Some(sink) => sink(msg.as_ref()),
        None => log::info!("{}", msg.as_ref()),
    }
}
