//! Headless CLI entry point (`rb-cli`). The GUI binary (`rusty-backup`)
//! does not call into this; both bins share the parsing + handler code in
//! `rusty_backup::cli`.

use clap::Parser;

fn main() {
    // Note: `env_logger` is initialized inside `run()` once we've parsed
    // the global flags, so the user-supplied --log-level takes effect.
    let cli = rusty_backup::cli::Cli::parse();
    let code = match rusty_backup::cli::run(cli) {
        Ok(()) => rusty_backup::cli::exit::SUCCESS,
        Err(e) => {
            // Best-effort plain-text error. Verbs that need to surface
            // structured errors do so before bubbling here.
            eprintln!("error: {e:#}");
            // Handlers that classified their failure keep that classification;
            // everything else is a generic failure as before.
            rusty_backup::cli::exit::code_for(&e)
        }
    };
    std::process::exit(code);
}
