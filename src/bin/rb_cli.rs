//! Headless CLI entry point (`rb-cli`). The GUI binary (`rusty-backup`)
//! does not call into this; both bins share the parsing + handler code in
//! `rusty_backup::cli`.

use clap::{CommandFactory, FromArgMatches};

fn main() {
    // Note: `env_logger` is initialized inside `run()` once we've parsed
    // the global flags, so the user-supplied --log-level takes effect.
    //
    // Parsed through `ArgMatches` rather than `Cli::parse()` so the selected
    // `--format` can be recorded before dispatch; the error arm below needs it
    // and the parsed `Cli` no longer carries it centrally (R-005).
    let mut matches = rusty_backup::cli::Cli::command().get_matches();
    rusty_backup::cli::output::record_active_format(
        rusty_backup::cli::output::format_from_matches(&matches),
    );
    let cli = match rusty_backup::cli::Cli::from_arg_matches_mut(&mut matches) {
        Ok(cli) => cli,
        Err(e) => e.exit(),
    };

    let code = match rusty_backup::cli::run(cli) {
        Ok(()) => rusty_backup::cli::exit::SUCCESS,
        Err(e) => {
            // A caller who asked for JSON/YAML gets the failure in that shape on
            // stdout, as src/cli/output.rs has always documented. The plain-text
            // line still goes to stderr, which is the human channel.
            rusty_backup::cli::output::emit_error_envelope_for(&e);
            eprintln!("error: {e:#}");
            // Handlers that classified their failure keep that classification;
            // everything else is a generic failure as before.
            rusty_backup::cli::exit::code_for(&e)
        }
    };
    std::process::exit(code);
}
