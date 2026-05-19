//! Headless CLI entry point (`rb-cli`). The GUI binary (`rusty-backup`)
//! does not call into this; both bins share the parsing + handler code in
//! `rusty_backup::cli`.

use clap::Parser;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp_millis()
        .init();

    let cli = rusty_backup::cli::Cli::parse();
    let code = match rusty_backup::cli::run(cli) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("error: {e:#}");
            1
        }
    };
    std::process::exit(code);
}
