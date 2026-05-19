//! Generate a Unix manpage for `rb-cli` from the clap definition.
//!
//! Run with `cargo run --example generate_manpage -- OUT_DIR`. Writes
//! `OUT_DIR/rb-cli.1`. Packagers wire this into their build pipeline;
//! the project's own packaging recipes will call it once the
//! release-engineering work in Phase H lands.

use clap::CommandFactory;
use rusty_backup::cli::Cli;

fn main() -> std::io::Result<()> {
    let out_dir = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());

    let cmd = Cli::command();
    let bin = cmd.get_name().to_string();
    let man = clap_mangen::Man::new(cmd);
    let mut buf = Vec::new();
    man.render(&mut buf)?;

    let path = std::path::Path::new(&out_dir).join(format!("{bin}.1"));
    std::fs::write(&path, &buf)?;
    eprintln!("Wrote {} ({} bytes)", path.display(), buf.len());
    Ok(())
}
