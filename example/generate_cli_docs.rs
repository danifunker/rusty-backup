//! One-shot, cross-platform regeneration of ALL rb-cli documentation
//! artifacts, from the clap definitions in `src/cli/`:
//!
//!   * `docs/cli-reference.md`  — the Markdown command reference
//!   * `docs/cli-html-help/`    — the offline HTML help bundle
//!
//! This is the pure-Rust replacement for the old `scripts/gen-cli-docs.sh`
//! (works identically on Windows / macOS / Linux) and is what the pre-commit
//! hook runs. Output paths are anchored to `CARGO_MANIFEST_DIR`, so it does
//! the right thing regardless of the working directory it's launched from.
//!
//! Run it with: `cargo run --example generate_cli_docs`

#[path = "cli_docs_common.rs"]
mod common;

use std::path::Path;

fn main() -> std::io::Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let reference = root.join("docs/cli-reference.md");
    let html_dir = root.join("docs/cli-html-help");

    // Prune first so pages for renamed/removed subcommands don't linger.
    common::prune_html_bundle(&html_dir)?;
    common::write_reference(&reference)?;
    common::write_html_bundle(&html_dir)?;

    eprintln!("gen-cli-docs: done");
    Ok(())
}
