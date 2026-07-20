//! Generate the Windows-friendly HTML help bundle from the clap definition.
//!
//! Run with `cargo run --example generate_cli_html_help -- OUT_DIR`,
//! defaulting to `docs/cli-html-help/`. Prunes stale pages, then writes
//! `index.html`, one `<verb>.html` per leaf verb, and `style.css`. The
//! rendering logic lives in `cli_docs_common.rs`; to regenerate every doc
//! artifact in one shot use `generate_cli_docs`.

#[path = "cli_docs_common.rs"]
mod common;

use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let out_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "docs/cli-html-help".to_string());
    let out_dir = PathBuf::from(out_dir);
    common::prune_html_bundle(&out_dir)?;
    common::write_html_bundle(&out_dir)
}
