//! Generate `docs/cli-reference.md` from the clap definition.
//!
//! Run with `cargo run --example generate_cli_reference -- OUT_PATH`,
//! defaulting to `docs/cli-reference.md`. The rendering logic lives in
//! `cli_docs_common.rs`; to regenerate every doc artifact in one shot
//! (including the HTML bundle) use `generate_cli_docs`.

#[path = "cli_docs_common.rs"]
mod common;

use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let out_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "docs/cli-reference.md".to_string());
    common::write_reference(&PathBuf::from(out_path))
}
