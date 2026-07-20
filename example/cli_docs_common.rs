//! Shared generation logic for the rb-cli documentation artifacts.
//!
//! Included (via `#[path = "cli_docs_common.rs"] mod common;`) by the three
//! generator examples:
//!
//! - `generate_cli_reference` — writes `docs/cli-reference.md`
//! - `generate_cli_html_help` — writes the `docs/cli-html-help/` bundle
//! - `generate_cli_docs`      — prunes the bundle, then writes both (the
//!   cross-platform, pure-Rust replacement for the old shell wrapper)
//!
//! This file has no `main()` and is not itself a registered example, so cargo
//! never builds it standalone — it exists only to be included.

#![allow(dead_code)] // each including example uses only part of the surface

use clap::{Arg, ArgAction, Command, CommandFactory};
use rusty_backup::cli::Cli;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

/// The canonical one-command way to regenerate everything. Referenced in the
/// generated footers so readers know how to refresh the docs.
pub const REGEN_HINT: &str = "cargo run --example generate_cli_docs";

// ---------------------------------------------------------------------------
// Markdown reference (docs/cli-reference.md)
// ---------------------------------------------------------------------------

/// Render the full Markdown reference and write it to `out_path`.
pub fn write_reference(out_path: &Path) -> std::io::Result<()> {
    let cmd = Cli::command();
    let bin = cmd.get_name().to_string();
    let mut md = String::new();

    writeln!(
        md,
        "# `{bin}` reference\n\n\
         _Auto-generated from the clap argument definitions in `src/cli/`.  \
         Re-run `{REGEN_HINT}` after grammar changes._\n"
    )
    .unwrap();

    writeln!(md, "## Synopsis\n").unwrap();
    writeln!(md, "```\n{}\n```\n", cmd.clone().render_usage()).unwrap();

    writeln!(md, "## Global options\n").unwrap();
    render_args_md(&mut md, &cmd);

    md.push_str(PATH_GRAMMAR);

    writeln!(md, "## Verbs\n").unwrap();
    let mut sub_path = Vec::new();
    render_subcommands_md(&mut md, &cmd, &mut sub_path);

    fs::write(out_path, &md)?;
    eprintln!("Wrote {} ({} bytes)", out_path.display(), md.len());
    Ok(())
}

/// Static section documenting how in-image paths are tokenised. Lives in the
/// generator (not a per-arg help string) because it applies uniformly to every
/// path-taking verb (`ls`, `get`, `get-binhex`, `put`, `put-binhex`, `mkdir`,
/// `rm`, `cp`, `locate`).
const PATH_GRAMMAR: &str = "\
## Path grammar (in-image paths)

Verbs that take a path *inside* an image (`ls`, `get`, `get-binhex`, `put`,
`put-binhex`, `mkdir`, `rm`, `cp`, `locate`) address it with one of two
grammars:

- **Slash** (default, every filesystem): `/` is the separator. A literal `/`
  inside a single name — legal on classic-Mac HFS / HFS+ volumes, e.g.
  `Oxyd b/w` — is written `\\/`; a literal backslash is written `\\\\`. So
  `rb-cli get-binhex IMG \"/Games/Oxyd 3.6/Oxyd b\\/w\" out.hqx` extracts the
  single file `Oxyd b/w` from the folder `Oxyd 3.6`.
- **Colon** (HFS / HFS+ only): because classic Mac OS reserves `:` as its path
  separator, `:` can never appear in a name, so you may instead write the path
  with `:` separators — the native Mac convention — and then `/` is ordinary
  data needing no escape: `rb-cli get-binhex IMG \":Games:Oxyd 3.6:Oxyd b/w\"
  out.hqx`. A colon-grammar path is always literal (it never globs).

On every other filesystem `:` is an ordinary filename byte and only the slash
grammar applies. Glob patterns (`*`, `?`, `[`, `{`) use the slash grammar; pass
`--literal` (or use the colon grammar) to address a name containing those
characters verbatim.

";

fn render_subcommands_md(out: &mut String, cmd: &Command, path: &mut Vec<String>) {
    let mut subs: Vec<&Command> = cmd.get_subcommands().collect();
    subs.sort_by_key(|c| c.get_name());
    for sub in subs {
        if sub.get_name() == "help" {
            continue;
        }
        path.push(sub.get_name().to_string());
        let heading = path.join(" ");
        writeln!(out, "### `{heading}`\n").unwrap();
        if let Some(about) = sub.get_about() {
            writeln!(out, "{about}\n").unwrap();
        }
        writeln!(out, "```\n{}\n```\n", sub.clone().render_usage()).unwrap();
        render_args_md(out, sub);
        render_subcommands_md(out, sub, path);
        path.pop();
    }
}

fn render_args_md(out: &mut String, cmd: &Command) {
    let positionals: Vec<&Arg> = cmd.get_positionals().collect();
    let flags: Vec<&Arg> = cmd
        .get_arguments()
        .filter(|a| !a.is_positional())
        .filter(|a| !is_help_flag(a))
        .collect();

    if !positionals.is_empty() {
        writeln!(out, "**Arguments**\n").unwrap();
        for a in &positionals {
            write_arg_md(out, a);
        }
        writeln!(out).unwrap();
    }
    if !flags.is_empty() {
        writeln!(out, "**Options**\n").unwrap();
        for a in &flags {
            write_arg_md(out, a);
        }
        writeln!(out).unwrap();
    }
}

fn write_arg_md(out: &mut String, a: &Arg) {
    let name = a.get_id().as_str();
    let long = a.get_long().map(|s| format!("--{s}"));
    let short = a.get_short().map(|c| format!("-{c}"));
    let label = if a.is_positional() {
        format!("`<{}>`", name.to_uppercase())
    } else {
        match (short, long) {
            (Some(s), Some(l)) => format!("`{s}` / `{l}`"),
            (None, Some(l)) => format!("`{l}`"),
            (Some(s), None) => format!("`{s}`"),
            _ => format!("`{name}`"),
        }
    };
    let help = arg_help(a);
    writeln!(out, "- {label} — {help}").unwrap();
}

// ---------------------------------------------------------------------------
// HTML help bundle (docs/cli-html-help/)
// ---------------------------------------------------------------------------

const STYLE_CSS: &str = r#"
body { font-family: 'Segoe UI', system-ui, sans-serif; max-width: 900px; margin: 2em auto; padding: 0 1em; color: #1f2328; }
h1, h2, h3 { color: #1f2328; }
h1 { border-bottom: 1px solid #d0d7de; padding-bottom: 0.3em; }
code, pre { font-family: Consolas, 'Cascadia Code', monospace; }
pre { background: #f6f8fa; padding: 1em; border-radius: 6px; overflow-x: auto; }
code { background: #f6f8fa; padding: 1px 4px; border-radius: 3px; }
pre code { background: none; padding: 0; }
nav.breadcrumb { font-size: 0.9em; color: #57606a; margin-bottom: 1em; }
nav.breadcrumb a { color: #0969da; text-decoration: none; }
nav.breadcrumb a:hover { text-decoration: underline; }
ul.verbs { list-style: none; padding-left: 0; }
ul.verbs li { padding: 0.4em 0; border-bottom: 1px solid #eee; }
ul.verbs a { color: #0969da; text-decoration: none; font-weight: 600; }
ul.verbs a:hover { text-decoration: underline; }
ul.verbs .desc { color: #57606a; font-weight: normal; margin-left: 0.5em; }
dl.args dt { font-family: Consolas, monospace; margin-top: 0.6em; color: #0a3069; }
dl.args dd { margin: 0 0 0.4em 1.5em; }
footer { margin-top: 3em; padding-top: 1em; border-top: 1px solid #d0d7de; font-size: 0.85em; color: #57606a; }
"#;

/// Remove the previously-generated HTML pages and stylesheet so pages for
/// renamed/removed subcommands don't linger — the writer only ever adds files.
/// The generator fully owns this directory (every `*.html` plus `style.css` is
/// rewritten by [`write_html_bundle`]), so clearing it is safe and keeps the
/// output deterministic. A missing directory is a no-op.
pub fn prune_html_bundle(out_dir: &Path) -> std::io::Result<()> {
    if !out_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(out_dir)? {
        let path = entry?.path();
        let is_generated = path.extension().and_then(|e| e.to_str()) == Some("html")
            || path.file_name().and_then(|n| n.to_str()) == Some("style.css");
        if is_generated {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Render the full HTML help bundle (index + per-verb pages + stylesheet) into
/// `out_dir`, creating it if needed.
pub fn write_html_bundle(out_dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(out_dir)?;
    fs::write(out_dir.join("style.css"), STYLE_CSS.trim_start())?;

    let cmd = Cli::command();
    let bin = cmd.get_name().to_string();

    // Index page
    let mut index = String::new();
    write_html_header(&mut index, &format!("{bin} — command-line help"), &[]);
    writeln!(index, "<h1><code>{bin}</code> command-line help</h1>").unwrap();
    if let Some(about) = cmd.get_about() {
        writeln!(index, "<p>{}</p>", html_escape(&about.to_string())).unwrap();
    }
    writeln!(index, "<h2>Global options</h2>").unwrap();
    render_args_html(&mut index, &cmd);

    writeln!(index, "<h2>Verbs</h2>").unwrap();
    writeln!(index, "<ul class=\"verbs\">").unwrap();
    let mut subs: Vec<&Command> = cmd.get_subcommands().collect();
    subs.sort_by_key(|c| c.get_name());
    for sub in &subs {
        if sub.get_name() == "help" {
            continue;
        }
        let name = sub.get_name();
        let about = sub.get_about().map(|a| a.to_string()).unwrap_or_default();
        writeln!(
            index,
            "<li><a href=\"{name}.html\"><code>{name}</code></a><span class=\"desc\">{}</span></li>",
            html_escape(&about)
        )
        .unwrap();
    }
    writeln!(index, "</ul>").unwrap();
    write_html_footer(&mut index, &bin);
    fs::write(out_dir.join("index.html"), index)?;

    // Per-verb pages
    for sub in subs {
        if sub.get_name() == "help" {
            continue;
        }
        emit_verb_pages(out_dir, &bin, sub, &mut vec![]);
    }

    eprintln!("Wrote HTML help bundle to {}", out_dir.display());
    Ok(())
}

fn emit_verb_pages(out_dir: &Path, bin: &str, cmd: &Command, path: &mut Vec<String>) {
    path.push(cmd.get_name().to_string());
    let filename = format!("{}.html", path.join("-"));
    let breadcrumb = build_breadcrumb(bin, path);

    let mut page = String::new();
    write_html_header(
        &mut page,
        &format!("{} {}", bin, path.join(" ")),
        &breadcrumb,
    );

    writeln!(page, "<h1><code>{} {}</code></h1>", bin, path.join(" ")).unwrap();
    if let Some(about) = cmd.get_about() {
        writeln!(page, "<p>{}</p>", html_escape(&about.to_string())).unwrap();
    }
    if let Some(long) = cmd.get_long_about() {
        let long = long.to_string();
        if !long.is_empty() {
            writeln!(
                page,
                "<p>{}</p>",
                html_escape(&long).replace("\n\n", "</p><p>")
            )
            .unwrap();
        }
    }

    writeln!(page, "<h2>Usage</h2>").unwrap();
    writeln!(
        page,
        "<pre><code>{}</code></pre>",
        html_escape(&cmd.clone().render_usage().to_string())
    )
    .unwrap();

    render_args_html(&mut page, cmd);

    let subs: Vec<&Command> = cmd.get_subcommands().collect();
    let real_subs: Vec<&&Command> = subs.iter().filter(|s| s.get_name() != "help").collect();
    if !real_subs.is_empty() {
        writeln!(page, "<h2>Subcommands</h2>").unwrap();
        writeln!(page, "<ul class=\"verbs\">").unwrap();
        for sub in &real_subs {
            let sub_path = {
                let mut p = path.clone();
                p.push(sub.get_name().to_string());
                p.join("-")
            };
            let about = sub.get_about().map(|a| a.to_string()).unwrap_or_default();
            writeln!(
                page,
                "<li><a href=\"{sub_path}.html\"><code>{}</code></a><span class=\"desc\">{}</span></li>",
                sub.get_name(),
                html_escape(&about)
            )
            .unwrap();
        }
        writeln!(page, "</ul>").unwrap();
    }

    write_html_footer(&mut page, bin);
    fs::write(out_dir.join(filename), page).expect("write verb page");

    for sub in cmd.get_subcommands() {
        if sub.get_name() == "help" {
            continue;
        }
        emit_verb_pages(out_dir, bin, sub, path);
    }

    path.pop();
}

fn build_breadcrumb(bin: &str, path: &[String]) -> Vec<(String, String)> {
    let mut crumbs = vec![("index.html".to_string(), bin.to_string())];
    let mut acc: Vec<String> = Vec::new();
    for p in path {
        acc.push(p.clone());
        crumbs.push((format!("{}.html", acc.join("-")), p.clone()));
    }
    crumbs
}

fn render_args_html(out: &mut String, cmd: &Command) {
    let positionals: Vec<&Arg> = cmd.get_positionals().collect();
    let flags: Vec<&Arg> = cmd
        .get_arguments()
        .filter(|a| !a.is_positional())
        .filter(|a| !is_help_flag(a))
        .collect();

    if !positionals.is_empty() {
        writeln!(out, "<h2>Arguments</h2>").unwrap();
        writeln!(out, "<dl class=\"args\">").unwrap();
        for a in &positionals {
            write_arg_html(out, a);
        }
        writeln!(out, "</dl>").unwrap();
    }
    if !flags.is_empty() {
        writeln!(out, "<h2>Options</h2>").unwrap();
        writeln!(out, "<dl class=\"args\">").unwrap();
        for a in &flags {
            write_arg_html(out, a);
        }
        writeln!(out, "</dl>").unwrap();
    }
}

fn write_arg_html(out: &mut String, a: &Arg) {
    let name = a.get_id().as_str();
    let label = if a.is_positional() {
        format!("&lt;{}&gt;", name.to_uppercase())
    } else {
        let long = a.get_long().map(|s| format!("--{s}"));
        let short = a.get_short().map(|c| format!("-{c}"));
        match (short, long) {
            (Some(s), Some(l)) => format!("{s} / {l}"),
            (None, Some(l)) => l,
            (Some(s), None) => s,
            _ => name.to_string(),
        }
    };
    let help = arg_help(a);
    writeln!(out, "<dt><code>{label}</code></dt>").unwrap();
    writeln!(out, "<dd>{}</dd>", html_escape(&help)).unwrap();
}

fn write_html_header(out: &mut String, title: &str, breadcrumbs: &[(String, String)]) {
    out.push_str("<!DOCTYPE html>\n<html lang=\"en\"><head>\n");
    out.push_str("<meta charset=\"UTF-8\">\n");
    out.push_str(&format!("<title>{}</title>\n", html_escape(title)));
    out.push_str("<link rel=\"stylesheet\" href=\"style.css\">\n");
    out.push_str("</head>\n<body>\n");
    if !breadcrumbs.is_empty() {
        out.push_str("<nav class=\"breadcrumb\">");
        for (i, (href, label)) in breadcrumbs.iter().enumerate() {
            if i + 1 == breadcrumbs.len() {
                out.push_str(&format!("<span>{}</span>", html_escape(label)));
            } else {
                out.push_str(&format!(
                    "<a href=\"{}\">{}</a> &raquo; ",
                    href,
                    html_escape(label)
                ));
            }
        }
        out.push_str("</nav>\n");
    }
}

fn write_html_footer(out: &mut String, bin: &str) {
    out.push_str(&format!(
        "<footer>Auto-generated from the clap argument definitions in <code>src/cli/</code>. Re-run <code>{REGEN_HINT}</code> after grammar changes. {bin} version reflects the binary built when this bundle was generated.</footer>\n",
    ));
    out.push_str("</body></html>\n");
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn is_help_flag(a: &Arg) -> bool {
    a.get_id().as_str() == "help"
        || matches!(
            a.get_action(),
            ArgAction::Help | ArgAction::HelpLong | ArgAction::HelpShort | ArgAction::Version
        )
}

fn arg_help(a: &Arg) -> String {
    a.get_help()
        .map(|h| h.to_string())
        .or_else(|| a.get_long_help().map(|h| h.to_string()))
        .unwrap_or_default()
}
