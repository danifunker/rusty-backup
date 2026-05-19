//! Generate the Windows-friendly HTML help bundle from the clap definition.
//!
//! Run with `cargo run --example generate_cli_html_help -- OUT_DIR`,
//! defaulting to `docs/cli-html-help/`. Walks every subcommand
//! recursively and emits:
//!
//! - `index.html` — verb tree with descriptions, linked to per-verb pages
//! - `<verb>.html`, `<verb>-<sub>.html` — one page per leaf verb with
//!   usage, arguments, options, and breadcrumbs
//! - `style.css` — minimal styling that works in Windows Help Viewer,
//!   Edge, and offline Chrome
//!
//! The bundle is self-contained — no external assets, no JS — so it
//! ships well in installers (.zip, .msi, .iso) and renders the same
//! offline as online.

use clap::{Arg, ArgAction, Command, CommandFactory};
use rusty_backup::cli::Cli;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

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

fn main() -> std::io::Result<()> {
    let out_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "docs/cli-html-help".to_string());
    let out_dir = PathBuf::from(out_dir);
    fs::create_dir_all(&out_dir)?;

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
        emit_verb_pages(&out_dir, &bin, sub, &mut vec![]);
    }

    eprintln!("Wrote HTML help bundle to {}", out_dir.display());
    Ok(())
}

fn emit_verb_pages(out_dir: &std::path::Path, bin: &str, cmd: &Command, path: &mut Vec<String>) {
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
    let help = a
        .get_help()
        .map(|h| h.to_string())
        .or_else(|| a.get_long_help().map(|h| h.to_string()))
        .unwrap_or_default();
    writeln!(out, "<dt><code>{label}</code></dt>").unwrap();
    writeln!(out, "<dd>{}</dd>", html_escape(&help)).unwrap();
}

fn is_help_flag(a: &Arg) -> bool {
    a.get_id().as_str() == "help"
        || matches!(
            a.get_action(),
            ArgAction::Help | ArgAction::HelpLong | ArgAction::HelpShort | ArgAction::Version
        )
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
        "<footer>Auto-generated from the clap argument definitions in <code>src/cli/</code>. Re-run <code>cargo run --example generate_cli_html_help</code> after grammar changes. {} version reflects the binary built when this bundle was generated.</footer>\n",
        bin
    ));
    out.push_str("</body></html>\n");
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
