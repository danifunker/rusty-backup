//! `rb-regress` — the Rusty Backup regression harness.
//!
//! Drives the shipped `rb-cli` binary through a declarative matrix of cases
//! and produces a report bundle. See `regression-tests/README.md`.
//!
//! The prime directive: **report, never abort.** A broken case is data. The
//! run always reaches the end, and every case resolves to exactly one of the
//! seven verdicts in [`report::Verdict`].

mod assertion;
mod consolidate;
mod envelope;
mod exec;
mod fixtures;
mod gitinfo;
mod manifest;
mod parity;
mod plan;
mod produce;
mod registry;
mod report;
mod verify;

use report::{Bundle, CaseResult, RunIdentity, Verdict};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_TIMEOUT_MS: u64 = 120_000;

struct Args {
    command: Command,
    cases_dir: PathBuf,
    rb_cli: PathBuf,
    fixture_root: Option<PathBuf>,
    report_root: PathBuf,
    tiers: BTreeSet<u8>,
    filter: Option<String>,
    allow_hardware: bool,
    keep_scratch: bool,
    require_clean: bool,
    check: bool,
    scratch_root: PathBuf,
    artifacts_root: PathBuf,
    verifications_root: PathBuf,
    db: Option<PathBuf>,
}

enum Command {
    Run,
    List,
    Validate,
    /// Write the normalised JSON snapshot of the registry.
    Export,
    /// Ask the registry a question by name.
    Query(String),
    /// Map requirements onto the machines that exist.
    Plan,
    /// Build every artifact rb-cli can write, on whatever OS is running.
    Produce,
    /// Compare artifacts produced on different OSes. Needs no oracle.
    Parity(String),
    /// Hand produced artifacts to whatever oracles this host has.
    Verify,
    /// Merge results from many hosts/runs and report how far a regression got.
    Consolidate(String),
    Help,
}

/// Repository root, so the catalogue can resolve `repo:` rows against the
/// ~4 MB of fixtures already committed under `tests/fixtures/`. Those cost
/// nothing extra on any machine — they arrive with the clone.
fn repo_root() -> Option<PathBuf> {
    regression_dir().parent().map(|p| p.to_path_buf())
}

/// A bare `consolidate` with no path should not swallow the next flag.
fn root_is_flag(s: &str) -> bool {
    s.is_empty() || s.starts_with('-')
}

fn cmd_consolidate(args: &Args, root: &str) -> i32 {
    let root = if root_is_flag(root) {
        args.report_root.clone()
    } else {
        PathBuf::from(root)
    };
    match consolidate::consolidate(&root) {
        Ok(c) => {
            print!("{}", consolidate::render(&c, &root));
            0
        }
        Err(e) => {
            eprintln!("error: {}", e);
            2
        }
    }
}

fn regression_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR is regression-tests/runner; the suite lives one up.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn parse_args() -> Result<Args, String> {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let base = regression_dir();

    let mut args = Args {
        command: Command::Help,
        cases_dir: base.join("cases"),
        rb_cli: default_rb_cli(&base),
        fixture_root: None,
        report_root: base.join("runs"),
        tiers: BTreeSet::new(),
        filter: None,
        allow_hardware: false,
        keep_scratch: false,
        require_clean: false,
        check: false,
        scratch_root: base.join("scratch"),
        artifacts_root: base.join("artifacts"),
        verifications_root: base.join("verifications"),
        db: None,
    };

    let mut i = 0;
    while i < raw.len() {
        let a = raw[i].as_str();
        match a {
            "run" => args.command = Command::Run,
            "list" => args.command = Command::List,
            "validate" => args.command = Command::Validate,
            "plan" => args.command = Command::Plan,
            "produce" => args.command = Command::Produce,
            "verify" => args.command = Command::Verify,
            "parity" => {
                let root = raw.get(i + 1).cloned().unwrap_or_default();
                if !root_is_flag(&root) {
                    i += 1;
                }
                args.command = Command::Parity(root);
            }
            "consolidate" => {
                let root = raw.get(i + 1).cloned().unwrap_or_default();
                if !root_is_flag(&root) {
                    i += 1;
                }
                args.command = Command::Consolidate(root);
            }
            "export" => args.command = Command::Export,
            "query" => {
                let q = raw.get(i + 1).cloned().unwrap_or_default();
                args.command = Command::Query(q);
                i += 1;
            }
            "-h" | "--help" | "help" => args.command = Command::Help,
            "--allow-hardware" => args.allow_hardware = true,
            "--keep-scratch" => args.keep_scratch = true,
            "--require-clean" => args.require_clean = true,
            "--check" => args.check = true,
            _ => {
                let value = || -> Result<String, String> {
                    raw.get(i + 1)
                        .cloned()
                        .ok_or_else(|| format!("{} needs a value", a))
                };
                match a {
                    "--cases" => {
                        args.cases_dir = PathBuf::from(value()?);
                        i += 1;
                    }
                    "--rb-cli" => {
                        args.rb_cli = PathBuf::from(value()?);
                        i += 1;
                    }
                    "--fixture-root" => {
                        args.fixture_root = Some(PathBuf::from(value()?));
                        i += 1;
                    }
                    "--report-root" => {
                        args.report_root = PathBuf::from(value()?);
                        i += 1;
                    }
                    "--scratch-root" => {
                        args.scratch_root = PathBuf::from(value()?);
                        i += 1;
                    }
                    "--artifacts" => {
                        args.artifacts_root = PathBuf::from(value()?);
                        i += 1;
                    }
                    "--verifications" => {
                        args.verifications_root = PathBuf::from(value()?);
                        i += 1;
                    }
                    "--filter" => {
                        args.filter = Some(value()?);
                        i += 1;
                    }
                    "--db" => {
                        args.db = Some(PathBuf::from(value()?));
                        i += 1;
                    }
                    "--tiers" => {
                        args.tiers = parse_tiers(&value()?)?;
                        i += 1;
                    }
                    other => return Err(format!("unknown argument: {}", other)),
                }
            }
        }
        i += 1;
    }

    // Cases and recipes all run with a scratch directory as cwd, so anything
    // that becomes a program path or is handed to a subprocess must be
    // absolute before it gets there. See exec::absolutise.
    args.rb_cli = exec::absolutise(&args.rb_cli);
    args.scratch_root = exec::absolutise(&args.scratch_root);
    args.artifacts_root = exec::absolutise(&args.artifacts_root);
    args.verifications_root = exec::absolutise(&args.verifications_root);

    Ok(args)
}

/// `--tiers 0-6`, `--tiers 0,1,5`, `--tiers 2`.
fn parse_tiers(spec: &str) -> Result<BTreeSet<u8>, String> {
    let mut out = BTreeSet::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = part.split_once('-') {
            let lo: u8 = lo
                .trim()
                .parse()
                .map_err(|_| format!("bad tier: {}", part))?;
            let hi: u8 = hi
                .trim()
                .parse()
                .map_err(|_| format!("bad tier: {}", part))?;
            if lo > hi {
                return Err(format!("bad tier range: {}", part));
            }
            for t in lo..=hi {
                out.insert(t);
            }
        } else {
            out.insert(part.parse().map_err(|_| format!("bad tier: {}", part))?);
        }
    }
    Ok(out)
}

fn usage() {
    println!(
        r#"rb-regress — Rusty Backup regression harness

USAGE:
    rb-regress <run|list|validate> [OPTIONS]

COMMANDS:
    run          Execute the matrix and write a report bundle
    list         List the cases that would run, without running them
    validate     Parse every manifest and report problems; runs nothing
    plan         Map requirements onto the machines that exist
    produce      Build every artifact rb-cli can write, twice, into <artifacts>/<os>
    parity       Compare artifacts across producer OSes; needs no oracle
    verify       Run this host's oracles over the artifact tree
    consolidate  Merge results from many hosts/runs; reports how far a regression got
    export       Write the normalised JSON snapshot of the registry
    query        Ask the registry a named question

OPTIONS:
    --cases <DIR>          Case manifests           [default: regression-tests/cases]
    --rb-cli <PATH>        Binary under test        [default: <repo>/target/release/rb-cli]
                           Always resolved to an absolute path. run and produce
                           print it with its version and refuse to start if it
                           is missing; they warn if it is not a release build or
                           is older than the sources in the tree.
    --fixture-root <DIR>   Fixture corpus root      [or RB_FIXTURE_ROOT, or local.toml]
    --report-root <DIR>    Where bundles are written[default: regression-tests/runs]
    --scratch-root <DIR>   Working directory root   [default: regression-tests/scratch]
    --artifacts <DIR>      Artifact tree root       [default: regression-tests/artifacts]
                           produce writes <DIR>/<os>; parity and verify read <DIR>
    --verifications <DIR>  Verdict tree root        [default: regression-tests/verifications]
                           verify writes <DIR>/<os>
    --tiers <SPEC>         e.g. 0-6, or 0,1,5       [default: all]
    --filter <SUBSTR>      Only cases whose ID contains SUBSTR
    --allow-hardware       Permit cases that write to physical devices
    --keep-scratch         Keep scratch dirs for passing cases too
    --require-clean        Refuse to run on a dirty tree, so results can be
                           attributed to a commit
    --check                (export) verify the snapshot is current; write nothing

Fixture IDs that cannot be resolved are reported as skip-fixture and written
to the run's shopping list. They are never failures. See FIXTURES.md."#
    );
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {}", e);
            eprintln!("try `rb-regress --help`");
            std::process::exit(2);
        }
    };

    let code = match args.command {
        Command::Help => {
            usage();
            0
        }
        Command::Validate => cmd_validate(&args),
        Command::List => cmd_list(&args),
        Command::Run => cmd_run(&args),
        Command::Export => cmd_export(&args),
        Command::Query(ref q) => cmd_query(q),
        Command::Plan => cmd_plan(&args),
        Command::Produce => cmd_produce(&args),
        Command::Parity(ref root) => cmd_parity(&args, root),
        Command::Verify => cmd_verify(&args),
        Command::Consolidate(ref root) => cmd_consolidate(&args, root),
    };
    std::process::exit(code);
}

/// `produce` writes into `<artifacts>/<platform>`, so every host can fill the
/// same tree without coordinating and the result is still attributable.
fn cmd_produce(args: &Args) -> i32 {
    let base = regression_dir();
    let recipes = match produce::load_recipes(&base.join("data").join("produce.toml")) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    };
    let reg = match registry::Registry::load(&base) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    };
    let known: std::collections::BTreeMap<String, String> = reg
        .formats
        .iter()
        .map(|f| (f.id.clone(), f.name.clone()))
        .collect();
    let builders: std::collections::BTreeMap<String, String> = reg
        .formats
        .iter()
        .filter_map(|f| f.builder.clone().map(|b| (f.id.clone(), b)))
        .collect();

    let repo = repo_root().unwrap_or_else(|| PathBuf::from("."));
    let git_sha = gitinfo::head_sha(&repo).unwrap_or_default();
    let dirty = gitinfo::dirty_files(&repo).unwrap_or_default();
    if args.require_clean && !dirty.is_empty() {
        eprintln!("error: working tree is dirty; refusing to produce with --require-clean");
        eprintln!("  artifacts outlive the run that made them, so one built from an");
        eprintln!("  uncommitted tree can never be traced back to a build.");
        return 2;
    }
    match preflight(&args.rb_cli, &repo) {
        Ok(banner) => print!("{}", banner),
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    }
    let rb_version = probe_version(&args.rb_cli).unwrap_or_else(|| "unknown".to_string());
    let build_label = gitinfo::build_label(&repo, &rb_version);

    let out_root = args.artifacts_root.join(exec::platform_token());
    let scratch = args.scratch_root.join("produce");
    let report = match produce::produce(
        &args.rb_cli,
        &recipes,
        &known,
        &builders,
        &out_root,
        &scratch,
        &hostname(),
        &build_label,
        &git_sha,
        args.filter.as_deref(),
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    };

    print!("{}", produce::render(&report, &out_root));
    // A recipe that cannot build its artifact is a finding, so the exit code
    // has to say so — a green produce is what lets `parity` and `verify` trust
    // the tree.
    let failed = report
        .outcomes
        .iter()
        .filter(|(_, o)| matches!(o, produce::Outcome::Failed { .. }))
        .count();
    if failed > 0 {
        1
    } else {
        0
    }
}

/// Verification is per-verifier, so this host writes only into its own
/// directory and several machines can fill one tree with no coordination.
fn cmd_verify(args: &Args) -> i32 {
    let reg = match registry::Registry::load(&regression_dir()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    };
    let out_dir = args
        .verifications_root
        .join(exec::platform_token());
    let report = match verify::verify(
        &reg,
        &args.artifacts_root,
        &out_dir,
        &hostname(),
        args.filter.as_deref(),
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    };
    print!("{}", verify::render(&report, &out_dir));
    let bad = report
        .records
        .iter()
        .filter(|r| matches!(r.verdict, verify::Verdict::Fail { .. }))
        .count();
    if bad > 0 {
        1
    } else {
        0
    }
}

fn cmd_parity(args: &Args, root: &str) -> i32 {
    let root = if root_is_flag(root) {
        args.artifacts_root.clone()
    } else {
        PathBuf::from(root)
    };
    match parity::parity(&root) {
        Ok(r) => {
            print!("{}", parity::render(&r));
            let bad = r
                .comparisons
                .iter()
                .filter(|c| {
                    matches!(
                        c.verdict,
                        parity::Verdict::Differ { .. } | parity::Verdict::SizeDiffer { .. }
                    )
                })
                .count();
            if bad > 0 {
                1
            } else {
                0
            }
        }
        Err(e) => {
            eprintln!("error: {}", e);
            2
        }
    }
}

fn cmd_plan(_args: &Args) -> i32 {
    match plan::build_plan(&regression_dir()) {
        Ok(p) => {
            print!("{}", plan::render(&p));
            0
        }
        Err(e) => {
            eprintln!("error: {}", e);
            2
        }
    }
}

fn export_path(args: &Args) -> PathBuf {
    args.db
        .clone()
        .unwrap_or_else(|| regression_dir().join("data").join("regression.json"))
}

fn cmd_export(args: &Args) -> i32 {
    let reg = match registry::Registry::load(&regression_dir()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    };
    let json = match reg.export_json() {
        Ok(j) => j,
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    };
    let path = export_path(args);

    // --check verifies the committed snapshot is current WITHOUT writing.
    // The export is generated but tracked, so a stale copy would otherwise
    // dirty the tree and block a --require-clean run. This is the usual
    // codegen treatment: regenerate, compare, report.
    if args.check {
        let current = fs::read_to_string(&path).unwrap_or_default();
        let want = json
            + "
";
        if current == want {
            println!("{} is up to date", path.display());
            return 0;
        }
        eprintln!(
            "error: {} is stale — run `rb-regress export`",
            path.display()
        );
        return 1;
    }

    if let Some(p) = path.parent() {
        let _ = fs::create_dir_all(p);
    }
    if let Err(e) = fs::write(
        &path,
        json + "
",
    ) {
        eprintln!("error: writing {}: {}", path.display(), e);
        return 2;
    }
    println!("wrote {}", path.display());
    for (k, v) in reg.counts() {
        println!("  {:<14} {}", k, v);
    }
    for w in &reg.warnings {
        println!("  warning: {}", w);
    }
    0
}

fn cmd_query(name: &str) -> i32 {
    const NAMES: &[&str] = &[
        "unverified-writes",
        "unfixtured-reads",
        "platform-pins",
        "counts",
        "fixtures",
    ];
    if name.is_empty() {
        println!("queries: {}", NAMES.join(", "));
        return 0;
    }
    let reg = match registry::Registry::load(&regression_dir()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    };
    match name {
        "unverified-writes" => {
            for f in reg.unverified_writes() {
                println!(
                    "{:<22} {:<10} {}",
                    f.id,
                    f.kind,
                    f.builder.as_deref().unwrap_or("")
                );
            }
        }
        "unfixtured-reads" => {
            for f in reg.unfixtured_reads() {
                println!("{:<22} {:<10} {}", f.id, f.kind, f.name);
            }
        }
        "platform-pins" => {
            let mut by_plat: std::collections::BTreeMap<String, usize> =
                std::collections::BTreeMap::new();
            for (fmt, dir, plat) in reg.platform_pins() {
                println!("{:<22} {:<6} {}", fmt, dir, plat);
                *by_plat.entry(plat).or_insert(0) += 1;
            }
            println!();
            for (p, n) in by_plat {
                println!("  {:<14} {}", p, n);
            }
        }
        "counts" => {
            for (k, v) in reg.counts() {
                println!("{:<14} {}", k, v);
            }
        }
        "fixtures" => {
            let mut by_loc: std::collections::BTreeMap<&str, (usize, u64)> =
                std::collections::BTreeMap::new();
            for f in &reg.fixtures {
                let e = by_loc.entry(f.location.as_str()).or_insert((0, 0));
                e.0 += 1;
                e.1 += f.bytes;
            }
            for (loc, (n, b)) in by_loc {
                println!(
                    "{:<10} {:>4} fixtures  {:>8.1} MB",
                    loc,
                    n,
                    b as f64 / 1048576.0
                );
            }
        }
        other => {
            eprintln!(
                "unknown query '{}'; try one of: {}",
                other,
                NAMES.join(", ")
            );
            return 2;
        }
    }
    0
}

fn cmd_validate(args: &Args) -> i32 {
    let (manifests, errors) = manifest::load_dir(&args.cases_dir);
    let total: usize = manifests.iter().map(|(_, m)| m.cases.len()).sum();
    println!(
        "{} manifest(s), {} case(s), {} problem(s)",
        manifests.len(),
        total,
        errors.len()
    );

    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut dupes = Vec::new();
    for (_, m) in &manifests {
        for c in &m.cases {
            if !seen.insert(c.id.clone()) {
                dupes.push(c.id.clone());
            }
        }
    }

    for e in &errors {
        println!("  problem: {}", e);
    }
    for d in &dupes {
        println!("  problem: duplicate case id '{}'", d);
    }

    if errors.is_empty() && dupes.is_empty() {
        0
    } else {
        1
    }
}

fn cmd_list(args: &Args) -> i32 {
    let (manifests, errors) = manifest::load_dir(&args.cases_dir);
    for e in &errors {
        eprintln!("warning: {}", e);
    }
    let platform = exec::platform_token();
    let mut n = 0;
    for (_, m) in &manifests {
        for c in &m.cases {
            let tier = c.tier.unwrap_or(m.meta.tier);
            if !selected(args, &c.id, tier) {
                continue;
            }
            if !c.platforms.is_empty() && !c.platforms.iter().any(|p| p == platform) {
                continue;
            }
            n += 1;
            println!(
                "[{}] {:<50} {}",
                tier,
                c.id,
                c.description.as_deref().unwrap_or("")
            );
        }
    }
    println!("\n{} case(s) selected on platform {}", n, platform);
    0
}

fn selected(args: &Args, id: &str, tier: u8) -> bool {
    if !args.tiers.is_empty() && !args.tiers.contains(&tier) {
        return false;
    }
    if let Some(f) = &args.filter {
        if !id.contains(f.as_str()) {
            return false;
        }
    }
    true
}

fn cmd_run(args: &Args) -> i32 {
    let started = Instant::now();
    let platform = exec::platform_token();
    let host = hostname();
    let stamp = timestamp();

    let (manifests, load_errors) = manifest::load_dir(&args.cases_dir);
    for e in &load_errors {
        eprintln!("warning: {}", e);
    }

    let fixture_root = fixtures::discover_root(args.fixture_root.clone(), &regression_dir());
    let catalog = fixtures::Catalog::load(fixture_root.as_deref(), repo_root().as_deref());
    for w in &catalog.warnings {
        eprintln!("warning: {}", w);
    }

    // Build provenance. rb-cli does not self-report its commit, so the sha
    // comes from the working tree — sound only if the tree is clean and the
    // binary was built from it. See gitinfo.rs.
    let repo = repo_root().unwrap_or_else(|| PathBuf::from("."));
    let git_sha = gitinfo::head_sha(&repo).unwrap_or_default();
    let dirty = gitinfo::dirty_files(&repo).unwrap_or_default();

    if args.require_clean && !dirty.is_empty() {
        eprintln!("error: working tree is dirty; refusing to run with --require-clean");
        eprintln!("  a dirty tree means no commit describes what is being tested,");
        eprintln!("  so results could not be attributed to a build.");
        for f in dirty.iter().take(10) {
            eprintln!("    {}", f);
        }
        if dirty.len() > 10 {
            eprintln!("    ... and {} more", dirty.len() - 10);
        }
        eprintln!("  commit, stash, or add transient paths to .gitignore.");
        return 2;
    }
    if !dirty.is_empty() {
        eprintln!(
            "warning: working tree is dirty ({} path(s)); results will be tagged .dirty",
            dirty.len()
        );
    }

    match preflight(&args.rb_cli, &repo) {
        Ok(banner) => print!("{}", banner),
        Err(e) => {
            eprintln!("error: {}", e);
            return 2;
        }
    }
    let rb_version = probe_version(&args.rb_cli);

    let build_label = gitinfo::build_label(&repo, rb_version.as_deref().unwrap_or("unknown"));
    let identity = RunIdentity {
        run_id: format!("{}-{}-{}", stamp, host, platform),
        git_sha: git_sha.clone(),
        rb_version: build_label.clone(),
    };

    let mut bundle = match Bundle::create(&args.report_root, &host, platform, &stamp, identity) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("error: could not create report bundle: {}", e);
            return 2;
        }
    };
    println!("report bundle: {}", bundle.dir.display());
    println!("fixtures     : {} catalogued", catalog.len());

    let env = serde_json::json!({
        "platform": platform,
        "host": host,
        "stamp": stamp,
        "rb_cli": args.rb_cli.display().to_string(),
        "rb_cli_version": rb_version,
        "build_label": build_label,
        "git_sha": git_sha,
        "git_branch": gitinfo::branch(&repo),
        "git_clean": dirty.is_empty(),
        "fixture_root": catalog.root().map(|p| p.display().to_string()),
        "fixtures_catalogued": catalog.len(),
        "allow_hardware": args.allow_hardware,
        "manifest_load_errors": load_errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
    });
    let _ = bundle.write_env(&env);

    // A manifest that would not parse is a harness error against that file,
    // recorded like any other result rather than aborting the run.
    for e in &load_errors {
        let r = CaseResult {
            run_id: String::new(),
            git_sha: String::new(),
            rb_version: String::new(),
            case_id: format!(
                "harness.manifest.{}",
                sanitise_id(&e.path.to_string_lossy())
            ),
            group: "harness.manifest".to_string(),
            tier: 0,
            verdict: Verdict::Error,
            fixture_id: None,
            skip_reason: Some(e.message.clone()),
            duration_ms: 0,
            failed_step: None,
            failed_assertions: Vec::new(),
            platform: platform.to_string(),
        };
        let _ = bundle.record(&r);
    }

    let mut counts = std::collections::BTreeMap::<&'static str, usize>::new();

    for (_, m) in &manifests {
        for case in &m.cases {
            let tier = case.tier.unwrap_or(m.meta.tier);
            if !selected(args, &case.id, tier) {
                continue;
            }
            let result = run_case(
                args,
                &catalog,
                &m.meta.group,
                tier,
                case,
                platform,
                &mut bundle,
            );
            *counts.entry(result.verdict.label()).or_insert(0) += 1;
            println!("{:<12} {}", result.verdict.label(), result.case_id);
            let _ = bundle.record(&result);
        }
    }

    let wall = started.elapsed().as_millis();
    let _ = bundle.write_missing_fixtures();
    let _ = bundle.write_tool_skips();
    let _ = bundle.write_summary(&env, wall);

    println!();
    for (label, n) in &counts {
        println!("{:<14} {}", label, n);
    }
    println!("\nsummary: {}", bundle.dir.join("summary.md").display());

    // Exit 0 even with failures: this harness reports, it does not gate. A
    // non-zero exit is reserved for the harness itself being unable to run.
    0
}

fn run_case(
    args: &Args,
    catalog: &fixtures::Catalog,
    group: &str,
    tier: u8,
    case: &manifest::Case,
    platform: &str,
    bundle: &mut Bundle,
) -> CaseResult {
    let started = Instant::now();
    let mut result = CaseResult {
        run_id: String::new(),
        git_sha: String::new(),
        rb_version: String::new(),
        case_id: case.id.clone(),
        group: group.to_string(),
        tier,
        verdict: Verdict::Pass,
        fixture_id: case.fixture.clone(),
        skip_reason: None,
        duration_ms: 0,
        failed_step: None,
        failed_assertions: Vec::new(),
        platform: platform.to_string(),
    };

    let skip = |result: &mut CaseResult, v: Verdict, why: String, started: Instant| {
        result.verdict = v;
        result.skip_reason = Some(why);
        result.duration_ms = started.elapsed().as_millis();
    };

    if !case.platforms.is_empty() && !case.platforms.iter().any(|p| p == platform) {
        skip(
            &mut result,
            Verdict::SkipPlatform,
            format!("case targets {:?}", case.platforms),
            started,
        );
        return result;
    }

    if case.hardware && !args.allow_hardware {
        skip(
            &mut result,
            Verdict::SkipHardware,
            "hardware case; --allow-hardware not given".to_string(),
            started,
        );
        return result;
    }

    for tool in &case.requires {
        if !exec::tool_available(tool) {
            bundle.note_tool_skip(tool, &case.id);
            skip(
                &mut result,
                Verdict::SkipTool,
                format!("required tool '{}' not on PATH", tool),
                started,
            );
            return result;
        }
    }

    // Resolve the fixture. An unresolvable ID is a shopping-list entry, not a
    // failure — the corpus is expected to be incomplete.
    let fixture_path = match &case.fixture {
        Some(id) => match catalog.materialise(id, &args.scratch_root.join("_fixture-cache")) {
            Ok(p) => Some(p),
            Err(why) => {
                bundle.note_missing_fixture(id, &case.id);
                skip(&mut result, Verdict::SkipFixture, why, started);
                return result;
            }
        },
        None => None,
    };

    let scratch = args.scratch_root.join(sanitise_id(&case.id));
    let _ = fs::remove_dir_all(&scratch);
    if let Err(e) = fs::create_dir_all(&scratch) {
        result.verdict = Verdict::Error;
        result.skip_reason = Some(format!("could not create scratch dir: {}", e));
        result.duration_ms = started.elapsed().as_millis();
        return result;
    }

    let steps = case.resolved_steps();

    // A mutating case must never touch the corpus. If any step asks for
    // {fixture_copy}, take a private copy into scratch first.
    let wants_copy = steps
        .iter()
        .any(|s| s.args.iter().any(|a| a.contains("{fixture_copy}")));
    let fixture_copy = if wants_copy {
        match &fixture_path {
            Some(src) => {
                let name = src
                    .file_name()
                    .map(|n| n.to_os_string())
                    .unwrap_or_else(|| std::ffi::OsString::from("fixture.img"));
                let dst = scratch.join(name);
                if let Err(e) = fs::copy(src, &dst) {
                    result.verdict = Verdict::Error;
                    result.skip_reason =
                        Some(format!("could not copy fixture into scratch: {}", e));
                    result.duration_ms = started.elapsed().as_millis();
                    return result;
                }
                Some(dst)
            }
            None => {
                result.verdict = Verdict::Error;
                result.skip_reason =
                    Some("case uses {fixture_copy} but declares no fixture".to_string());
                result.duration_ms = started.elapsed().as_millis();
                return result;
            }
        }
    } else {
        None
    };

    let scratch_s = scratch.display().to_string();
    let fixture_s = fixture_path.as_ref().map(|p| p.display().to_string());
    let copy_s = fixture_copy.as_ref().map(|p| p.display().to_string());
    let cases_s = args.cases_dir.display().to_string();

    let resolve = move |raw: &str| -> String {
        let mut s = raw.replace("{scratch}", &scratch_s);
        s = s.replace("{cases}", &cases_s);
        if let Some(f) = &fixture_s {
            s = s.replace("{fixture}", f);
        }
        if let Some(c) = &copy_s {
            s = s.replace("{fixture_copy}", c);
        }
        s
    };

    let timeout = Duration::from_millis(case.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));

    for (idx, step) in steps.iter().enumerate() {
        let args_resolved: Vec<String> = step.args.iter().map(|a| resolve(a)).collect();

        let out = match exec::run(&args.rb_cli, &args_resolved, &scratch, timeout) {
            Ok(o) => o,
            Err(e) => {
                result.verdict = Verdict::Error;
                result.failed_step = Some(idx);
                result.skip_reason = Some(e.to_string());
                result.duration_ms = started.elapsed().as_millis();
                return result;
            }
        };

        let failures = assertion::evaluate(step, &out, &resolve);
        if !failures.is_empty() {
            result.verdict = Verdict::Fail;
            result.failed_step = Some(idx);
            let _ = bundle.write_failure(
                &case.id,
                &args.rb_cli,
                &args_resolved,
                &scratch,
                out.exit_code,
                step.expect_exit,
                &out.stdout,
                &out.stderr,
                &failures,
                case.fixture.as_deref().zip(fixture_path.as_deref()),
            );
            result.failed_assertions = failures;
            result.duration_ms = started.elapsed().as_millis();
            // Scratch is preserved for failing cases so the artifacts are
            // there when someone comes to reproduce.
            return result;
        }
    }

    result.duration_ms = started.elapsed().as_millis();
    if !args.keep_scratch {
        let _ = fs::remove_dir_all(&scratch);
    }
    result
}

/// The binary under test, as an absolute path into the repo's **release**
/// target directory.
///
/// It used to be a bare `rb-cli`, i.e. a PATH lookup, which meant the harness
/// could test whichever build happened to be on PATH — or, once cases started
/// running in a scratch cwd, nothing at all. An absolute default makes the
/// answer to "what did we just test?" a property of the path rather than of
/// the caller's shell.
fn default_rb_cli(regression_dir: &Path) -> PathBuf {
    let repo = regression_dir
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    repo.join("target").join("release").join(if cfg!(windows) {
        "rb-cli.exe"
    } else {
        "rb-cli"
    })
}

/// Newest mtime under `src/`, so a binary older than the sources it was built
/// from can be called out.
fn newest_source_mtime(repo: &Path) -> Option<SystemTime> {
    fn walk(dir: &Path, newest: &mut Option<SystemTime>) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, newest);
            } else if p.extension().map(|x| x == "rs").unwrap_or(false) {
                if let Ok(m) = e.metadata().and_then(|m| m.modified()) {
                    if newest.map(|n| m > n).unwrap_or(true) {
                        *newest = Some(m);
                    }
                }
            }
        }
    }
    let mut newest = None;
    walk(&repo.join("src"), &mut newest);
    newest
}

/// What is about to be tested, checked and printed before anything runs.
///
/// The harness cannot tell a stale binary from a current one by looking at it —
/// rb-cli does not bake its commit into `--version` — so this checks the three
/// things that can be checked: the binary exists where we say, it runs and
/// reports a version, and it is not older than the sources in the tree. A run
/// against a month-old binary produces a full report that describes nothing
/// anyone can act on, and nothing in the output would have said so.
fn preflight(rb_cli: &Path, repo: &Path) -> Result<String, String> {
    if !rb_cli.is_file() {
        return Err(format!(
            "no rb-cli at {}
  build it first:  cargo build --release --bin rb-cli",
            rb_cli.display()
        ));
    }
    let version = probe_version(rb_cli)
        .ok_or_else(|| format!("{} exists but will not run --version", rb_cli.display()))?;

    let mut notes = Vec::new();
    // A debug build is a different program: different assertions, different
    // timing, and every published number assumes release.
    if !rb_cli.to_string_lossy().replace('\\', "/").contains("/release/") {
        notes.push("WARNING: not a /release/ build — results will not be comparable".to_string());
    }
    if let (Ok(bin), Some(src)) = (
        rb_cli.metadata().and_then(|m| m.modified()),
        newest_source_mtime(repo),
    ) {
        if src > bin {
            notes.push(
                "WARNING: sources under src/ are newer than this binary; rebuild, or the run describes a build that no longer exists"
                    .to_string(),
            );
        }
    }

    let mut s = format!("rb-cli   : {}
version  : {}
", rb_cli.display(), version);
    for n in notes {
        s.push_str(&format!("{}
", n));
    }
    Ok(s)
}

fn probe_version(rb_cli: &Path) -> Option<String> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let out = exec::run(
        rb_cli,
        &["--version".to_string()],
        &cwd,
        Duration::from_secs(30),
    )
    .ok()?;
    if out.exit_code != Some(0) {
        return None;
    }
    Some(out.stdout.trim().to_string())
}

/// Every result line and bundle directory is attributed to a host, and
/// `consolidate` groups by it, so "unknown-host" from a second machine would
/// quietly merge two hosts into one row. `COMPUTERNAME` is Windows-only and
/// `HOSTNAME` is a shell variable most Unix shells never export, so the
/// portable answer is to ask the system.
fn hostname() -> String {
    if let Ok(h) = std::env::var("COMPUTERNAME") {
        if !h.trim().is_empty() {
            return h.trim().to_string();
        }
    }
    if let Ok(h) = std::env::var("HOSTNAME") {
        if !h.trim().is_empty() {
            return h.trim().to_string();
        }
    }
    if let Ok(out) = std::process::Command::new("hostname").output() {
        let h = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !h.is_empty() {
            return h;
        }
    }
    "unknown-host".to_string()
}

/// UTC-ish stamp without pulling in a date crate. Seconds since the epoch
/// rendered as a sortable value is enough — bundles are ordered, not read as
/// wall-clock times, and the exact civil time is in the filesystem metadata.
fn timestamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{}", secs)
}

fn sanitise_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
