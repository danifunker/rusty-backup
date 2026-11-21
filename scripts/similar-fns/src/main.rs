//! similar-fns: find functions that do the same job under different names.
//!
//! Parses every `.rs` file under the given roots with `syn`, reduces each function
//! body to a token shape (local names, field names, user types and literals become
//! placeholders; keywords, primitive types, std names, called functions, methods
//! and macros stay), then reports three things:
//!
//! 1. exact-shape groups: bodies that are identical once names are abstracted;
//! 2. near-shape clusters: bodies whose 6-token shingle sets overlap by at least
//!    `--threshold` (Jaccard), found with MinHash + LSH so the run stays linear;
//! 3. same-name free functions defined in two or more crates.
//!
//! Called names stay because they carry the meaning: two readers that differ only
//! in `from_be_bytes` vs `from_le_bytes` are not duplicates, and the shape keeps
//! them apart. The output is a markdown report ranked by the lines a merge would
//! remove. It is a lead list for a human, not a verdict. See README.md.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use proc_macro2::{Delimiter, Spacing, TokenStream, TokenTree};
use quote::ToTokens;
use syn::spanned::Spanned;
use syn::visit::Visit;

const KEYWORDS: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub",
    "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true", "type",
    "unsafe", "use", "where", "while",
];
const PRIMITIVES: &[&str] = &[
    "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize", "f32",
    "f64", "bool", "char", "str",
];
const STD_NAMES: &[&str] = &[
    "Vec",
    "String",
    "Option",
    "Some",
    "None",
    "Result",
    "Ok",
    "Err",
    "Box",
    "Rc",
    "Arc",
    "HashMap",
    "HashSet",
    "BTreeMap",
    "BTreeSet",
    "VecDeque",
    "Path",
    "PathBuf",
    "Cow",
    "Default",
    "Read",
    "Write",
    "Seek",
    "SeekFrom",
    "Start",
    "End",
    "Current",
    "Error",
    "ErrorKind",
    "Ordering",
    "Duration",
    "Instant",
    "SystemTime",
    "UNIX_EPOCH",
    "Mutex",
    "RwLock",
    "Cell",
    "RefCell",
    "Iterator",
    "IntoIterator",
    "From",
    "Into",
    "TryFrom",
    "TryInto",
    "AsRef",
    "File",
];

#[derive(Debug)]
struct Func {
    file: String,
    line: usize,
    lines: usize,
    unit: String,
    name: String,
    owner: String,
    free: bool,
    shape: Vec<u32>,
}

#[derive(Default)]
struct Interner {
    ids: HashMap<String, u32>,
}

impl Interner {
    fn id(&mut self, s: &str) -> u32 {
        let next = self.ids.len() as u32;
        *self.ids.entry(s.to_string()).or_insert(next)
    }
}

/// One flattened token: its text and whether an ident is "kept" is decided with lookahead.
#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Punct(char, bool),
    Lit(String),
    Open(char),
    Close(char),
}

fn flatten(ts: TokenStream, out: &mut Vec<Tok>) {
    for tt in ts {
        match tt {
            TokenTree::Ident(i) => out.push(Tok::Ident(i.to_string())),
            TokenTree::Punct(p) => out.push(Tok::Punct(p.as_char(), p.spacing() == Spacing::Joint)),
            TokenTree::Literal(l) => out.push(Tok::Lit(l.to_string())),
            TokenTree::Group(g) => {
                let (o, c) = match g.delimiter() {
                    Delimiter::Parenthesis => ('(', ')'),
                    Delimiter::Brace => ('{', '}'),
                    Delimiter::Bracket => ('[', ']'),
                    Delimiter::None => (' ', ' '),
                };
                if o != ' ' {
                    out.push(Tok::Open(o));
                }
                flatten(g.stream(), out);
                if c != ' ' {
                    out.push(Tok::Close(c));
                }
            }
        }
    }
}

fn is_upper(s: &str) -> bool {
    s.chars().next().is_some_and(|c| c.is_ascii_uppercase())
}

/// Reduces a token stream to placeholder text: what the code does, not what it names.
fn shape_text(ts: TokenStream) -> Vec<String> {
    let mut toks = Vec::new();
    flatten(ts, &mut toks);
    let mut out = Vec::with_capacity(toks.len());
    for (i, t) in toks.iter().enumerate() {
        let next = toks.get(i + 1);
        let next2 = toks.get(i + 2);
        let prev = if i > 0 { toks.get(i - 1) } else { None };
        let s = match t {
            Tok::Ident(s) => {
                let called = matches!(next, Some(Tok::Open('(')))
                    || matches!(next, Some(Tok::Punct('!', _)));
                let path_head = matches!(next, Some(Tok::Punct(':', true)))
                    && matches!(next2, Some(Tok::Punct(':', _)));
                let after_dot = matches!(prev, Some(Tok::Punct('.', _)));
                if KEYWORDS.contains(&s.as_str())
                    || PRIMITIVES.contains(&s.as_str())
                    || STD_NAMES.contains(&s.as_str())
                {
                    s.clone()
                } else if is_upper(s) {
                    "$T".to_string()
                } else if called || path_head {
                    s.clone()
                } else if after_dot {
                    "$f".to_string()
                } else {
                    "$i".to_string()
                }
            }
            Tok::Punct(c, _) => c.to_string(),
            Tok::Lit(l) => {
                if l.starts_with('"')
                    || l.starts_with("r\"")
                    || l.starts_with("r#")
                    || l.starts_with("b\"")
                {
                    "$s".to_string()
                } else {
                    "$n".to_string()
                }
            }
            Tok::Open(c) | Tok::Close(c) => c.to_string(),
        };
        out.push(s);
    }
    out
}

fn has_attr(attrs: &[syn::Attribute], name: &str) -> bool {
    attrs.iter().any(|a| a.path().is_ident(name))
}

fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("cfg")
            && a.meta
                .require_list()
                .is_ok_and(|l| l.tokens.to_string().contains("test"))
    })
}

struct Collector<'a> {
    file: String,
    unit: String,
    include_tests: bool,
    in_test: usize,
    owner: Vec<(String, bool)>,
    interner: &'a mut Interner,
    out: Vec<Func>,
}

impl<'a> Collector<'a> {
    fn push(
        &mut self,
        name: &str,
        attrs: &[syn::Attribute],
        sig: &syn::Signature,
        block: &syn::Block,
        free: bool,
    ) {
        if !self.include_tests && (self.in_test > 0 || has_attr(attrs, "test")) {
            return;
        }
        let span = block.span();
        let line = sig.fn_token.span.start().line;
        let lines = span.end().line.saturating_sub(line) + 1;
        let mut ts = TokenStream::new();
        quote_into(&sig.inputs, &mut ts);
        quote_into(&sig.output, &mut ts);
        quote_into(block, &mut ts);
        let shape = shape_text(ts).iter().map(|s| self.interner.id(s)).collect();
        let owner = self
            .owner
            .last()
            .map(|(o, _)| o.clone())
            .unwrap_or_default();
        self.out.push(Func {
            file: self.file.clone(),
            line,
            lines,
            unit: self.unit.clone(),
            name: name.to_string(),
            owner,
            free,
            shape,
        });
    }
}

fn quote_into<T: ToTokens>(t: &T, ts: &mut TokenStream) {
    t.to_tokens(ts);
}

impl<'a, 'ast> Visit<'ast> for Collector<'a> {
    fn visit_item_mod(&mut self, m: &'ast syn::ItemMod) {
        let test = is_cfg_test(&m.attrs) || m.ident == "tests";
        self.in_test += test as usize;
        syn::visit::visit_item_mod(self, m);
        self.in_test -= test as usize;
    }

    fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
        self.push(&f.sig.ident.to_string(), &f.attrs, &f.sig, &f.block, true);
        syn::visit::visit_item_fn(self, f);
    }

    fn visit_item_impl(&mut self, i: &'ast syn::ItemImpl) {
        let ty = ToTokens::to_token_stream(&*i.self_ty)
            .to_string()
            .replace(' ', "");
        let owner = match &i.trait_ {
            Some((_, p, _)) => {
                let tr = ToTokens::to_token_stream(p).to_string().replace(' ', "");
                (format!("impl {} for {}", tr, ty), true)
            }
            None => (format!("impl {}", ty), false),
        };
        let test = is_cfg_test(&i.attrs);
        self.in_test += test as usize;
        self.owner.push(owner);
        syn::visit::visit_item_impl(self, i);
        self.owner.pop();
        self.in_test -= test as usize;
    }

    fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
        self.push(&f.sig.ident.to_string(), &f.attrs, &f.sig, &f.block, false);
        syn::visit::visit_impl_item_fn(self, f);
    }

    fn visit_item_trait(&mut self, t: &'ast syn::ItemTrait) {
        self.owner.push((format!("trait {}", t.ident), false));
        syn::visit::visit_item_trait(self, t);
        self.owner.pop();
    }

    fn visit_trait_item_fn(&mut self, f: &'ast syn::TraitItemFn) {
        if let Some(b) = &f.default {
            self.push(&f.sig.ident.to_string(), &f.attrs, &f.sig, b, false);
        }
        syn::visit::visit_trait_item_fn(self, f);
    }
}

/// The review unit a file belongs to: a crate, a top-level src/ module, a front-end.
fn unit_of(rel: &str) -> String {
    let parts: Vec<&str> = rel.split('/').collect();
    match parts.as_slice() {
        ["crates", c, ..] | ["frontends", c, ..] => format!("{}/{}", parts[0], c),
        ["src", m, _, ..] => format!("src/{}", m),
        ["src", f] => format!("src/{}", f.trim_end_matches(".rs")),
        [a, b, ..] => format!("{}/{}", a, b),
        _ => rel.to_string(),
    }
}

fn collect(
    repo: &Path,
    roots: &[PathBuf],
    include_tests: bool,
    interner: &mut Interner,
) -> (Vec<Func>, usize, Vec<String>) {
    let mut funcs = Vec::new();
    let mut files = 0;
    let mut failed = Vec::new();
    for root in roots {
        let walk = walkdir::WalkDir::new(repo.join(root))
            .into_iter()
            .filter_entry(|e| {
                let n = e.file_name().to_string_lossy();
                !(n == "target" || n == "fixtures" || n == "fixtures-large" || n.starts_with('.'))
            });
        for e in walk.flatten() {
            if !e.file_type().is_file() || e.path().extension().map_or(true, |x| x != "rs") {
                continue;
            }
            let rel = e
                .path()
                .strip_prefix(repo)
                .unwrap_or(e.path())
                .to_string_lossy()
                .replace('\\', "/");
            if !include_tests
                && (rel.starts_with("tests/")
                    || rel.contains("/tests/")
                    || rel.ends_with("tests.rs"))
            {
                continue;
            }
            let Ok(src) = std::fs::read_to_string(e.path()) else {
                continue;
            };
            let Ok(ast) = syn::parse_file(&src) else {
                failed.push(rel);
                continue;
            };
            files += 1;
            let mut c = Collector {
                unit: unit_of(&rel),
                file: rel,
                include_tests,
                in_test: 0,
                owner: Vec::new(),
                interner,
                out: Vec::new(),
            };
            c.visit_file(&ast);
            funcs.extend(c.out);
        }
    }
    (funcs, files, failed)
}

fn fnv(data: &[u32]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &d in data {
        for b in d.to_le_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
    }
    h
}

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

const SHINGLE: usize = 6;
const HASHES: usize = 32;
const BANDS: usize = 8;

fn shingles(shape: &[u32]) -> Vec<u64> {
    let mut v: Vec<u64> = shape.windows(SHINGLE).map(fnv).collect();
    v.sort_unstable();
    v.dedup();
    v
}

fn jaccard(a: &[u64], b: &[u64]) -> f64 {
    let (mut i, mut j, mut inter) = (0, 0, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                inter += 1;
                i += 1;
                j += 1;
            }
        }
    }
    let union = a.len() + b.len() - inter;
    if union == 0 {
        0.0
    } else {
        inter as f64 / union as f64
    }
}

struct Dsu(Vec<usize>);

impl Dsu {
    fn find(&mut self, x: usize) -> usize {
        let p = self.0[x];
        if p == x {
            return x;
        }
        let r = self.find(p);
        self.0[x] = r;
        r
    }
    fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            self.0[ra] = rb;
        }
    }
}

struct Cluster {
    members: Vec<usize>,
    similarity: f64,
}

impl Cluster {
    fn removable(&self, funcs: &[Func]) -> usize {
        let total: usize = self.members.iter().map(|&m| funcs[m].lines).sum();
        let max = self
            .members
            .iter()
            .map(|&m| funcs[m].lines)
            .max()
            .unwrap_or(0);
        total - max
    }
    fn units(&self, funcs: &[Func]) -> usize {
        self.members
            .iter()
            .map(|&m| funcs[m].unit.as_str())
            .collect::<HashSet<_>>()
            .len()
    }
}

/// A body that is mostly one repeated pattern (an enum-to-string `match`) is a table, not logic.
fn is_table(shape: &[u32]) -> bool {
    let windows = shape.len().saturating_sub(SHINGLE - 1);
    windows > 0 && (shingles(shape).len() as f64) < TABLE_RATIO * windows as f64
}

const TABLE_RATIO: f64 = 0.4;

fn exact_groups(funcs: &[Func], eligible: &[usize]) -> Vec<Cluster> {
    let mut by: HashMap<&[u32], Vec<usize>> = HashMap::new();
    for &i in eligible {
        by.entry(funcs[i].shape.as_slice()).or_default().push(i);
    }
    by.into_values()
        .filter(|v| v.len() > 1)
        .map(|members| Cluster {
            members,
            similarity: 1.0,
        })
        .collect()
}

/// MinHash + LSH over shingle sets; candidate pairs are confirmed with exact Jaccard.
fn near_clusters(
    funcs: &[Func],
    eligible: &[usize],
    threshold: f64,
    exact: &[Cluster],
) -> (Vec<Cluster>, usize) {
    let sets: HashMap<usize, Vec<u64>> = eligible
        .iter()
        .map(|&i| (i, shingles(&funcs[i].shape)))
        .collect();
    let mut exact_of: HashMap<usize, usize> = HashMap::new();
    for (g, c) in exact.iter().enumerate() {
        for &m in &c.members {
            exact_of.insert(m, g);
        }
    }
    let rows = HASHES / BANDS;
    let mut buckets: HashMap<(usize, u64), Vec<usize>> = HashMap::new();
    for &i in eligible {
        let set = &sets[&i];
        if set.is_empty() {
            continue;
        }
        let sig: Vec<u64> = (0..HASHES)
            .map(|k| {
                set.iter()
                    .map(|&s| mix(s ^ mix(k as u64)))
                    .min()
                    .unwrap_or(0)
            })
            .collect();
        for b in 0..BANDS {
            let key = fnv(&sig[b * rows..(b + 1) * rows]
                .iter()
                .flat_map(|x| [*x as u32, (*x >> 32) as u32])
                .collect::<Vec<_>>());
            buckets.entry((b, key)).or_default().push(i);
        }
    }
    let mut dsu = Dsu((0..funcs.len()).collect());
    let mut best: HashMap<usize, f64> = HashMap::new();
    let mut seen: HashSet<(usize, usize)> = HashSet::new();
    let mut oversized = 0;
    for members in buckets.values() {
        if members.len() > 300 {
            oversized += 1;
            continue;
        }
        for (x, &a) in members.iter().enumerate() {
            for &b in &members[x + 1..] {
                let key = (a.min(b), a.max(b));
                if !seen.insert(key) {
                    continue;
                }
                if exact_of.contains_key(&a) && exact_of.get(&a) == exact_of.get(&b) {
                    continue;
                }
                let j = jaccard(&sets[&a], &sets[&b]);
                if j >= threshold {
                    dsu.union(a, b);
                    for m in [a, b] {
                        let e = best.entry(m).or_insert(0.0);
                        if j > *e {
                            *e = j;
                        }
                    }
                }
            }
        }
    }
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for &i in eligible {
        if best.contains_key(&i) {
            groups.entry(dsu.find(i)).or_default().push(i);
        }
    }
    let clusters = groups
        .into_values()
        .filter(|v| v.len() > 1)
        .map(|members| {
            let similarity = members.iter().map(|m| best[m]).fold(1.0, f64::min);
            Cluster {
                members,
                similarity,
            }
        })
        .collect();
    (clusters, oversized)
}

/// Free functions sharing a name across units, kept when two of the bodies are also alike.
fn same_name(funcs: &[Func], eligible: &[usize], min_sim: f64) -> Vec<(String, Vec<usize>, f64)> {
    let mut by: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for &i in eligible {
        if funcs[i].free && funcs[i].name != "main" {
            by.entry(funcs[i].name.as_str()).or_default().push(i);
        }
    }
    let mut v = Vec::new();
    for (name, ms) in by {
        if ms
            .iter()
            .map(|&i| funcs[i].unit.as_str())
            .collect::<HashSet<_>>()
            .len()
            < 2
        {
            continue;
        }
        let sets: Vec<Vec<u64>> = ms
            .iter()
            .take(40)
            .map(|&i| shingles(&funcs[i].shape))
            .collect();
        let mut best = 0.0f64;
        for a in 0..sets.len() {
            for b in a + 1..sets.len() {
                if funcs[ms[a]].unit != funcs[ms[b]].unit {
                    best = best.max(jaccard(&sets[a], &sets[b]));
                }
            }
        }
        if best >= min_sim {
            v.push((name.to_string(), ms, best));
        }
    }
    v.sort_by(|a, b| {
        b.1.len()
            .cmp(&a.1.len())
            .then(b.2.total_cmp(&a.2))
            .then(a.0.cmp(&b.0))
    });
    v
}

fn member_line(f: &Func) -> String {
    let owner = if f.owner.is_empty() {
        String::new()
    } else {
        format!(" in `{}`", f.owner)
    };
    format!(
        "- [{}:{}]({}#L{}) `{}`{} ({} lines)",
        f.file, f.line, f.file, f.line, f.name, owner, f.lines
    )
}

struct Opts {
    repo: PathBuf,
    roots: Vec<PathBuf>,
    min_tokens: usize,
    threshold: f64,
    top: usize,
    include_tests: bool,
    keep_tables: bool,
    out: Option<PathBuf>,
    unit: Option<String>,
}

fn parse_args() -> Result<Opts, String> {
    let mut o = Opts {
        repo: std::env::current_dir().map_err(|e| e.to_string())?,
        roots: Vec::new(),
        min_tokens: 60,
        threshold: 0.8,
        top: 60,
        include_tests: false,
        keep_tables: false,
        out: None,
        unit: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = |n: &str| args.next().ok_or(format!("{} needs a value", n));
        match a.as_str() {
            "--repo" => o.repo = PathBuf::from(val("--repo")?),
            "--min-tokens" => {
                o.min_tokens = val("--min-tokens")?.parse().map_err(|e| format!("{}", e))?
            }
            "--threshold" => {
                o.threshold = val("--threshold")?.parse().map_err(|e| format!("{}", e))?
            }
            "--top" => o.top = val("--top")?.parse().map_err(|e| format!("{}", e))?,
            "--out" => o.out = Some(PathBuf::from(val("--out")?)),
            "--unit" => o.unit = Some(val("--unit")?),
            "--include-tests" => o.include_tests = true,
            "--keep-tables" => o.keep_tables = true,
            "-h" | "--help" => return Err(USAGE.to_string()),
            r if r.starts_with('-') => return Err(format!("unknown flag {}\n{}", r, USAGE)),
            r => o.roots.push(PathBuf::from(r)),
        }
    }
    if o.roots.is_empty() {
        o.roots = ["crates", "src", "frontends"]
            .iter()
            .map(PathBuf::from)
            .collect();
    }
    Ok(o)
}

const USAGE: &str = "usage: similar-fns [--repo DIR] [--min-tokens N] [--threshold J] [--top N]
                   [--unit PREFIX] [--include-tests] [--keep-tables] [--out FILE] [ROOT ...]
  ROOT defaults to crates src frontends; --unit keeps groups touching that unit (e.g. crates/fs-mac)";

fn report(o: &Opts, funcs: &[Func], files: usize, failed: &[String]) -> String {
    let eligible: Vec<usize> = (0..funcs.len())
        .filter(|&i| funcs[i].shape.len() >= o.min_tokens)
        .filter(|&i| o.keep_tables || !is_table(&funcs[i].shape))
        .collect();
    let tables = (0..funcs.len())
        .filter(|&i| funcs[i].shape.len() >= o.min_tokens)
        .count()
        - eligible.len();
    let mut exact = exact_groups(funcs, &eligible);
    let (mut near, oversized) = near_clusters(funcs, &eligible, o.threshold, &exact);
    let mut names = same_name(funcs, &(0..funcs.len()).collect::<Vec<_>>(), 0.5);
    if let Some(u) = &o.unit {
        let touches = |c: &Cluster| {
            c.members
                .iter()
                .any(|&m| funcs[m].unit.starts_with(u.as_str()))
        };
        exact.retain(touches);
        near.retain(touches);
        names.retain(|(_, m, _)| m.iter().any(|&i| funcs[i].unit.starts_with(u.as_str())));
    }
    let rank = |v: &mut Vec<Cluster>| {
        v.sort_by(|a, b| {
            (b.units(funcs) > 1)
                .cmp(&(a.units(funcs) > 1))
                .then(b.removable(funcs).cmp(&a.removable(funcs)))
        })
    };
    rank(&mut exact);
    rank(&mut near);
    let sum = |v: &[Cluster]| v.iter().map(|c| c.removable(funcs)).sum::<usize>();

    let mut s = String::new();
    let _ = writeln!(s, "# Similar functions\n");
    let _ = writeln!(
        s,
        "Generated by `scripts/similar-fns` (see its README). A lead list for review, not a verdict: \
         two bodies with the same shape can still differ on purpose (endianness, bitmap polarity).\n"
    );
    let _ = writeln!(s, "| | |\n|---|---:|");
    let _ = writeln!(s, "| files parsed | {} |", files);
    let _ = writeln!(s, "| functions | {} |", funcs.len());
    let _ = writeln!(
        s,
        "| functions of at least {} shape tokens, compared | {} |",
        o.min_tokens,
        eligible.len()
    );
    if tables > 0 {
        let _ = writeln!(
            s,
            "| table-like bodies left out (enum-to-string matches; --keep-tables) | {} |",
            tables
        );
    }
    let _ = writeln!(
        s,
        "| exact-shape groups | {} ({} lines a merge could remove) |",
        exact.len(),
        sum(&exact)
    );
    let _ = writeln!(
        s,
        "| near-shape clusters (Jaccard >= {}) | {} ({} lines) |",
        o.threshold,
        near.len(),
        sum(&near)
    );
    let _ = writeln!(
        s,
        "| same-name free functions across units, bodies alike | {} |",
        names.len()
    );
    if !failed.is_empty() {
        let _ = writeln!(s, "| files syn could not parse | {} |", failed.len());
    }
    if oversized > 0 {
        let _ = writeln!(s, "| LSH buckets skipped as too common | {} |", oversized);
    }
    let _ = writeln!(
        s,
        "\nCross-unit groups (more than one crate, module or front-end) are listed first.\n"
    );
    let mut per_unit: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for c in exact.iter().chain(near.iter()) {
        let mut touched: HashSet<&str> = HashSet::new();
        for &m in &c.members {
            let e = per_unit.entry(funcs[m].unit.as_str()).or_default();
            e.1 += 1;
            if touched.insert(funcs[m].unit.as_str()) {
                e.0 += 1;
            }
        }
    }
    let mut rows: Vec<_> = per_unit.into_iter().collect();
    rows.sort_by(|a, b| (b.1).1.cmp(&(a.1).1).then(a.0.cmp(b.0)));
    let _ = writeln!(s, "## By unit\n\nWhere to start a slice review: groups that touch each unit, and its member functions.\n");
    let _ = writeln!(s, "| unit | groups | functions |\n|---|---:|---:|");
    for (u, (g, f)) in rows.iter().take(40) {
        let _ = writeln!(s, "| {} | {} | {} |", u, g, f);
    }
    let _ = writeln!(s);

    for (title, v) in [
        ("Exact-shape groups", &exact),
        ("Near-shape clusters", &near),
    ] {
        let _ = writeln!(s, "## {}\n", title);
        for (n, c) in v.iter().take(o.top).enumerate() {
            let _ = writeln!(
                s,
                "### {}.{} {} functions, {} units, ~{} lines removable{}\n",
                if title.starts_with("Exact") { "E" } else { "N" },
                n + 1,
                c.members.len(),
                c.units(funcs),
                c.removable(funcs),
                if c.similarity < 1.0 {
                    format!(", similarity >= {:.2}", c.similarity)
                } else {
                    String::new()
                }
            );
            let mut ms = c.members.clone();
            ms.sort_by(|&a, &b| {
                funcs[a]
                    .file
                    .cmp(&funcs[b].file)
                    .then(funcs[a].line.cmp(&funcs[b].line))
            });
            for m in ms.iter().take(12) {
                let _ = writeln!(s, "{}", member_line(&funcs[*m]));
            }
            if ms.len() > 12 {
                let _ = writeln!(s, "- ... and {} more", ms.len() - 12);
            }
            let _ = writeln!(s);
        }
        if v.len() > o.top {
            let _ = writeln!(s, "({} more not shown; raise --top)\n", v.len() - o.top);
        }
    }
    let _ = writeln!(s, "## Same-name free functions\n");
    let _ = writeln!(
        s,
        "Free functions (not methods) whose name is defined in more than one unit, where two of the \
         definitions in different units have bodies at least 0.5 alike. Small helpers count here: \
         this is where eight separate `le32`s show up.\n"
    );
    for (name, ms, sim) in names.iter().take(o.top) {
        let units: HashSet<&str> = ms.iter().map(|&i| funcs[i].unit.as_str()).collect();
        let _ = writeln!(
            s,
            "- `{}`: {} definitions in {} units, closest pair {:.2}",
            name,
            ms.len(),
            units.len(),
            sim
        );
        for &m in ms.iter().take(8) {
            let _ = writeln!(s, "  {}", member_line(&funcs[m]));
        }
        if ms.len() > 8 {
            let _ = writeln!(s, "  - ... and {} more", ms.len() - 8);
        }
    }
    if !failed.is_empty() {
        let _ = writeln!(s, "\n## Files syn could not parse\n");
        for f in failed {
            let _ = writeln!(s, "- {}", f);
        }
    }
    s
}

fn main() {
    let o = match parse_args() {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(2);
        }
    };
    let mut interner = Interner::default();
    let (funcs, files, failed) = collect(&o.repo, &o.roots, o.include_tests, &mut interner);
    let text = report(&o, &funcs, files, &failed);
    match &o.out {
        Some(p) => {
            if let Err(e) = std::fs::write(p, text) {
                eprintln!("{}: {}", p.display(), e);
                std::process::exit(2);
            }
        }
        None => print!("{}", text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn funcs_of(src: &str) -> Vec<Func> {
        let mut interner = Interner::default();
        let ast = syn::parse_file(src).unwrap();
        let mut c = Collector {
            file: "crates/x/src/lib.rs".into(),
            unit: "crates/x".into(),
            include_tests: false,
            in_test: 0,
            owner: Vec::new(),
            interner: &mut interner,
            out: Vec::new(),
        };
        c.visit_file(&ast);
        c.out
    }

    #[test]
    fn renamed_copy_has_the_same_shape() {
        let f = funcs_of(
            "fn read_be32(buf: &[u8], off: usize) -> u32 { u32::from_be_bytes([buf[off], buf[off+1], buf[off+2], buf[off+3]]) }
             fn get_u32(data: &[u8], at: usize) -> u32 { u32::from_be_bytes([data[at], data[at+1], data[at+2], data[at+3]]) }",
        );
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].shape, f[1].shape);
    }

    #[test]
    fn endianness_keeps_shapes_apart() {
        let f = funcs_of(
            "fn a(b: &[u8]) -> u32 { u32::from_be_bytes([b[0], b[1], b[2], b[3]]) }
             fn c(b: &[u8]) -> u32 { u32::from_le_bytes([b[0], b[1], b[2], b[3]]) }",
        );
        assert_ne!(f[0].shape, f[1].shape);
    }

    #[test]
    fn tests_are_skipped_by_default() {
        let f =
            funcs_of("fn keep() {} #[cfg(test)] mod t { fn skip() {} } #[test] fn also_skip() {}");
        assert_eq!(
            f.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            vec!["keep"]
        );
    }

    #[test]
    fn a_lightly_edited_copy_is_a_near_match() {
        let body = "let mut total = 0u64; for (i, b) in buf.iter().enumerate() { if *b == 0 { continue; } \
                    total = total.wrapping_mul(31).wrapping_add(*b as u64 ^ i as u64); if total > limit { break; } } \
                    let rest = total % 7; if rest == 3 { return Err(Error::Bad); } Ok(total.rotate_left(5) ^ rest)";
        let src = format!(
            "fn one(buf: &[u8], limit: u64) -> Result<u64, Error> {{ {} }}
             fn two(data: &[u8], cap: u64) -> Result<u64, Error> {{ log::trace!(\"x\"); {} }}",
            body,
            body.replace("buf", "data").replace("limit", "cap")
        );
        let f = funcs_of(&src);
        assert_ne!(f[0].shape, f[1].shape);
        let j = jaccard(&shingles(&f[0].shape), &shingles(&f[1].shape));
        assert!(j > 0.8, "jaccard {}", j);
        let (near, _) = near_clusters(&f, &[0, 1], 0.8, &[]);
        assert_eq!(near.len(), 1);
    }

    #[test]
    fn enum_label_tables_are_recognised() {
        let f = funcs_of(
            "fn label(k: K) -> &'static str { match k { K::Alpha => \"a\", K::Beta => \"b\", K::Gamma => \"c\",
             K::Delta => \"d\", K::Eps => \"e\", K::Zeta => \"f\", K::Eta => \"g\", K::Theta => \"h\" } }
             fn real(b: &[u8]) -> u32 { let mut s = 0u32; for x in b.iter() { s = s.rotate_left(3) ^ *x as u32; } s }",
        );
        assert!(is_table(&f[0].shape));
        assert!(!is_table(&f[1].shape));
    }

    #[test]
    fn units_follow_the_review_slices() {
        assert_eq!(unit_of("crates/fs-mac/src/hfs.rs"), "crates/fs-mac");
        assert_eq!(unit_of("src/controller/restore/mod.rs"), "src/controller");
        assert_eq!(unit_of("src/error.rs"), "src/error");
        assert_eq!(unit_of("frontends/gtk4/src/main.rs"), "frontends/gtk4");
    }
}
