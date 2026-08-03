CRATE = "instability"
TARGETS = ["instability/src/stable.rs", "instability/src/unstable.rs"]
GAP = """\
`instability` builds its doc strings with `indoc::formatdoc!`, and a proc
macro that *forwards* a token from its input loses that token's hygiene
context crossing mrustc's proc-macro bridge. `formatdoc!` re-emits the
trailing arguments verbatim, so

    formatdoc! {"... version {}.", version.trim_start_matches('v')}

expands to a `format!` whose `version` carries an empty hygiene context and
no longer resolves to the `if let Some(ref version)` binding around it:

    MACRO<::"alloc"::format> error:0: Couldn't find variable name 'version'

(Confirmed with MRUSTC_DEBUG=Expand: the expansion is correct token-for-token
-- `format!{"...{}.", version.trim_start_matches('v')}` -- and the forwarded
ident is the only one carrying `/*Rust2021 /**/*/`.)

`format!` is a builtin, so writing these three call sites as `format!` with
the string already unindented keeps the output byte-identical and takes the
proc macro out of the picture. Fixing the bridge's hygiene is the real
answer and is filed as an open item; it is a much larger change than this.
"""
UPSTREAM = None

# Anchor on the rewritten call site: the crate's own test constants contain
# the folded `"# Stability\n\n..."` string, so the string alone is ambiguous.
APPLIED = r'format!\(\s*"# Stability\\n\\n'
MATCH = r'formatdoc! \{"'

EDITS = {
    "instability/src/stable.rs": [
        ('            formatdoc! {"\n'
         '                # Stability\n'
         '\n'
         '                This API was stabilized in version {}.",\n'
         "                version.trim_start_matches('v')\n"
         '            }\n',
         '            format!(\n'
         '                "# Stability\\n\\nThis API was stabilized in version {}.",\n'
         "                version.trim_start_matches('v')\n"
         '            )\n'),
        ('            formatdoc! {"\n'
         '                # Stability\n'
         '\n'
         '                This API is stable."}\n',
         '            format!("# Stability\\n\\nThis API is stable.")\n'),
    ],
    "instability/src/unstable.rs": [
        ('        let doc = formatdoc! {"\n'
         '            # Stability\n'
         '\n'
         '            **This API is marked as unstable** and is only available when the `{feature_flag}`\n'
         '            crate feature is enabled. This comes with no stability guarantees, and could be changed\n'
         '            or removed at any time."};\n',
         '        let doc = format!(\n'
         '            "# Stability\\n\\n**This API is marked as unstable** and is only available when the `{feature_flag}`\\n'
         'crate feature is enabled. This comes with no stability guarantees, and could be changed\\n'
         'or removed at any time."\n'
         '        );\n'),
    ],
}


def patch(text, path):
    for old, new in EDITS[path]:
        text = text.replace(old, new, 1)
    return text
