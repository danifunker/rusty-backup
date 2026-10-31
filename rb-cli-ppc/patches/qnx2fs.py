CRATE = "qnx2fs"
TARGETS = ["qnx2fs/Cargo.toml"]
GAP = """\
Not a language gap: minicargo has no `[patch]` support ("TODO: Support
repository patches", tools/minicargo/manifest.cpp). The rb-cli manifests use
`[patch.crates-io] disk-core = { path = "../crates/disk-core" }` so that
qnx2fs's `disk-core` feature (its adapter to the disk-core traits) and the app
share one disk-core; with two copies the adapter's trait impls would not be
the app's traits. The build scripts hide that `[patch]` table from minicargo
(minicargo_run), and this patch does its job instead: the vendored qnx2fs takes
disk-core by path, from the in-tree crate (vendor/qnx2fs -> ../../../crates).
"""
UPSTREAM = None

APPLIED = r'^path = "\.\./\.\./\.\./crates/disk-core"$'
MATCH = r'^\[dependencies\.disk-core\]\nversion = "0\.1(\.\d+)?"$'


def patch(text, path):
    import re
    return re.sub(
        r'^(\[dependencies\.disk-core\]\n)version = "0\.1(\.\d+)?"$',
        r'\1path = "../../../crates/disk-core"',
        text,
        count=1,
        flags=re.M,
    )
