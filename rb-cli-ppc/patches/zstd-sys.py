CRATE = "zstd-sys"
TARGETS = ["zstd-sys/Cargo.toml"]
GAP = """\
zstd-sys is the ONLY crate in this graph that asks for `cc`'s `parallel`
feature, and `parallel` is what drags in cc's async build-command runner
(src/parallel/{async_executor,command_runner}.rs). mrustc's `async fn`
support does not produce a `Future` impl for the generated async block:

    cc/src/parallel/command_runner.rs:175:1 error:0:
      Cannot find an impl of ::"core"::future::future::Future for async[...]

cc gates the whole module on the feature (`#[cfg(feature = "parallel")]
mod parallel;`) and keeps a `#[cfg(not(feature = "parallel"))]` serial arm in
command_helpers.rs, so dropping the feature deletes every async construct in
the crate -- there are none outside src/parallel/ -- and leaves cc
functionally identical, only compiling the C files one at a time.

cc is a BUILD-dependency (of bzip2-sys and zstd-sys) and never reaches the
PowerPC binary, so this costs build-script wall clock and nothing else.
Chosen over pinning cc back to a pre-async 1.0.x, which would mean
re-vendoring the whole graph to work around a crate that is not shipped.
"""
UPSTREAM = None

MARKER = '# rb-cli-ppc: cc "parallel" feature dropped (mrustc async fn yields no Future)'

APPLIED = r'^# rb-cli-ppc: cc "parallel" feature dropped'
MATCH = r'^features = \["parallel"\]$'


def patch(text, path):
    out, section = [], None
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped
        if section == "[build-dependencies.cc]" and stripped == 'features = ["parallel"]':
            out.append(MARKER + "\n")
            continue
        out.append(line)
    return "".join(out)
