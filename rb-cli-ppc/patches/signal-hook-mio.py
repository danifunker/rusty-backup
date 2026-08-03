CRATE = "signal-hook-mio"
TARGETS = ["signal-hook-mio/src/lib.rs"]
GAP = """\
`implement_signals_with_pipe!` binds its `$pipe:path` argument with

    use $pipe as Pipe;

and mrustc cannot parse a `use` whose path is an interpolated fragment
followed by `as` -- having consumed the path it insists on `::`:

    signal-hook-mio/src/lib.rs:32:22 error:0:
      Unexpected token TOK_RWORD_AS, expected TOK_DOUBLE_COLON

`Pipe` is only ever used as a *type* here -- `SignalDelivery<Pipe, E>` and
`Pipe::pair()` -- so a type alias binds it identically and sidesteps the use
statement entirely. Every invocation passes a plain type path
(`mio::net::UnixStream`, `mio_uds::UnixStream`).

This is the same `TOK_RWORD_AS` family as the libyml gap that keeps the
`yaml` feature off for this target; fixing the parser would likely clear
both, and is the better long-term answer. See docs/build-ppc-mrustc.md.
"""
UPSTREAM = None

APPLIED = r"^        type Pipe = \$pipe;$"
MATCH = r"^        use \$pipe as Pipe;$"


def patch(text, path):
    return text.replace("        use $pipe as Pipe;",
                        "        type Pipe = $pipe;", 1)
