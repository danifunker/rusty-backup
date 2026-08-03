CRATE = "chrono"
TARGETS = ["chrono/src/naive/datetime/mod.rs"]
GAP = """\
chrono 0.4 defines `NaiveDateTime::UNIX_EPOCH` as
`DateTime::UNIX_EPOCH.naive_utc()`. Nothing in that expression pins
`DateTime`'s `Tz`; rustc resolves it because only one inherent impl
(`impl DateTime<Utc>`) declares a `UNIX_EPOCH`, but mrustc can't infer an
impl's type parameter from which impl happens to carry the associated const.
Same class of gap as the crc turbofish, same shape of fix -- `Utc` is already
in scope in that module. Doc comments and the `#[deprecated(note = ...)]`
string mention the path without using it, so those lines are left alone.
"""
UPSTREAM = None

APPLIED = r"DateTime::<Utc>::UNIX_EPOCH"
MATCH = r"DateTime::UNIX_EPOCH"


def patch(text, path):
    out = []
    for line in text.splitlines(keepends=True):
        if line.lstrip().startswith("///") or "#[deprecated" in line:
            out.append(line)
        else:
            out.append(line.replace("DateTime::UNIX_EPOCH",
                                    "DateTime::<Utc>::UNIX_EPOCH"))
    return "".join(out)
