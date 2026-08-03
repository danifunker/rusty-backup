import re

CRATE = "crc"
TARGETS = ["crc/src/crc%d.rs" % w for w in (8, 16, 32, 64, 128)]
GAP = """\
crc 3.x defines `Digest::new` so its const-generic impl parameters are pinned
only by the return type, and mrustc cannot infer an impl's params from the
return type alone. Spelling them out as `Digest::<uN, Table<L>>::new` says
what mrustc cannot infer and changes nothing else.
"""
UPSTREAM = None

APPLIED = r"Digest::<u\d+, Table<L>>::new"
MATCH = r"^        Digest::new\(self, value\)$"


def patch(text, path):
    width = re.search(r"crc(\d+)\.rs$", path).group(1)
    return text.replace(
        "        Digest::new(self, value)",
        "        Digest::<u%s, Table<L>>::new(self, value)" % width,
    )
