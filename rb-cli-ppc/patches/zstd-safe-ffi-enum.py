CRATE = "zstd-safe"
TARGETS = ["zstd-safe/src/lib.rs"]
GAP = """\
Call-site half of zstd-sys-ffi-enum: those entry points now take the
underlying integer rather than the enum, so the enum values need an explicit
`as u32`. Fieldless enums cast to their repr for free, so this changes no
value -- only the type mrustc sees at the FFI boundary.
"""
UPSTREAM = None

# Must name the LAST edit of the set: the runner skips a file whose
# APPLIED matches, so a partial marker would strand later edits.
APPLIED = r"end_op as u32,"
MATCH = r"ZSTD_(CCtx|DCtx)_setParameter\(self\.0\.as_ptr\(\), param,"

_SUBS = [
    ("ZSTD_CCtx_setParameter(self.0.as_ptr(), param, value)",
     "ZSTD_CCtx_setParameter(self.0.as_ptr(), param as u32, value)"),
    ("ZSTD_DCtx_setParameter(self.0.as_ptr(), param, value)",
     "ZSTD_DCtx_setParameter(self.0.as_ptr(), param as u32, value)"),
    ("ZSTD_CCtx_reset(self.0.as_ptr(), reset.as_sys())",
     "ZSTD_CCtx_reset(self.0.as_ptr(), reset.as_sys() as u32)"),
    ("ZSTD_DCtx_reset(self.0.as_ptr(), reset.as_sys())",
     "ZSTD_DCtx_reset(self.0.as_ptr(), reset.as_sys() as u32)"),
    # ZSTD_compressStream2's end_op arrives already typed as the sys enum.
    ("                end_op,\n", "                end_op as u32,\n"),
]


def patch(text, path):
    for old, new in _SUBS:
        text = text.replace(old, new)
    return text
