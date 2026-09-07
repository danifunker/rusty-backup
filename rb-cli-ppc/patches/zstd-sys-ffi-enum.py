CRATE = "zstd-sys"
TARGETS = ["zstd-sys/src/bindings_zstd.rs"]
GAP = """\
mrustc lowers a fieldless `#[repr(u32)]` enum to a one-field C struct

    struct e_..ZSTD_cParameter.. { uint32_t TAG; };

and emits `extern "C"` signatures taking it BY VALUE. The real C function
takes a plain C enum, i.e. an `int`. On SPARC V9 those disagree: a 4-byte
struct is passed left-aligned in the 8-byte parameter slot, an `int` in the
low half. `ZSTD_c_compressionLevel` (100) therefore arrives as
0x64_00000000 and zstd answers "Unsupported parameter", failing every
`--format zstd` backup on Solaris 9.

Invisible on 32-bit big-endian: on powerpc-apple-darwin the slot is 4 bytes
wide, so the struct and the int occupy the same bits. This is the first
64-bit big-endian mrustc target, which is why it surfaces here and the
PowerPC parity gate stayed green.

The fix declares the eight affected entry points as taking the underlying
integer instead of the enum, so mrustc emits a scalar and gcc handles the
ABI on both sides. Values are unchanged; see the zstd-safe-ffi-enum patch
for the matching casts at the call sites.

Properly this belongs in mrustc's codegen (any extern "C" fn taking a
fieldless enum by value is affected, not just zstd), and it is written up in
docs/build-sol9-mrustc.md for that. This patch unblocks the target.
"""
UPSTREAM = None

# Each entry point paired with the enum parameter to scalarise.
_SUBS = [
    ("code: ZSTD_ErrorCode,", "code: ::core::ffi::c_uint,"),
    ("cParam: ZSTD_cParameter)", "cParam: ::core::ffi::c_uint)"),
    ("dParam: ZSTD_dParameter)", "dParam: ::core::ffi::c_uint)"),
    ("param: ZSTD_cParameter,", "param: ::core::ffi::c_uint,"),
    ("param: ZSTD_dParameter,", "param: ::core::ffi::c_uint,"),
    ("reset: ZSTD_ResetDirective,", "reset: ::core::ffi::c_uint,"),
    ("endOp: ZSTD_EndDirective,", "endOp: ::core::ffi::c_uint,"),
]

APPLIED = r"param: ::core::ffi::c_uint,\n"
MATCH = r"param: ZSTD_cParameter,"


def patch(text, path):
    for old, new in _SUBS:
        text = text.replace(old, new)
    return text
