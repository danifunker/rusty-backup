CRATE = "zstd-safe"
TARGETS = ["zstd-safe/src/lib.rs"]
GAP = """\
zstd-safe passes its `OutBufferWrapper` / `InBufferWrapper` to

    fn ptr_mut<B>(ptr_void: &mut B) -> *mut B

as `ptr_mut(&mut output)`. `B` is only pinned by *deref-coercing*
`&mut OutBufferWrapper` to `&mut ZSTD_outBuffer` (the wrapper's `DerefMut`
target), driven by what the enclosing zstd_sys call expects. mrustc gets as
far as the autoderef and then aborts inside the coercion, with a bare C++
assertion and no span:

    autoderef: Deref OutBufferWrapper<..> into ZSTD_outBuffer_s
    check_unsize_tys: From? ZSTD_outBuffer_s
    mrustc: src/hir/type.hpp:236: as_Borrow(): Assertion `m_tag == TAG_Borrow' failed.

(`add_coerce_borrow` assumes the node it is handed is a borrow; on this path
it is the dereffed struct.) Writing the deref out -- `&mut *output` -- pins
`B` directly and removes the coercion. Same class of fix as the crc and
chrono turbofishes: say what mrustc cannot infer, change nothing else.
"""
UPSTREAM = None

APPLIED = r"ptr_mut\(&mut \*(output|input)\)"
MATCH = r"ptr_mut\(&mut (output|input)\)"


def patch(text, path):
    for name in ("output", "input"):
        text = text.replace("ptr_mut(&mut %s)" % name,
                            "ptr_mut(&mut *%s)" % name)
    return text
