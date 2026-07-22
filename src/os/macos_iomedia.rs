//! Modern (Rust 1.77+) `c"…"` C-string literal for the IOKit `IOMedia` class
//! name. Isolated in its own file on purpose: `c"…"` is a lexer-level token, so
//! the vintage macOS 10.7 build (Rust 1.73) would reject it even in a cfg'd-out
//! block. This file is behind `#[cfg(not(feature = "rust173-polyfill"))] mod`,
//! which the vintage build never loads — so 1.73 never lexes the literal. The
//! vintage build uses the byte-string form in `macos.rs` instead.

pub(super) fn iomedia_class_name() -> *const std::os::raw::c_char {
    c"IOMedia".as_ptr()
}
