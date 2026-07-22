//! Std-API polyfills so the shared engine compiles on **Rust 1.73** — the last
//! toolchain whose prebuilt `x86_64-apple-darwin` std targets macOS 10.7 (the
//! vintage build in `rb-cli-vintage/`). 1.74+ raised the macOS floor to 10.12.
//!
//! This module is compiled **only** when the `rust173-polyfill` feature is on,
//! which is enabled solely by the vintage manifest. The desktop build omits it
//! entirely and uses the real std methods — so nothing modern is lost there.
//!
//! ## Why an extension trait works for `is_multiple_of`
//!
//! `<uN>::is_multiple_of` was stabilized in Rust 1.87 and does **not exist at
//! all** on 1.73 (calling it there is `E0599: no method found`, not merely an
//! unstable-feature gate). So there is no inherent method to shadow, and the
//! trait method below resolves cleanly with the exact same `x.is_multiple_of(n)`
//! call syntax used throughout the codebase — no call sites change.
//!
//! Do **not** enable `rust173-polyfill` on a modern toolchain: there the std
//! inherent method also exists and you'd get an ambiguity error. It is for the
//! 1.73 vintage build only.
//!
//! (`io::Error::other`, by contrast, *does* exist on 1.73 as an unstable
//! inherent fn, so the same trick can't shadow it — those call sites are
//! rewritten to the equivalent `crate::compat::io_other(…)`, which is
//! stable on every toolchain. And trait-upcasting `dyn EditableFilesystem` ->
//! `dyn Filesystem` uses an explicit `as_filesystem()` method rather than the
//! 1.86 upcast coercion.)

/// Polyfill of the Rust 1.87 `is_multiple_of` inherent method for the unsigned
/// integer types (the only ones std provides it on). Matches std's semantics,
/// including the `rhs == 0` edge cases.
pub trait IntIsMultipleOf: Copy {
    /// Returns `true` if `self` is a multiple of `rhs`.
    ///
    /// Mirrors std: `n.is_multiple_of(0)` is `true` iff `n == 0`.
    fn is_multiple_of(self, rhs: Self) -> bool;
}

macro_rules! impl_is_multiple_of {
    ($($t:ty),* $(,)?) => {
        $(
            impl IntIsMultipleOf for $t {
                #[inline]
                fn is_multiple_of(self, rhs: $t) -> bool {
                    if rhs == 0 {
                        self == 0
                    } else {
                        self % rhs == 0
                    }
                }
            }
        )*
    };
}

impl_is_multiple_of!(u8, u16, u32, u64, u128, usize);

/// Polyfill of `Option::is_none_or` (stabilized Rust 1.82; absent on 1.73, so
/// the extension method resolves with no inherent to shadow it). Keeps the
/// `opt.is_none_or(f)` call syntax identical to the desktop build, where it
/// resolves to std's method.
pub trait OptionIsNoneOr<T> {
    /// Returns `true` if the option is `None` or `f` returns `true` for the
    /// contained value.
    fn is_none_or(self, f: impl FnOnce(T) -> bool) -> bool;
}

impl<T> OptionIsNoneOr<T> for Option<T> {
    #[inline]
    fn is_none_or(self, f: impl FnOnce(T) -> bool) -> bool {
        match self {
            None => true,
            Some(v) => f(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IntIsMultipleOf;

    #[test]
    fn matches_std_semantics() {
        assert!(12u32.is_multiple_of(4));
        assert!(!13u32.is_multiple_of(4));
        assert!(0u32.is_multiple_of(0));
        assert!(!5u32.is_multiple_of(0));
        assert!(0u64.is_multiple_of(7));
        assert!(1024usize.is_multiple_of(512));
    }
}
