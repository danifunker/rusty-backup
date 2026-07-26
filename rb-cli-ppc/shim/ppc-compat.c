/*
 * ppc-compat.c -- the handful of libc/libm entry points that Rust's libstd
 * references but Mac OS X 10.4/10.5 on PowerPC does not export.
 *
 * This is deliberately tiny. Anything broader (device enumeration, raw disk
 * I/O) belongs in the hand-written platform shell, not here; this file exists
 * only so the *standard library* links. Each entry documents which SDK first
 * shipped the real function, so entries can be dropped if the floor ever moves.
 *
 * Built and linked by scripts/ppc-cc-remote.py (see PPC_SHIM).
 */

#include <math.h>

/*
 * Leopard's <math.h> only declares `lgamma_r` under _REENTRANT-ish feature
 * macros that this translation unit does not opt into, so declare it here
 * rather than perturb the feature-macro state for the whole file.
 */
extern double lgamma_r(double, int *);

/*
 * `lgammaf_r` -- referenced by `core`/`std`'s `f32::ln_gamma`.
 *
 * Leopard's libm exports `lgamma`, `lgammaf` and the reentrant `lgamma_r`, but
 * not the float reentrant form (it arrived later). Widening to double and back
 * is what the platforms that do ship it effectively do for this function, and
 * `ln_gamma` is an unstable API that nothing in the engine calls -- this exists
 * purely to satisfy the linker.
 */
float lgammaf_r(float x, int *signgamp)
{
    return (float)lgamma_r((double)x, signgamp);
}
