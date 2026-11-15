//! The PowerPC build's crate root (docs/cocoa_vintage_shell_plan.md § 3): mrustc links a binary,
//! not a staticlib, so Rust owns `main` and hands it to the Objective-C host's `rb_objc_main`,
//! which runs the app and calls back into the same C ABI the 10.7 staticlib exports.

include!("app.rs");

// librbcocoahost.a is host.o + main.o, built on the PowerPC Mac (scripts/build-ppc-cocoa.sh).
#[link(name = "rbcocoahost", kind = "static")]
#[link(name = "Cocoa", kind = "framework")]
#[link(name = "objc")]
extern "C" {
    fn rb_objc_main(argc: i32, argv: *const *const c_char) -> i32;
}

fn main() {
    // mrustc keeps only what main reaches, and only the host calls these: take their addresses.
    let entry = [
        rb_version as usize,
        rb_start as usize,
        rb_tick as usize,
        rb_command as usize,
        rb_open_files as usize,
        host::rb_event as usize,
        host::rb_window_event as usize,
        host::rb_later as usize,
        host::rb_timed as usize,
        host::rb_canvas_draw as usize,
        host::rb_rows_event as usize,
        host::rb_dropped as usize,
    ];
    std::hint::black_box(&entry);
    let args: Vec<std::ffi::CString> = std::env::args_os()
        .map(|a| std::ffi::CString::new(a.to_string_lossy().into_owned()).unwrap_or_default())
        .collect();
    let mut argv: Vec<*const c_char> = args.iter().map(|a| a.as_ptr()).collect();
    argv.push(std::ptr::null());
    // SAFETY: argv lives until the app returns; the host only reads it.
    let code = unsafe { rb_objc_main(args.len() as i32, argv.as_ptr()) };
    std::process::exit(code);
}
