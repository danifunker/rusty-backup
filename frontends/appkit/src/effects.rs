//! The AppKit panels behind the shared effect host (frontends/mac-screens/src/effects.rs):
//! open and save panels, alerts, the one-line prompt, the pasteboard. The queue and the rule that
//! a question opens on a turn of its own are the shared host's; these are its `Host` leaves.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use objc2::rc::Retained;
use objc2::runtime::{NSObject, NSObjectProtocol, ProtocolObject, Sel};
use objc2::{define_class, msg_send, sel, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSAlert, NSAlertFirstButtonReturn, NSAlertStyle, NSColor, NSControl,
    NSControlTextEditingDelegate, NSModalResponseOK, NSOpenPanel, NSPasteboard,
    NSPasteboardTypeString, NSSavePanel, NSSecureTextField, NSTextField, NSTextFieldDelegate,
    NSTextView,
};
use objc2_foundation::{NSNotification, NSPoint, NSRect, NSSize, NSString, NSURL};
use rusty_backup::controller::app::picker_start_folder;
use rusty_backup::controller::remote_pick::asks_for_a_daemon;
use rusty_backup::controller::{ChoiceSpec, ConfirmSpec, PasswordSpec, PickSpec, TextSpec};

use crate::ui::{self, ns};

pub use crate::screens::effects::{EffectHost, Reply, ReplyFn};

/// Put `text` on the general pasteboard.
pub fn copy_text(text: &str) {
    let pb = NSPasteboard::generalPasteboard();
    pb.clearContents();
    // SAFETY: a constant AppKit defines.
    pb.setString_forType(&ns(&ui::plain(text)), unsafe { NSPasteboardTypeString });
}

fn file_url(path: &std::path::Path) -> Retained<NSURL> {
    NSURL::fileURLWithPath(&ns(&path.to_string_lossy()))
}

fn path_of(url: Option<Retained<NSURL>>) -> Option<PathBuf> {
    url.and_then(|u| u.path())
        .map(|p| PathBuf::from(p.to_string()))
}

/// The open or save panel; the flow suite's scripted answers are the shared effect host's.
#[allow(deprecated)] // allowedFileTypes: its replacement needs macOS 11, the floor is 10.13
pub(crate) fn pick(mtm: MainThreadMarker, spec: &PickSpec, folder: bool) -> Option<PathBuf> {
    let exts: Vec<Retained<NSString>> = spec
        .filters
        .iter()
        .flat_map(|(_, e)| e.iter())
        .map(|e| ns(e.trim_start_matches("*.").trim_start_matches('.')))
        .collect();
    let any = exts.is_empty() || exts.iter().any(|e| e.to_string() == "*");
    let start = picker_start_folder();
    if spec.save && !folder {
        let panel = NSSavePanel::savePanel(mtm);
        panel.setMessage(Some(&ns(&spec.title)));
        panel.setCanCreateDirectories(true);
        if !spec.file_name.is_empty() {
            panel.setNameFieldStringValue(&ns(&spec.file_name));
        }
        if let Some(dir) = &start {
            panel.setDirectoryURL(Some(&file_url(dir)));
        }
        return (panel.runModal() == NSModalResponseOK)
            .then(|| path_of(panel.URL()))
            .flatten();
    }
    let panel = NSOpenPanel::openPanel(mtm);
    panel.setMessage(Some(&ns(&spec.title)));
    panel.setCanChooseFiles(!folder);
    panel.setCanChooseDirectories(folder);
    panel.setCanCreateDirectories(folder);
    panel.setAllowsMultipleSelection(false);
    if !folder && !any {
        panel.setAllowedFileTypes(Some(&objc2_foundation::NSArray::from_retained_slice(&exts)));
    }
    if let Some(dir) = &start {
        panel.setDirectoryURL(Some(&file_url(dir)));
    }
    if panel.runModal() != NSModalResponseOK {
        return None;
    }
    path_of(panel.URLs().firstObject())
}

/// An open panel for several files; none when cancelled.
pub(crate) fn pick_files(mtm: MainThreadMarker, title: &str) -> Vec<PathBuf> {
    let panel = NSOpenPanel::openPanel(mtm);
    panel.setMessage(Some(&ns(title)));
    panel.setCanChooseFiles(true);
    panel.setAllowsMultipleSelection(true);
    if panel.runModal() != NSModalResponseOK {
        return Vec::new();
    }
    panel
        .URLs()
        .iter()
        .filter_map(|u| u.path())
        .map(|p| PathBuf::from(p.to_string()))
        .collect()
}

fn alert(mtm: MainThreadMarker, title: &str, message: &str) -> Retained<NSAlert> {
    let a = NSAlert::new(mtm);
    a.setMessageText(&ns(&ui::plain(title)));
    a.setInformativeText(&ns(&ui::plain(message)));
    a
}

/// A one-button note (About), opened from a block of its own like every other question.
pub fn note(title: String, text: String) {
    dispatch2::DispatchQueue::main().exec_async(move || {
        let Some(mtm) = MainThreadMarker::new() else {
            return;
        };
        let a = alert(mtm, &title, &text);
        a.addButtonWithTitle(&ns("OK"));
        a.runModal();
    });
}

/// A question the shell itself asks (which pane takes a drop): the button picked, Cancel none.
pub fn ask(mtm: MainThreadMarker, title: &str, message: &str, buttons: &[String]) -> Option<usize> {
    let a = alert(mtm, title, message);
    for b in buttons {
        a.addButtonWithTitle(&ns(b));
    }
    a.addButtonWithTitle(&ns("Cancel"));
    let i = (a.runModal() - NSAlertFirstButtonReturn).max(0) as usize;
    (i < buttons.len()).then_some(i)
}

pub(crate) fn confirm(mtm: MainThreadMarker, spec: &ConfirmSpec) -> bool {
    let a = alert(mtm, &spec.title, &spec.message);
    a.addButtonWithTitle(&ns(&spec.confirm_label));
    a.addButtonWithTitle(&ns("Cancel"));
    if spec.destructive {
        a.setAlertStyle(NSAlertStyle::Critical);
    }
    a.runModal() == NSAlertFirstButtonReturn
}

pub(crate) fn choose(mtm: MainThreadMarker, spec: &ChoiceSpec) -> usize {
    let a = alert(mtm, &spec.title, &spec.message);
    for c in &spec.choices {
        a.addButtonWithTitle(&ns(&c.label));
    }
    // Esc gives the controller's dismiss answer.
    if let Some(b) = a
        .buttons()
        .iter()
        .nth(spec.dismiss)
        .filter(|_| spec.dismiss > 0)
    {
        b.setKeyEquivalent(&ns("\u{1b}"));
    }
    let i = (a.runModal() - NSAlertFirstButtonReturn).max(0) as usize;
    if i < spec.choices.len() {
        i
    } else {
        spec.dismiss
    }
}

define_class!(
    /// Runs the prompt's validator as the user types.
    #[unsafe(super(NSObject))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBPromptDelegate"]
    #[ivars = RefCell<(Option<Box<dyn Fn()>>, Option<Box<dyn Fn()>>)>]
    struct PromptDelegate;

    unsafe impl NSObjectProtocol for PromptDelegate {}
    unsafe impl NSControlTextEditingDelegate for PromptDelegate {}
    unsafe impl NSTextFieldDelegate for PromptDelegate {}

    impl PromptDelegate {
        #[unsafe(method(controlTextDidChange:))]
        fn changed(&self, _note: &NSNotification) {
            if let Some(f) = &self.ivars().borrow().0 {
                f();
            }
        }

        /// Return in the field presses the alert's default button.
        #[unsafe(method(control:textView:doCommandBySelector:))]
        fn command(&self, _control: &NSControl, _view: &NSTextView, sel: Sel) -> bool {
            match (sel == sel!(insertNewline:), &self.ivars().borrow().1) {
                (true, Some(f)) => {
                    f();
                    true
                }
                _ => false,
            }
        }
    }
);

/// What is wrong with a prompt's text, if anything.
type Check = Rc<dyn Fn(&str) -> Result<(), String>>;

/// A one-line prompt in an alert.
struct Ask<'a> {
    title: &'a str,
    message: &'a str,
    initial: &'a str,
    ok: &'a str,
    secure: bool,
    /// The address of a remote daemon: the alert shows the system's network icon.
    network: bool,
    check: Check,
    first_error: Option<String>,
}

/// A connection's icon: two passing arrows in a circle (a system symbol, macOS 11 and later),
/// else the system network icon.
fn connection_icon() -> Option<Retained<objc2_app_kit::NSImage>> {
    use objc2_app_kit::NSImage;
    if objc2::available!(macos = 11.0) {
        let symbol = NSImage::imageWithSystemSymbolName_accessibilityDescription(
            &ns("arrow.left.arrow.right.circle"),
            Some(&ns("Connection")),
        );
        if let Some(symbol) = symbol {
            // SAFETY: a constant font weight AppKit exports.
            let weight = unsafe { objc2_app_kit::NSFontWeightRegular };
            let config =
                objc2_app_kit::NSImageSymbolConfiguration::configurationWithPointSize_weight(
                    48.0, weight,
                );
            return symbol
                .imageWithSymbolConfiguration(&config)
                .or(Some(symbol));
        }
    }
    // SAFETY: a constant image name AppKit exports.
    NSImage::imageNamed(unsafe { objc2_app_kit::NSImageNameNetwork })
}

fn prompt(mtm: MainThreadMarker, ask: Ask) -> Option<String> {
    let Ask {
        title,
        message,
        initial,
        ok,
        secure,
        network,
        check,
        first_error,
    } = ask;
    let a = alert(mtm, title, message);
    if let Some(icon) = network.then(connection_icon).flatten() {
        // SAFETY: the alert keeps its own reference to the image.
        unsafe { a.setIcon(Some(&icon)) };
    }
    a.addButtonWithTitle(&ns(ok));
    a.addButtonWithTitle(&ns("Cancel"));
    let buttons = a.buttons();
    let ok_button = buttons.firstObject()?;
    ui::set_id(&ok_button, "prompt.ok");
    if let Some(cancel) = buttons.lastObject() {
        ui::set_id(&cancel, "prompt.cancel");
    }
    let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(320.0, 22.0));
    let field: Retained<NSTextField> = if secure {
        Retained::into_super(NSSecureTextField::initWithFrame(mtm.alloc(), frame))
    } else {
        NSTextField::initWithFrame(mtm.alloc(), frame)
    };
    field.setStringValue(&ns(&ui::plain(initial)));
    ui::set_id(&field, "prompt.text");
    let error = ui::label(mtm, "");
    error.setTextColor(Some(&NSColor::systemRedColor()));
    let body = ui::flipped(mtm);
    body.setFrame(NSRect::new(
        NSPoint::new(0.0, 0.0),
        NSSize::new(320.0, 46.0),
    ));
    body.addSubview(&field);
    body.addSubview(&error);
    error.setFrame(NSRect::new(
        NSPoint::new(0.0, 26.0),
        NSSize::new(320.0, 18.0),
    ));
    // The alert's text is the field's caption, so a screen reader names the field by it.
    field.setAccessibilityLabel(Some(&ns(&ui::plain(message))));
    a.setAccessoryView(Some(&body));
    let delegate: Retained<PromptDelegate> = {
        let this = mtm
            .alloc::<PromptDelegate>()
            .set_ivars(RefCell::new((None, None)));
        // SAFETY: NSObject's plain init.
        unsafe { msg_send![super(this), init] }
    };
    let validate = {
        let (field, error, check) = (field.clone(), error.clone(), check.clone());
        move || {
            let verdict = check(&field.stringValue().to_string());
            // OK stays on: a value set through accessibility never runs this, and OK checks again.
            ui::set_text(&error, verdict.as_ref().err().map_or("", |e| e.as_str()));
        }
    };
    let submit = {
        let ok_button = ok_button.clone();
        move || {
            // SAFETY: a plain click on the alert's own button, with no sender.
            unsafe { ok_button.performClick(None) };
        }
    };
    *delegate.ivars().borrow_mut() = (Some(Box::new(validate.clone())), Some(Box::new(submit)));
    // SAFETY: the delegate outlives the field's use of it; it is cleared before both go.
    unsafe { field.setDelegate(Some(ProtocolObject::from_ref(&*delegate))) };
    a.window().setInitialFirstResponder(Some(&field));
    validate();
    if let Some(e) = first_error {
        ui::set_text(&error, &e);
    }
    let answer = loop {
        if a.runModal() != NSAlertFirstButtonReturn {
            break None;
        }
        let text = field.stringValue().to_string();
        match check(&text) {
            Ok(()) => break Some(text),
            Err(e) => ui::set_text(&error, &e),
        }
    };
    // SAFETY: clearing the delegate set above.
    unsafe { field.setDelegate(None) };
    answer
}

pub(crate) fn ask_text(mtm: MainThreadMarker, spec: &TextSpec) -> Option<String> {
    let rule = spec.clone();
    let check: Check = Rc::new(move |t: &str| (rule.validator)(&rule, t));
    let ask = Ask {
        title: &spec.title,
        message: &spec.prompt,
        initial: &spec.initial,
        ok: &spec.ok_label,
        secure: false,
        network: asks_for_a_daemon(spec.purpose),
        check,
        first_error: spec.error.clone(),
    };
    prompt(mtm, ask)
}

pub(crate) fn ask_password(mtm: MainThreadMarker, spec: &PasswordSpec) -> Option<String> {
    let ask = Ask {
        title: "Password",
        message: &spec.prompt,
        initial: "",
        ok: "OK",
        secure: true,
        network: false,
        check: Rc::new(|_| Ok(())),
        first_error: None,
    };
    prompt(mtm, ask)
}
