//! Fulfils controller [`Effect`]s: pickers, questions, prompts, the clipboard, URLs.
//!
//! A question is a modal panel or alert, opened from a block of its own on the main queue: an AX
//! press waits for the action it pressed, so a modal raised inside that action would hold the flow
//! driver until the user answers. A modal runs its own loop, so the controllers are never borrowed
//! while one is up, and a wake that arrives meanwhile ticks them as usual. A question asked while
//! another is up waits its turn. The flow suite answers pickers through `controller::picks`.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::rc::{Rc, Weak};

use objc2::rc::Retained;
use objc2::runtime::{NSObject, NSObjectProtocol, ProtocolObject};
use objc2::{define_class, msg_send, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSAlert, NSAlertFirstButtonReturn, NSAlertStyle, NSApplication, NSColor,
    NSControlTextEditingDelegate, NSModalResponseOK, NSOpenPanel, NSPasteboard,
    NSPasteboardTypeString, NSSavePanel, NSSecureTextField, NSTextField, NSTextFieldDelegate,
    NSWorkspace,
};
use objc2_foundation::{NSNotification, NSPoint, NSRect, NSSize, NSString, NSURL};
use rusty_backup::controller::app::picker_start_folder;
use rusty_backup::controller::{
    picks, ChoiceSpec, ConfirmSpec, Effect, PasswordSpec, PickSpec, TextSpec,
};

use crate::ui::{self, ns};

/// An answer, routed by the screen that asked to its controller's matching `Action`.
// The screens of slices 1 to 8 read every field; until then only the app's Confirm is routed.
#[allow(dead_code)]
pub enum Reply {
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Confirmed {
        purpose: &'static str,
        yes: bool,
    },
    Answered {
        purpose: &'static str,
        text: Option<String>,
    },
    Chose {
        purpose: &'static str,
        choice: usize,
    },
    CloseScreen,
}

pub type ReplyFn = Rc<dyn Fn(Reply)>;

#[derive(Clone)]
pub struct EffectHost {
    inner: Rc<Inner>,
}

struct Inner {
    mtm: MainThreadMarker,
    /// Where a transient message shows (the status line under the screens).
    status: Retained<NSTextField>,
    busy: Cell<bool>,
    /// A question waits for a block of its own, not the action that raised it.
    posted: Cell<bool>,
    queue: RefCell<VecDeque<(Effect, ReplyFn)>>,
}

thread_local! {
    static LATER: RefCell<Vec<Weak<Inner>>> = const { RefCell::new(Vec::new()) };
}

fn is_question(e: &Effect) -> bool {
    matches!(
        e,
        Effect::PickFile(_)
            | Effect::PickFolder(_)
            | Effect::Confirm(_)
            | Effect::Choose(_)
            | Effect::AskText(_)
            | Effect::AskPassword(_)
    )
}

fn post_later(inner: &Rc<Inner>) {
    LATER.with(|l| l.borrow_mut().push(Rc::downgrade(inner)));
    dispatch2::DispatchQueue::main().exec_async(drain_later);
}

fn drain_later() {
    let hosts = LATER.with(|l| std::mem::take(&mut *l.borrow_mut()));
    for inner in hosts.iter().filter_map(Weak::upgrade) {
        inner.posted.set(false);
        EffectHost { inner }.drain(true);
    }
}

impl EffectHost {
    pub fn new(mtm: MainThreadMarker, status: Retained<NSTextField>) -> Self {
        EffectHost {
            inner: Rc::new(Inner {
                mtm,
                status,
                busy: Cell::new(false),
                posted: Cell::new(false),
                queue: RefCell::default(),
            }),
        }
    }

    /// True while a dialog the host opened is up.
    #[allow(dead_code)] // the screens' key handlers ask it from slice 1 on
    pub fn dialog_open(&self) -> bool {
        self.inner.busy.get()
    }

    /// Carry out `effects` in order; questions open on a block of their own.
    pub fn run(&self, effects: Vec<Effect>, reply: &ReplyFn) {
        for effect in effects {
            self.inner
                .queue
                .borrow_mut()
                .push_back((effect, reply.clone()));
        }
        if self.inner.busy.get() || self.inner.posted.get() {
            return;
        }
        self.drain(false);
    }

    fn drain(&self, fresh: bool) {
        loop {
            let next = self.inner.queue.borrow_mut().pop_front();
            let Some((effect, reply)) = next else { break };
            if !fresh && is_question(&effect) {
                self.inner.queue.borrow_mut().push_front((effect, reply));
                self.inner.posted.set(true);
                post_later(&self.inner);
                break;
            }
            self.inner.busy.set(true);
            self.one(effect, &reply);
            self.inner.busy.set(false);
        }
    }

    /// No `_` arm, so a new `Effect` fails to compile until handled here.
    fn one(&self, effect: Effect, reply: &ReplyFn) {
        let mtm = self.inner.mtm;
        match effect {
            Effect::PickFile(spec) => {
                let path = pick(mtm, &spec, false);
                reply(Reply::Picked {
                    purpose: spec.purpose,
                    path,
                });
            }
            Effect::PickFolder(spec) => {
                let path = pick(mtm, &spec, true);
                reply(Reply::Picked {
                    purpose: spec.purpose,
                    path,
                });
            }
            Effect::Confirm(spec) => {
                let yes = confirm(mtm, &spec);
                reply(Reply::Confirmed {
                    purpose: spec.purpose,
                    yes,
                });
            }
            Effect::Choose(spec) => {
                let choice = choose(mtm, &spec);
                reply(Reply::Chose {
                    purpose: spec.purpose,
                    choice,
                });
            }
            Effect::AskText(spec) => {
                let text = ask_text(mtm, &spec);
                reply(Reply::Answered {
                    purpose: spec.purpose,
                    text,
                });
            }
            Effect::AskPassword(spec) => {
                let text = ask_password(mtm, &spec);
                reply(Reply::Answered {
                    purpose: spec.purpose,
                    text,
                });
            }
            Effect::Message(text) => ui::set_text(&self.inner.status, &text),
            Effect::OpenUrl(url) => {
                if let Some(u) = NSURL::URLWithString(&ns(&url)) {
                    NSWorkspace::sharedWorkspace().openURL(&u);
                }
            }
            Effect::CopyToClipboard(text) => copy_text(&text),
            Effect::CloseScreen => reply(Reply::CloseScreen),
            Effect::Quit => NSApplication::sharedApplication(mtm).terminate(None),
        }
    }
}

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

/// The open or save panel, or the flow suite's scripted answer.
#[allow(deprecated)] // allowedFileTypes: its replacement needs macOS 11, the floor is 10.13
fn pick(mtm: MainThreadMarker, spec: &PickSpec, folder: bool) -> Option<PathBuf> {
    if let Some(answer) = picks::scripted(spec.purpose) {
        return answer;
    }
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

fn confirm(mtm: MainThreadMarker, spec: &ConfirmSpec) -> bool {
    let a = alert(mtm, &spec.title, &spec.message);
    a.addButtonWithTitle(&ns(&spec.confirm_label));
    a.addButtonWithTitle(&ns("Cancel"));
    if spec.destructive {
        a.setAlertStyle(NSAlertStyle::Critical);
    }
    a.runModal() == NSAlertFirstButtonReturn
}

fn choose(mtm: MainThreadMarker, spec: &ChoiceSpec) -> usize {
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
    #[ivars = RefCell<Option<Box<dyn Fn()>>>]
    struct PromptDelegate;

    unsafe impl NSObjectProtocol for PromptDelegate {}
    unsafe impl NSControlTextEditingDelegate for PromptDelegate {}
    unsafe impl NSTextFieldDelegate for PromptDelegate {}

    impl PromptDelegate {
        #[unsafe(method(controlTextDidChange:))]
        fn changed(&self, _note: &NSNotification) {
            if let Some(f) = &*self.ivars().borrow() {
                f();
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
    check: Check,
    first_error: Option<String>,
}

fn prompt(mtm: MainThreadMarker, ask: Ask) -> Option<String> {
    let Ask {
        title,
        message,
        initial,
        ok,
        secure,
        check,
        first_error,
    } = ask;
    let a = alert(mtm, title, message);
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
        let this = mtm.alloc::<PromptDelegate>().set_ivars(RefCell::new(None));
        // SAFETY: NSObject's plain init.
        unsafe { msg_send![super(this), init] }
    };
    let validate = {
        let (field, error, ok_button, check) = (
            field.clone(),
            error.clone(),
            ok_button.clone(),
            check.clone(),
        );
        move || {
            let verdict = check(&field.stringValue().to_string());
            ui::set_text(&error, verdict.as_ref().err().map_or("", |e| e.as_str()));
            ui::set_enabled(&ok_button, verdict.is_ok());
        }
    };
    *delegate.ivars().borrow_mut() = Some(Box::new(validate.clone()));
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

fn ask_text(mtm: MainThreadMarker, spec: &TextSpec) -> Option<String> {
    let rule = spec.clone();
    let check: Check = Rc::new(move |t: &str| (rule.validator)(&rule, t));
    let ask = Ask {
        title: &spec.title,
        message: &spec.prompt,
        initial: &spec.initial,
        ok: &spec.ok_label,
        secure: false,
        check,
        first_error: spec.error.clone(),
    };
    prompt(mtm, ask)
}

fn ask_password(mtm: MainThreadMarker, spec: &PasswordSpec) -> Option<String> {
    let ask = Ask {
        title: "Password",
        message: &spec.prompt,
        initial: "",
        ok: "OK",
        secure: true,
        check: Rc::new(|_| Ok(())),
        first_error: None,
    };
    prompt(mtm, ask)
}
