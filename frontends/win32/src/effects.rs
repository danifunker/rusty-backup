//! Fulfils controller [`Effect`]s: pickers, questions, prompts, the clipboard, URLs.
//!
//! A question is a modal system dialog (Task Dialog, the common file dialog) or the small prompt
//! window below; each runs its own message loop, so the controllers are never borrowed while one
//! is up, and a wake that arrives meanwhile ticks them as usual. A dialog asked for while another
//! is up waits its turn in a queue. The flow suite answers pickers through `controller::picks`.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::rc::{Rc, Weak};

use rusty_backup::controller::{
    picks, ChoiceSpec, ConfirmSpec, Effect, PasswordSpec, PickSpec, TextSpec,
};
use windows::core::{w, Interface, PCWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::System::Com::{CoCreateInstance, CoTaskMemFree, CLSCTX_INPROC_SERVER};
use windows::Win32::System::DataExchange::{
    CloseClipboard, EmptyClipboard, OpenClipboard, SetClipboardData,
};
use windows::Win32::System::LibraryLoader::GetModuleHandleW;
use windows::Win32::System::Memory::{GlobalAlloc, GlobalLock, GlobalUnlock, GMEM_MOVEABLE};
use windows::Win32::System::Ole::CF_UNICODETEXT;
use windows::Win32::UI::Controls::{
    TaskDialogIndirect, EM_SETSEL, TASKDIALOGCONFIG, TASKDIALOG_BUTTON, TDCBF_CANCEL_BUTTON,
    TDF_ALLOW_DIALOG_CANCELLATION, TDF_POSITION_RELATIVE_TO_WINDOW, TD_INFORMATION_ICON,
    TD_WARNING_ICON,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::Input::KeyboardAndMouse::{EnableWindow, SetFocus};
use windows::Win32::UI::Shell::Common::COMDLG_FILTERSPEC;
use windows::Win32::UI::Shell::{
    FileOpenDialog, FileSaveDialog, IFileDialog, IFileOpenDialog, IFileSaveDialog, ShellExecuteW,
    FOS_ALLOWMULTISELECT, FOS_FORCEFILESYSTEM, FOS_OVERWRITEPROMPT, FOS_PICKFOLDERS,
    SIGDN_FILESYSPATH,
};
use windows::Win32::UI::WindowsAndMessaging::{
    CreateWindowExW, DefWindowProcW, DestroyWindow, DispatchMessageW, GetMessageW, GetWindowRect,
    IsDialogMessageW, IsWindow, PostMessageW, RegisterClassW, SendMessageW, SetWindowPos,
    TranslateMessage, BN_CLICKED, CW_USEDEFAULT, EN_CHANGE, ES_AUTOHSCROLL, ES_PASSWORD, HMENU,
    IDCANCEL, IDOK, MSG, SWP_NOZORDER, SW_SHOWNORMAL, WINDOW_EX_STYLE, WINDOW_STYLE, WM_CLOSE,
    WM_COMMAND, WM_DESTROY, WM_ERASEBKGND, WNDCLASSW, WS_CAPTION, WS_EX_CLIENTEDGE,
    WS_EX_DLGMODALFRAME, WS_POPUP, WS_SYSMENU, WS_TABSTOP,
};

use crate::layout::{self, Node, Rect, Size};
use crate::ui;
use rusty_backup::controller::remote_pick::asks_for_a_daemon;

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
    owner: HWND,
    /// Where a transient message shows (the status line under the tabs).
    status: HWND,
    busy: Cell<bool>,
    /// A question waits for its own message (`WM_APP_QUESTION`), not the one that raised it.
    posted: Cell<bool>,
    queue: RefCell<VecDeque<(Effect, ReplyFn)>>,
}

/// Posted to the effects window: open the questions that waited for a message of their own.
const WM_APP_QUESTION: u32 = windows::Win32::UI::WindowsAndMessaging::WM_APP + 2;

thread_local! {
    static LATER: RefCell<Vec<Weak<Inner>>> = const { RefCell::new(Vec::new()) };
    static LATER_WINDOW: Cell<isize> = const { Cell::new(0) };
}

/// A modal question; one raised inside a control's notification (a combo's selection) would hold
/// that notification, and an automation client waiting on it, until the user answers.
fn is_question(e: &Effect) -> bool {
    matches!(
        e,
        Effect::Confirm(_) | Effect::Choose(_) | Effect::AskText(_) | Effect::AskPassword(_)
    )
}

/// Ask for `inner`'s waiting questions on a message of their own.
fn post_later(inner: &Rc<Inner>) {
    use windows::Win32::UI::WindowsAndMessaging::HWND_MESSAGE;
    LATER.with(|l| l.borrow_mut().push(Rc::downgrade(inner)));
    let mut hwnd = LATER_WINDOW.with(Cell::get);
    if hwnd == 0 {
        // SAFETY: registering and creating our own message-only window once per thread.
        hwnd = unsafe {
            let name = w!("RustyBackupQuestions");
            let wc = WNDCLASSW {
                lpfnWndProc: Some(later_proc),
                hInstance: GetModuleHandleW(None).unwrap_or_default().into(),
                lpszClassName: name,
                ..Default::default()
            };
            RegisterClassW(&wc);
            CreateWindowExW(
                Default::default(),
                name,
                PCWSTR::null(),
                WINDOW_STYLE(0),
                0,
                0,
                0,
                0,
                Some(HWND_MESSAGE),
                None,
                GetModuleHandleW(None).ok().map(|i| i.into()),
                None,
            )
        }
        .map(|h| h.0 as isize)
        .unwrap_or(0);
        LATER_WINDOW.with(|w| w.set(hwnd));
    }
    // SAFETY: posting to our own window.
    let _ = unsafe { PostMessageW(Some(HWND(hwnd as _)), WM_APP_QUESTION, WPARAM(0), LPARAM(0)) };
}

unsafe extern "system" fn later_proc(hwnd: HWND, msg: u32, wp: WPARAM, lp: LPARAM) -> LRESULT {
    if msg != WM_APP_QUESTION {
        // SAFETY: the default handling.
        return unsafe { DefWindowProcW(hwnd, msg, wp, lp) };
    }
    let hosts = LATER.with(|l| std::mem::take(&mut *l.borrow_mut()));
    for inner in hosts.iter().filter_map(Weak::upgrade) {
        inner.posted.set(false);
        EffectHost { inner }.drain(true);
    }
    LRESULT(0)
}

impl EffectHost {
    pub fn new(owner: HWND, status: HWND) -> Self {
        EffectHost {
            inner: Rc::new(Inner {
                owner,
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

    /// Carry out `effects`; no `_` arm, so a new `Effect` fails to compile until handled here.
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

    /// Carry out the queue in order; `fresh` when on a message of its own, where a question may open.
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

    fn one(&self, effect: Effect, reply: &ReplyFn) {
        let owner = self.inner.owner;
        match effect {
            Effect::PickFile(spec) => {
                let path = pick(owner, &spec, false);
                reply(Reply::Picked {
                    purpose: spec.purpose,
                    path,
                });
            }
            Effect::PickFolder(spec) => {
                let path = pick(owner, &spec, true);
                reply(Reply::Picked {
                    purpose: spec.purpose,
                    path,
                });
            }
            Effect::Confirm(spec) => {
                let yes = confirm(owner, &spec);
                reply(Reply::Confirmed {
                    purpose: spec.purpose,
                    yes,
                });
            }
            Effect::Choose(spec) => {
                let choice = choose(owner, &spec);
                reply(Reply::Chose {
                    purpose: spec.purpose,
                    choice,
                });
            }
            Effect::AskText(spec) => {
                let text = Prompt::text(owner, &spec);
                reply(Reply::Answered {
                    purpose: spec.purpose,
                    text,
                });
            }
            Effect::AskPassword(spec) => {
                let text = Prompt::password(owner, &spec);
                reply(Reply::Answered {
                    purpose: spec.purpose,
                    text,
                });
            }
            Effect::Message(text) => ui::set_text(self.inner.status, &text),
            Effect::OpenUrl(url) => {
                let u = ui::wide(&url);
                // SAFETY: `u` outlives the call; the shell opens the default browser.
                unsafe {
                    ShellExecuteW(
                        Some(owner),
                        w!("open"),
                        PCWSTR(u.as_ptr()),
                        None,
                        None,
                        SW_SHOWNORMAL,
                    );
                }
            }
            Effect::CopyToClipboard(text) => copy_text(owner, &ui::plain(&text)),
            Effect::CloseScreen => reply(Reply::CloseScreen),
            Effect::Quit => {
                // SAFETY: the owner is the main window; destroying it ends the message loop.
                let _ = unsafe { DestroyWindow(owner) };
            }
        }
    }
}

impl EffectHost {
    /// Pick several host files; none when cancelled. A script answers `add_files` with one path.
    pub fn pick_files(&self, title: &str) -> Vec<PathBuf> {
        if let Some(answer) = picks::scripted("add_files") {
            return answer.into_iter().collect();
        }
        // SAFETY: COM is initialised on this thread; every string outlives the call that takes it.
        unsafe {
            let Ok(dialog) =
                CoCreateInstance::<_, IFileOpenDialog>(&FileOpenDialog, None, CLSCTX_INPROC_SERVER)
            else {
                return Vec::new();
            };
            let opts = dialog.GetOptions().unwrap_or_default()
                | FOS_FORCEFILESYSTEM
                | FOS_ALLOWMULTISELECT;
            let _ = dialog.SetOptions(opts);
            let t = ui::wide(title);
            let _ = dialog.SetTitle(PCWSTR(t.as_ptr()));
            if dialog.Show(Some(self.inner.owner)).is_err() {
                return Vec::new();
            }
            let Ok(items) = dialog.GetResults() else {
                return Vec::new();
            };
            let mut out = Vec::new();
            for i in 0..items.GetCount().unwrap_or(0) {
                let Ok(item) = items.GetItemAt(i) else {
                    continue;
                };
                if let Ok(name) = item.GetDisplayName(SIGDN_FILESYSPATH) {
                    if let Ok(path) = name.to_string() {
                        out.push(PathBuf::from(path));
                    }
                    CoTaskMemFree(Some(name.0 as *const _));
                }
            }
            out
        }
    }

    /// Put `text` on the clipboard.
    pub fn copy(&self, text: &str) {
        copy_text(self.inner.owner, &ui::plain(text));
    }
}

/// The common file dialog, or the flow suite's scripted answer.
fn pick(owner: HWND, spec: &PickSpec, folder: bool) -> Option<PathBuf> {
    if let Some(answer) = picks::scripted(spec.purpose) {
        return answer;
    }
    // SAFETY: COM is initialised on this thread; every string outlives the call that takes it.
    unsafe {
        let dialog: IFileDialog = if spec.save && !folder {
            let d: IFileSaveDialog =
                CoCreateInstance(&FileSaveDialog, None, CLSCTX_INPROC_SERVER).ok()?;
            d.cast().ok()?
        } else {
            let d: IFileOpenDialog =
                CoCreateInstance(&FileOpenDialog, None, CLSCTX_INPROC_SERVER).ok()?;
            d.cast().ok()?
        };
        let mut opts = dialog.GetOptions().ok()? | FOS_FORCEFILESYSTEM;
        if folder {
            opts |= FOS_PICKFOLDERS;
        }
        if spec.save {
            opts |= FOS_OVERWRITEPROMPT;
        }
        let _ = dialog.SetOptions(opts);
        let title = ui::wide(&spec.title);
        let _ = dialog.SetTitle(PCWSTR(title.as_ptr()));
        if !spec.file_name.is_empty() {
            let name = ui::wide(&spec.file_name);
            let _ = dialog.SetFileName(PCWSTR(name.as_ptr()));
        }
        // The filter strings live in `texts` until the dialog has read them.
        let mut texts: Vec<(Vec<u16>, Vec<u16>)> = Vec::new();
        if !folder {
            for (name, exts) in &spec.filters {
                let pattern = if exts.is_empty() {
                    "*.*".to_string()
                } else {
                    exts.iter()
                        .map(|e| format!("*.{e}"))
                        .collect::<Vec<_>>()
                        .join(";")
                };
                texts.push((ui::wide(name), ui::wide(&pattern)));
            }
            if !texts.is_empty() {
                texts.push((ui::wide("All files"), ui::wide("*.*")));
            }
        }
        let specs: Vec<COMDLG_FILTERSPEC> = texts
            .iter()
            .map(|(n, p)| COMDLG_FILTERSPEC {
                pszName: PCWSTR(n.as_ptr()),
                pszSpec: PCWSTR(p.as_ptr()),
            })
            .collect();
        if !specs.is_empty() {
            let _ = dialog.SetFileTypes(&specs);
        }
        dialog.Show(Some(owner)).ok()?;
        let item = dialog.GetResult().ok()?;
        let name = item.GetDisplayName(SIGDN_FILESYSPATH).ok()?;
        let path = name.to_string().ok();
        CoTaskMemFree(Some(name.0 as *const _));
        path.map(PathBuf::from)
    }
}

const FIRST_BUTTON: i32 = 100;

/// A Task Dialog with `buttons` (ids from [`FIRST_BUTTON`]) and Cancel; the button pressed, if any.
fn task_dialog(
    owner: HWND,
    title: &str,
    message: &str,
    buttons: &[String],
    warn: bool,
    cancel: bool,
) -> Option<usize> {
    let title_w = ui::wide(title);
    let message_w = ui::wide(message);
    let labels: Vec<Vec<u16>> = buttons.iter().map(|b| ui::wide(b)).collect();
    let specs: Vec<TASKDIALOG_BUTTON> = labels
        .iter()
        .enumerate()
        .map(|(i, l)| TASKDIALOG_BUTTON {
            nButtonID: FIRST_BUTTON + i as i32,
            pszButtonText: PCWSTR(l.as_ptr()),
        })
        .collect();
    let mut cfg = TASKDIALOGCONFIG {
        cbSize: std::mem::size_of::<TASKDIALOGCONFIG>() as u32,
        hwndParent: owner,
        dwFlags: TDF_ALLOW_DIALOG_CANCELLATION | TDF_POSITION_RELATIVE_TO_WINDOW,
        pszWindowTitle: PCWSTR(title_w.as_ptr()),
        pszContent: PCWSTR(message_w.as_ptr()),
        cButtons: specs.len() as u32,
        pButtons: specs.as_ptr(),
        nDefaultButton: if cancel { IDCANCEL.0 } else { FIRST_BUTTON },
        ..Default::default()
    };
    if cancel {
        cfg.dwCommonButtons = TDCBF_CANCEL_BUTTON;
    }
    cfg.Anonymous1.pszMainIcon = if warn {
        TD_WARNING_ICON
    } else {
        TD_INFORMATION_ICON
    };
    let mut pressed = 0i32;
    // SAFETY: every pointer in `cfg` points into locals that outlive the call.
    let ok = unsafe { TaskDialogIndirect(&cfg, Some(&mut pressed), None, None) };
    if ok.is_err() {
        return None;
    }
    let i = pressed - FIRST_BUTTON;
    (0..buttons.len() as i32).contains(&i).then_some(i as usize)
}

fn confirm(owner: HWND, spec: &ConfirmSpec) -> bool {
    task_dialog(
        owner,
        &spec.title,
        &spec.message,
        std::slice::from_ref(&spec.confirm_label),
        spec.destructive,
        true,
    )
    .is_some()
}

fn choose(owner: HWND, spec: &ChoiceSpec) -> usize {
    let labels: Vec<String> = spec.choices.iter().map(|c| c.label.clone()).collect();
    task_dialog(owner, &spec.title, &spec.message, &labels, false, false).unwrap_or(spec.dismiss)
}

/// A question the shell asks itself (not the controller): the button picked, `None` for Cancel.
pub fn ask(owner: HWND, title: &str, message: &str, buttons: &[String]) -> Option<usize> {
    task_dialog(owner, title, message, buttons, false, true)
}

/// An information box with OK, for the About window and other plain notes.
pub fn note(owner: HWND, title: &str, message: &str) {
    task_dialog(owner, title, message, &["OK".to_string()], false, false);
}

fn copy_text(owner: HWND, text: &str) {
    let w = ui::wide(text);
    let bytes = w.len() * 2;
    // SAFETY: the clipboard owns the global block once SetClipboardData succeeds.
    unsafe {
        if OpenClipboard(Some(owner)).is_err() {
            return;
        }
        let _ = EmptyClipboard();
        if let Ok(mem) = GlobalAlloc(GMEM_MOVEABLE, bytes) {
            let p = GlobalLock(mem) as *mut u16;
            if !p.is_null() {
                std::ptr::copy_nonoverlapping(w.as_ptr(), p, w.len());
                let _ = GlobalUnlock(mem);
                let _ = SetClipboardData(
                    CF_UNICODETEXT.0 as u32,
                    Some(windows::Win32::Foundation::HANDLE(mem.0)),
                );
            }
        }
        let _ = CloseClipboard();
    }
}

/// The one-line text or password prompt: a small owned window with its own loop.
struct Prompt {
    edit: HWND,
    error: HWND,
    ok: HWND,
    cancel: HWND,
    spec: Option<TextSpec>,
    answer: Option<String>,
    done: bool,
}

thread_local! {
    static PROMPT: RefCell<Option<Prompt>> = const { RefCell::new(None) };
}

impl Prompt {
    fn text(owner: HWND, spec: &TextSpec) -> Option<String> {
        Self::run(
            owner,
            &spec.title,
            &spec.prompt,
            &spec.initial,
            &spec.ok_label,
            false,
            asks_for_a_daemon(spec.purpose),
            Some(spec.clone()),
        )
    }

    fn password(owner: HWND, spec: &PasswordSpec) -> Option<String> {
        Self::run(owner, "Password", &spec.prompt, "", "OK", true, false, None)
    }

    fn run(
        owner: HWND,
        title: &str,
        prompt: &str,
        initial: &str,
        ok: &str,
        secret: bool,
        network: bool,
        spec: Option<TextSpec>,
    ) -> Option<String> {
        // SAFETY: plain window creation and a modal loop over the owner, which is disabled meanwhile.
        unsafe {
            let inst = GetModuleHandleW(None).ok()?;
            let class = w!("RustyBackupPrompt");
            let wc = WNDCLASSW {
                lpfnWndProc: Some(prompt_proc),
                hInstance: inst.into(),
                lpszClassName: class,
                ..Default::default()
            };
            RegisterClassW(&wc);
            let t = ui::wide(title);
            let window = CreateWindowExW(
                WS_EX_DLGMODALFRAME,
                class,
                PCWSTR(t.as_ptr()),
                WS_POPUP | WS_CAPTION | WS_SYSMENU,
                CW_USEDEFAULT,
                CW_USEDEFAULT,
                0,
                0,
                Some(owner),
                Some(HMENU(std::ptr::null_mut())),
                Some(inst.into()),
                None,
            )
            .ok()?;
            crate::theme::title_bar(window);
            let label = ui::label(window, prompt);
            let style =
                ES_AUTOHSCROLL as u32 | WS_TABSTOP.0 | if secret { ES_PASSWORD as u32 } else { 0 };
            let edit = ui::child(
                window,
                w!("EDIT"),
                initial,
                WINDOW_STYLE(style),
                WS_EX_CLIENTEDGE,
                "prompt.text",
            );
            let error = ui::label(window, "");
            let okb = ui::button(window, ok, "prompt.ok");
            let cancel = ui::button(window, "Cancel", "prompt.cancel");
            let dpi = GetDpiForWindow(window).max(96);
            let form = Node::column(vec![
                Node::leaf(label, Size::Stretch(1), Size::Fixed(layout::ROW - 8)),
                Node::field(edit),
                Node::leaf(error, Size::Stretch(1), Size::Fixed(layout::ROW - 8)),
                Node::row(vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(okb, 75),
                    Node::button(cancel, 75),
                ]),
            ]);
            // A daemon's address: two opposing arrows ("Switch") from the system icon font.
            let icon_font = network
                .then(|| ui::icon_font(layout::scale(32, dpi)))
                .flatten();
            let icon = icon_font.map(|font| {
                let style = WINDOW_STYLE(0x1); // SS_CENTER
                let h = ui::child(
                    window,
                    w!("STATIC"),
                    "\u{E8AB}",
                    style,
                    WINDOW_EX_STYLE(0),
                    "prompt.icon",
                );
                ui::set_font(h, font);
                h
            });
            let tree = match icon {
                Some(h) => Node::inset(
                    Node::row(vec![Node::leaf(h, Size::Fixed(44), Size::Fixed(44)), form]),
                    layout::MARGIN,
                ),
                None => Node::inset(form, layout::MARGIN),
            };
            let width = if icon.is_some() { 410 } else { 360 };
            let (cw, ch) = (layout::scale(width, dpi), layout::scale(140, dpi));
            let mut r = Default::default();
            let _ = GetWindowRect(owner, &mut r);
            let x = r.left + ((r.right - r.left) - cw) / 2;
            let y = r.top + ((r.bottom - r.top) - ch) / 3;
            let _ = SetWindowPos(window, None, x, y, cw, ch, SWP_NOZORDER);
            let mut client = Default::default();
            let _ = windows::Win32::UI::WindowsAndMessaging::GetClientRect(window, &mut client);
            for (h, rc) in layout::layout(
                &tree,
                Rect {
                    x: 0,
                    y: 0,
                    w: client.right,
                    h: client.bottom,
                },
                dpi,
            ) {
                let _ = SetWindowPos(h, None, rc.x, rc.y, rc.w, rc.h, SWP_NOZORDER);
            }
            PROMPT.with(|p| {
                *p.borrow_mut() = Some(Prompt {
                    edit,
                    error,
                    ok: okb,
                    cancel,
                    spec,
                    answer: None,
                    done: false,
                });
            });
            Prompt::check();
            let _ = EnableWindow(owner, false);
            let _ = windows::Win32::UI::WindowsAndMessaging::ShowWindow(window, SW_SHOWNORMAL);
            let _ = SetFocus(Some(edit));
            SendMessageW(edit, EM_SETSEL, Some(WPARAM(0)), Some(LPARAM(-1)));
            let mut msg = MSG::default();
            while PROMPT.with(|p| p.borrow().as_ref().is_some_and(|p| !p.done)) {
                if !GetMessageW(&mut msg, None, 0, 0).as_bool() {
                    // WM_QUIT belongs to the main loop: put it back.
                    let _ = PostMessageW(None, msg.message, msg.wParam, msg.lParam);
                    break;
                }
                if !IsDialogMessageW(window, &msg).as_bool() {
                    let _ = TranslateMessage(&msg);
                    DispatchMessageW(&msg);
                }
            }
            let _ = EnableWindow(owner, true);
            let answer = PROMPT
                .with(|p| p.borrow_mut().take())
                .and_then(|p| p.answer);
            if IsWindow(Some(window)).as_bool() {
                let _ = DestroyWindow(window);
            }
            if let Some(font) = icon_font {
                let _ = windows::Win32::Graphics::Gdi::DeleteObject(
                    windows::Win32::Graphics::Gdi::HGDIOBJ(font.0),
                );
            }
            let _ = SetFocus(Some(owner));
            answer
        }
    }

    /// Run the controller's validator on the field: the error shows and OK follows it.
    fn check() {
        PROMPT.with(|p| {
            let p = p.borrow();
            let Some(p) = p.as_ref() else { return };
            let text = ui::text(p.edit);
            let verdict = p.spec.as_ref().map_or(Ok(()), |s| s.validate(&text));
            let shown = match (&verdict, p.spec.as_ref().and_then(|s| s.error.clone())) {
                (Err(e), _) => e.clone(),
                (Ok(()), Some(e)) if p.spec.as_ref().is_some_and(|s| s.initial == text) => e,
                _ => String::new(),
            };
            ui::set_text(p.error, &shown);
            ui::set_enabled(p.ok, verdict.is_ok());
        });
    }

    fn finish(ok: bool) {
        PROMPT.with(|p| {
            if let Some(p) = p.borrow_mut().as_mut() {
                if ok {
                    let text = ui::text(p.edit);
                    if p.spec
                        .as_ref()
                        .map_or(Ok(()), |s| s.validate(&text))
                        .is_err()
                    {
                        return;
                    }
                    p.answer = Some(text);
                }
                p.done = true;
            }
        });
    }
}

unsafe extern "system" fn prompt_proc(hwnd: HWND, msg: u32, wp: WPARAM, lp: LPARAM) -> LRESULT {
    match msg {
        WM_COMMAND => {
            let code = ((wp.0 >> 16) & 0xffff) as u32;
            let from = HWND(lp.0 as _);
            let id = (wp.0 & 0xffff) as i32;
            let (ok, cancel, edit) = PROMPT.with(|p| {
                p.borrow()
                    .as_ref()
                    .map_or((HWND::default(), HWND::default(), HWND::default()), |p| {
                        (p.ok, p.cancel, p.edit)
                    })
            });
            if code == EN_CHANGE && from == edit {
                Prompt::check();
            } else if (code == BN_CLICKED && from == ok) || (id == IDOK.0 && from.is_invalid()) {
                Prompt::finish(true);
            } else if (code == BN_CLICKED && from == cancel) || id == IDCANCEL.0 {
                Prompt::finish(false);
            }
            LRESULT(0)
        }
        WM_CLOSE => {
            Prompt::finish(false);
            LRESULT(0)
        }
        WM_DESTROY => LRESULT(0),
        WM_ERASEBKGND => {
            let mut rc = windows::Win32::Foundation::RECT::default();
            // SAFETY: painting our own window's background with the mode's brush.
            unsafe {
                let _ = windows::Win32::UI::WindowsAndMessaging::GetClientRect(hwnd, &mut rc);
                windows::Win32::Graphics::Gdi::FillRect(
                    windows::Win32::Graphics::Gdi::HDC(wp.0 as _),
                    &rc,
                    crate::theme::window_brush(),
                );
            }
            LRESULT(1)
        }
        msg if crate::theme::ctl_color(msg, wp, lp, false).is_some() => {
            crate::theme::ctl_color(msg, wp, lp, false).unwrap_or_default()
        }
        // SAFETY: the default handling of a window we registered.
        _ => unsafe { DefWindowProcW(hwnd, msg, wp, lp) },
    }
}
