//! The Gate 0 window: one form through the whole C ABI, both ways. Real screens replace it.

use std::cell::RefCell;
use std::rc::Rc;

use std::sync::Arc;

use rusty_backup::controller::app::{about, app_version, AppController};
use rusty_backup::controller::commander::Tone;

use crate::host::VintageHost;
use crate::screens::form::Form;
use crate::screens::host::Host;
use crate::screens::layout::{self, Node, Size};
use crate::screens::settings::SettingsWindow;
use crate::wake::VintageWake;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Title,
    Version,
    Summary,
    NameCaption,
    Name,
    Echo,
    Loud,
    Pick,
    Settings,
    Close,
}

struct Gate {
    host: Rc<dyn Host>,
    surface: u32,
    form: Form<K>,
    wake: Arc<VintageWake>,
    ctrl: RefCell<AppController>,
    settings: RefCell<Option<Rc<SettingsWindow>>>,
}

thread_local! {
    static GATE: RefCell<Option<Rc<Gate>>> = RefCell::new(None);
}

pub fn open() {
    let host: Rc<dyn Host> = VintageHost::get();
    let quit = host.clone();
    let surface = host.window("Rusty Backup", 460, 260, Box::new(move || quit.quit()));
    let form = Form::with_host(host.clone(), surface, Rc::new(fired));
    let f = &form;
    f.label(K::Title, "Rusty Backup");
    f.label(K::Version, &format!("Version {}", app_version()));
    f.wrap(K::Summary, about::SUMMARY);
    f.label(K::NameCaption, "Your name:");
    f.field(K::Name, "gate.name", "Your name", false);
    f.label(K::Echo, "");
    f.check(K::Loud, "Shout", "gate.loud");
    let greetings = vec!["Hello".to_string(), "Welcome".to_string(), "Hi".to_string()];
    f.popup(K::Pick, &greetings, "gate.pick");
    f.button(K::Settings, "Settings...", "gate.settings");
    f.button(K::Close, "Close", "gate.close");
    let wake = Arc::new(VintageWake::default());
    let ctrl = AppController::new(wake.clone());
    let gate = Rc::new(Gate {
        host,
        surface,
        form,
        wake,
        ctrl: RefCell::new(ctrl),
        settings: RefCell::new(None),
    });
    GATE.with(|g| *g.borrow_mut() = Some(gate.clone()));
    gate.refresh();
    gate.layout();
    gate.host.present(surface);
}

fn fired(k: K) {
    let gate = GATE.with(|g| g.borrow().clone());
    if let Some(gate) = gate {
        if k == K::Close {
            gate.host.quit();
            return;
        }
        if k == K::Settings {
            let recent = gate.ctrl.borrow().recent_handle();
            let w = SettingsWindow::open(gate.host.clone(), recent, Box::new(|| {}));
            *gate.settings.borrow_mut() = Some(w);
            return;
        }
        gate.refresh();
    }
}

/// Run the controllers, as a wake asks.
pub fn tick() {
    let gate = GATE.with(|g| g.borrow().clone());
    if let Some(gate) = gate {
        gate.wake.ticked();
        let _ = rusty_backup::controller::Controller::tick(&mut *gate.ctrl.borrow_mut());
    }
}

impl Gate {
    fn refresh(&self) {
        let f = &self.form;
        let name = f.text(K::Name);
        let greeting = match f.picked(K::Pick) {
            Some(1) => "Welcome",
            Some(2) => "Hi",
            _ => "Hello",
        };
        let mut echo = if name.is_empty() {
            String::new()
        } else {
            format!("{}, {}.", greeting, name)
        };
        if f.checked(K::Loud) {
            echo = echo.to_uppercase();
        }
        let tone = if f.checked(K::Loud) {
            Some(Tone::Warning)
        } else {
            None
        };
        let shown = if echo.is_empty() {
            None
        } else {
            Some(echo.as_str())
        };
        f.line(K::Echo, shown, tone);
    }

    fn layout(&self) {
        let row = Size::Fixed(layout::ROW);
        let tree = Node::inset(
            Node::column(vec![
                Node::leaf(K::Title, Size::Stretch(1), row),
                Node::leaf(K::Version, Size::Stretch(1), row),
                Node::leaf(K::Summary, Size::Stretch(1), Size::Fixed(layout::ROW * 2)),
                Node::row(vec![
                    Node::leaf(K::NameCaption, Size::Fixed(90), row),
                    Node::field(K::Name),
                ]),
                Node::row(vec![
                    Node::leaf(K::Loud, Size::Fixed(90), row),
                    Node::leaf(K::Pick, Size::Fixed(140), row),
                    Node::Space(Size::Stretch(1)),
                ]),
                Node::leaf(K::Echo, Size::Stretch(1), row),
                Node::Space(Size::Stretch(1)),
                Node::row(vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(K::Close, 90),
                ]),
            ]),
            layout::MARGIN,
        );
        // Every control stays laid out: a hidden one keeps its place for when it shows.
        self.form.place(&tree, self.host.area(self.surface));
    }
}
