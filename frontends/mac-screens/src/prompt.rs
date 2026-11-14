//! A one-line prompt as a modal window on a [`Form`], for a host whose alerts take no accessory
//! view (10.4). Its controls carry the ids the AppKit prompt does, so one flow drives both.

use std::cell::RefCell;
use std::rc::Rc;

use rusty_backup::controller::commander::Tone;

use super::form::Form;
use super::host::Host;
use super::layout::{self, Node, Size};

/// What is wrong with a prompt's text, if anything.
pub type Check = Rc<dyn Fn(&str) -> Result<(), String>>;

/// One prompt's wording and rule.
pub struct Ask<'a> {
    pub title: &'a str,
    pub message: &'a str,
    pub initial: &'a str,
    pub ok: &'a str,
    pub secure: bool,
    pub check: Check,
    pub first_error: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Message,
    Text,
    Error,
    Ok,
    Cancel,
}

/// The text the user confirmed, checked by `ask.check`; `None` when cancelled.
pub fn ask(host: &Rc<dyn Host>, ask: Ask) -> Option<String> {
    let answer: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
    let form_cell: Rc<RefCell<Option<Rc<Form<K>>>>> = Rc::new(RefCell::new(None));
    let stopper = host.clone();
    let s = host.window(ask.title, 380, 150, Box::new(move || stopper.stop_modal()));
    let check = ask.check.clone();
    let (cell, done, h) = (form_cell.clone(), answer.clone(), host.clone());
    let form = Rc::new(Form::with_host(
        host.clone(),
        s,
        Rc::new(move |k| {
            let f = match cell.borrow().clone() {
                Some(f) => f,
                None => return,
            };
            match k {
                K::Text => {
                    let verdict = check(&f.text(K::Text));
                    f.set_text(K::Error, verdict.as_ref().err().map_or("", |e| e.as_str()));
                }
                K::Ok => {
                    let text = f.text(K::Text);
                    match check(&text) {
                        Ok(()) => {
                            *done.borrow_mut() = Some(text);
                            h.stop_modal();
                        }
                        Err(e) => f.set_text(K::Error, &e),
                    }
                }
                K::Cancel => h.stop_modal(),
                K::Message | K::Error => {}
            }
        }),
    ));
    form.wrap(K::Message, ask.message);
    form.field(K::Text, "prompt.text", ask.message, ask.secure);
    form.label(K::Error, ask.first_error.as_deref().unwrap_or(""));
    form.set_tone(K::Error, Some(Tone::Error));
    form.button(K::Cancel, "Cancel", "prompt.cancel");
    form.button(K::Ok, ask.ok, "prompt.ok");
    form.key_equivalent(K::Ok, "\r");
    form.key_equivalent(K::Cancel, "\u{1b}");
    form.set_text(K::Text, ask.initial);
    *form_cell.borrow_mut() = Some(form.clone());
    let row = Size::Fixed(layout::ROW);
    let tree = Node::inset(
        Node::column(vec![
            Node::leaf(K::Message, Size::Stretch(1), Size::Fixed(layout::ROW * 2)),
            Node::leaf(K::Text, Size::Stretch(1), row),
            Node::leaf(K::Error, Size::Stretch(1), row),
            Node::Space(Size::Stretch(1)),
            Node::row(vec![
                Node::Space(Size::Stretch(1)),
                Node::button(K::Cancel, 90),
                Node::button(K::Ok, 90),
            ]),
        ]),
        layout::MARGIN,
    );
    form.place(&tree, host.area(s));
    host.present(s);
    host.run_modal(s);
    *form_cell.borrow_mut() = None;
    drop(form);
    let text = answer.borrow_mut().take();
    text
}
