//! Fulfils controller [`Effect`]s through the [`Host`]: pickers, questions, prompts, the
//! clipboard, URLs.
//!
//! A question is modal, opened from a turn of the main queue of its own: an AX press waits for the
//! action it pressed, so a modal raised inside that action would hold the flow driver until the
//! user answered. A modal runs its own loop, so the controllers are never borrowed while one is
//! up, and a wake that arrives meanwhile ticks them as usual. Effects queue in order behind it.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::rc::Rc;

use rusty_backup::controller::{picks, Effect};

use super::host::Host;

/// An answer, routed by the screen that asked to its controller's matching `Action`.
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
    host: Rc<dyn Host>,
    busy: Cell<bool>,
    /// A question waits for a turn of its own, not the action that raised it.
    posted: Cell<bool>,
    queue: RefCell<VecDeque<(Effect, ReplyFn)>>,
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

impl EffectHost {
    pub fn new(host: Rc<dyn Host>) -> Self {
        EffectHost {
            inner: Rc::new(Inner {
                host,
                busy: Cell::new(false),
                posted: Cell::new(false),
                queue: RefCell::new(VecDeque::new()),
            }),
        }
    }

    /// True while a dialog the host opened is up.
    pub fn dialog_open(&self) -> bool {
        self.inner.busy.get()
    }

    /// Carry out `effects` in order; questions open on a turn of their own.
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
            let (effect, reply) = match next {
                Some(x) => x,
                None => break,
            };
            if !fresh && is_question(&effect) {
                self.inner.queue.borrow_mut().push_front((effect, reply));
                self.inner.posted.set(true);
                let me = Rc::downgrade(&self.inner);
                self.inner.host.later(Box::new(move || {
                    if let Some(inner) = me.upgrade() {
                        inner.posted.set(false);
                        EffectHost { inner }.drain(true);
                    }
                }));
                break;
            }
            self.inner.busy.set(true);
            self.one(effect, &reply);
            self.inner.busy.set(false);
        }
    }

    /// No `_` arm, so a new `Effect` fails to compile until handled here.
    fn one(&self, effect: Effect, reply: &ReplyFn) {
        let host = &self.inner.host;
        match effect {
            Effect::PickFile(spec) => {
                let path = match picks::scripted(spec.purpose) {
                    Some(answer) => answer,
                    None => host.pick(&spec, false),
                };
                reply(Reply::Picked {
                    purpose: spec.purpose,
                    path,
                });
            }
            Effect::PickFolder(spec) => {
                let path = match picks::scripted(spec.purpose) {
                    Some(answer) => answer,
                    None => host.pick(&spec, true),
                };
                reply(Reply::Picked {
                    purpose: spec.purpose,
                    path,
                });
            }
            Effect::Confirm(spec) => {
                let yes = host.confirm(&spec);
                reply(Reply::Confirmed {
                    purpose: spec.purpose,
                    yes,
                });
            }
            Effect::Choose(spec) => {
                let choice = host.choose(&spec);
                reply(Reply::Chose {
                    purpose: spec.purpose,
                    choice,
                });
            }
            Effect::AskText(spec) => {
                let text = host.ask_text(&spec);
                reply(Reply::Answered {
                    purpose: spec.purpose,
                    text,
                });
            }
            Effect::AskPassword(spec) => {
                let text = host.ask_password(&spec);
                reply(Reply::Answered {
                    purpose: spec.purpose,
                    text,
                });
            }
            Effect::Message(text) => host.status(&text),
            Effect::OpenUrl(url) => host.open_url(&url),
            Effect::CopyToClipboard(text) => host.copy_text(&text),
            Effect::CloseScreen => reply(Reply::CloseScreen),
            Effect::Quit => host.quit(),
        }
    }
}
