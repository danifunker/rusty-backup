//! A screen's controls keyed by its own enum: built once, each tagged with its place, its actions
//! and text changes handed to the screen as that key. Screens are written on it, so each is its
//! controller's reading and its layout; the [`Host`] does the toolkit's part.
//!
//! A value or pick set through accessibility reaches the toolkit without an action. Before a
//! control's action is reported, the form reports such changes one at a time, each against the
//! text or pick it last reported, so the screen hears every change in order.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::commander::Tone;

use super::host::{Column, Ctl, Ev, Host, Painter, PullItem, Surface};
use super::layout::{self, Node};

/// What a control is, as far as reporting its changes goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Button,
    Check,
    Radio,
    Label,
    Field,
    Popup,
    Pulldown,
    Progress,
    Table,
    Canvas,
    TextView,
    /// A view the shell builds itself (a list, a bar) and hands the host.
    Custom,
}

/// Text from a volume can hold NUL, which an `NSString` would keep and a C API would cut: drop it.
pub fn plain(s: &str) -> String {
    s.replace('\0', "")
}

/// A list's control and its activate and toggle keys.
type ListKeys<K> = (Ctl, Option<K>, Option<K>);

struct State<K> {
    /// Each key by its control number; `None` until built, and again once removed.
    keys: RefCell<Vec<(K, Option<Kind>)>>,
    /// Each field and the text last reported to the screen or set from the controller.
    fields: RefCell<Vec<(K, String)>>,
    /// Each pop-up and the pick last reported or set.
    pops: RefCell<Vec<(K, Option<usize>)>>,
    /// A list's activate and toggle keys, when they differ from its own.
    lists: RefCell<Vec<ListKeys<K>>>,
    on: Rc<dyn Fn(K)>,
}

impl<K: Copy + PartialEq> State<K> {
    fn ctl(&self, k: K) -> Ctl {
        let mut keys = self.keys.borrow_mut();
        match keys.iter().position(|x| x.0 == k) {
            Some(i) => i as Ctl,
            None => {
                keys.push((k, None));
                (keys.len() - 1) as Ctl
            }
        }
    }

    fn key(&self, c: Ctl) -> Option<(K, Option<Kind>)> {
        self.keys.borrow().get(c as usize).copied()
    }

    /// The next field whose text changed since it was last reported, now marked as reported.
    fn next_edit(&self, host: &dyn Host, s: Surface) -> Option<K> {
        let list: Vec<K> = self.fields.borrow().iter().map(|f| f.0).collect();
        for k in list {
            let now = host.text(s, self.ctl(k));
            let mut fields = self.fields.borrow_mut();
            if let Some(entry) = fields.iter_mut().find(|f| f.0 == k) {
                if now != entry.1 {
                    entry.1 = now;
                    return Some(k);
                }
            }
        }
        None
    }

    /// The next pop-up whose pick changed without its action, now marked.
    fn next_pick(&self, host: &dyn Host, s: Surface) -> Option<K> {
        let list: Vec<K> = self.pops.borrow().iter().map(|p| p.0).collect();
        for k in list {
            let now = host.picked(s, self.ctl(k));
            let mut pops = self.pops.borrow_mut();
            if let Some(entry) = pops.iter_mut().find(|p| p.0 == k) {
                if now != entry.1 {
                    entry.1 = now;
                    return Some(k);
                }
            }
        }
        None
    }

    fn report(&self, host: &dyn Host, s: Surface, c: Ctl, ev: Ev) {
        let (k, kind) = match self.key(c) {
            Some(x) => x,
            None => return,
        };
        match ev {
            Ev::Edited => {
                while self.next_edit(host, s).is_some() {}
                (self.on)(k);
            }
            // Only a change the screen has not heard yet: typing reports as `Edited`.
            Ev::ValueSet => {
                if self.next_edit(host, s).is_some() {
                    (self.on)(k);
                }
            }
            Ev::Selected => (self.on)(k),
            Ev::Activated | Ev::Toggled => {
                let alias = self.lists.borrow().iter().find(|l| l.0 == c).map(|l| {
                    if ev == Ev::Activated {
                        l.1
                    } else {
                        l.2
                    }
                });
                (self.on)(alias.flatten().unwrap_or(k));
            }
            Ev::Act => {
                // The sender's own pick or tick, which the reports below could reset.
                let pick = match kind {
                    Some(Kind::Popup) => Some(host.picked(s, c)),
                    _ => None,
                };
                let tick = match kind {
                    Some(Kind::Check) | Some(Kind::Radio) => Some(host.checked(s, c)),
                    _ => None,
                };
                if let Some(p) = pick {
                    for entry in self.pops.borrow_mut().iter_mut() {
                        if entry.0 == k {
                            entry.1 = p;
                        }
                    }
                }
                loop {
                    let next = match self.next_edit(host, s) {
                        Some(f) => Some(f),
                        None => self.next_pick(host, s),
                    };
                    match next {
                        Some(f) => (self.on)(f),
                        None => break,
                    }
                }
                if let Some(Some(i)) = pick {
                    host.select(s, c, i);
                } else if let Some(t) = tick {
                    host.set_check(s, c, t);
                }
                (self.on)(k);
            }
        }
    }
}

pub struct Form<K: Copy + PartialEq + 'static> {
    host: Rc<dyn Host>,
    surface: Surface,
    st: Rc<State<K>>,
}

impl<K: Copy + PartialEq + 'static> Form<K> {
    /// A form on `surface`; `on` hears each control's key when it acts or its text changes.
    pub fn with_host(host: Rc<dyn Host>, surface: Surface, on: Rc<dyn Fn(K)>) -> Form<K> {
        let st = Rc::new(State {
            keys: RefCell::new(Vec::new()),
            fields: RefCell::new(Vec::new()),
            pops: RefCell::new(Vec::new()),
            lists: RefCell::new(Vec::new()),
            on,
        });
        let (h, state): (Weak<dyn Host>, Weak<State<K>>) =
            (Rc::downgrade(&host), Rc::downgrade(&st));
        host.bind(
            surface,
            Rc::new(move |c, ev| {
                if let (Some(h), Some(st)) = (h.upgrade(), state.upgrade()) {
                    st.report(&*h, surface, c, ev);
                }
            }),
        );
        Form { host, surface, st }
    }

    pub fn host(&self) -> &Rc<dyn Host> {
        &self.host
    }

    pub fn surface(&self) -> Surface {
        self.surface
    }

    /// The control number of `k`, handed out on first use.
    pub fn ctl(&self, k: K) -> Ctl {
        self.st.ctl(k)
    }

    fn built(&self, k: K, kind: Kind) -> Ctl {
        let c = self.st.ctl(k);
        self.st.keys.borrow_mut()[c as usize].1 = Some(kind);
        c
    }

    fn kind(&self, k: K) -> Option<Kind> {
        self.st.key(self.st.ctl(k)).and_then(|x| x.1)
    }

    /// Mark `k` built as a view the shell hands the host itself.
    pub fn custom(&self, k: K) -> Ctl {
        self.built(k, Kind::Custom)
    }

    pub fn button(&self, k: K, title: &str, id: &str) {
        let c = self.built(k, Kind::Button);
        self.host.button(self.surface, c, title, id);
    }

    pub fn check(&self, k: K, title: &str, id: &str) {
        let c = self.built(k, Kind::Check);
        self.host.check(self.surface, c, title, id);
    }

    /// A radio button in group `group`: only radios of one group exclude each other.
    pub fn radio(&self, k: K, group: usize, title: &str, id: &str) {
        let c = self.built(k, Kind::Radio);
        self.host.radio(self.surface, c, group, title, id);
    }

    pub fn label(&self, k: K, text: &str) {
        let c = self.built(k, Kind::Label);
        self.host.label(self.surface, c, &plain(text));
    }

    /// A label that wraps onto as many lines as its frame gives it.
    pub fn wrap(&self, k: K, text: &str) {
        let c = self.built(k, Kind::Label);
        self.host.wrap(self.surface, c, &plain(text));
    }

    pub fn popup(&self, k: K, items: &[String], id: &str) {
        let c = self.built(k, Kind::Popup);
        let items: Vec<String> = items.iter().map(|i| plain(i)).collect();
        self.host.popup(self.surface, c, &items, id);
        let now = self.host.picked(self.surface, c);
        self.st.pops.borrow_mut().push((k, now));
    }

    /// A pull-down whose first item is its title and whose items each act as their own key.
    pub fn pulldown(&self, k: K, id: &str) {
        let c = self.built(k, Kind::Pulldown);
        self.host.pulldown(self.surface, c, id);
    }

    /// Fill pull-down `k`: its title, then `(label, key, on)` items; a `None` key is a caption.
    pub fn set_pulldown(&self, k: K, title: &str, items: &[(String, Option<K>, bool)]) {
        let c = self.st.ctl(k);
        let items: Vec<PullItem> = items
            .iter()
            .map(|(label, key, on)| (plain(label), key.map(|k| self.st.ctl(k)), *on))
            .collect();
        self.host
            .set_pulldown(self.surface, c, &plain(title), &items);
    }

    /// An editable field; every change reaches the screen as `k`.
    pub fn field(&self, k: K, id: &str, caption: &str, secret: bool) {
        let c = self.built(k, Kind::Field);
        self.host.field(self.surface, c, id, caption, secret);
        self.st.fields.borrow_mut().push((k, String::new()));
    }

    pub fn progress(&self, k: K) {
        let c = self.built(k, Kind::Progress);
        self.host.progress(self.surface, c);
    }

    /// Show the bar for a running job at `pct`, or sweeping when there is no percentage.
    pub fn set_progress(&self, k: K, running: bool, pct: Option<u32>) {
        self.host
            .set_progress(self.surface, self.st.ctl(k), running, pct);
    }

    /// A list; Return or a double-click reports `activate`, a row's tick box `toggle` (with
    /// `checks`), each `k` when not given. `columns` are `(title, width, right-aligned)`.
    pub fn table(
        &self,
        k: K,
        id: &str,
        columns: &[(&str, f64, bool)],
        activate: Option<K>,
        toggle: Option<K>,
    ) {
        let c = self.built(k, Kind::Table);
        let cols: Vec<Column> = columns
            .iter()
            .map(|(t, w, r)| (t.to_string(), *w, *r))
            .collect();
        self.host
            .table(self.surface, c, id, &cols, toggle.is_some());
        if let Some(a) = activate {
            self.st.ctl(a);
        }
        if let Some(t) = toggle {
            self.st.ctl(t);
        }
        self.st.lists.borrow_mut().push((c, activate, toggle));
    }

    /// Show `rows`; only a change reloads, so the selection and scroll survive a refresh.
    pub fn set_rows(&self, k: K, rows: &[Vec<String>]) {
        let rows: Vec<Vec<String>> = rows
            .iter()
            .map(|r| r.iter().map(|x| plain(x)).collect())
            .collect();
        self.host.set_rows(self.surface, self.st.ctl(k), &rows);
    }

    pub fn set_checks(&self, k: K, checks: &[bool]) {
        self.host.set_checks(self.surface, self.st.ctl(k), checks);
    }

    pub fn selected_row(&self, k: K) -> Option<usize> {
        self.host.selected_row(self.surface, self.st.ctl(k))
    }

    /// Select row `i` as the screen's own change: it is not reported back.
    pub fn select_row(&self, k: K, i: usize) {
        self.host.select_row(self.surface, self.st.ctl(k), i);
    }

    /// The row and state of the last tick box the user changed on list `k`.
    pub fn toggled(&self, k: K) -> Option<(usize, bool)> {
        self.host.toggled(self.surface, self.st.ctl(k))
    }

    /// A view drawn by `painter`; a click reports `k`, and `clicked` says where.
    pub fn canvas(&self, k: K, id: &str, painter: Painter) {
        let c = self.built(k, Kind::Canvas);
        self.host.canvas(self.surface, c, id, painter);
    }

    pub fn redraw(&self, k: K) {
        self.host.redraw(self.surface, self.st.ctl(k));
    }

    /// The last click on canvas `k` and its size then: `(x, y, w, h)`.
    pub fn clicked(&self, k: K) -> Option<(f64, f64, f64, f64)> {
        self.host.clicked(self.surface, self.st.ctl(k))
    }

    /// A read-only text view in the fixed-width log font.
    pub fn text_view(&self, k: K, id: &str) {
        let c = self.built(k, Kind::TextView);
        self.host.text_view(self.surface, c, id);
    }

    pub fn set_text_view(&self, k: K, text: &str) {
        self.host
            .set_text_view(self.surface, self.st.ctl(k), &plain(text));
    }

    pub fn append_text_view(&self, k: K, text: &str, follow: bool) {
        self.host
            .append_text_view(self.surface, self.st.ctl(k), &plain(text), follow);
    }

    /// Drop every view whose key `gone` names (rows built again for a new source).
    pub fn remove(&self, gone: impl Fn(&K) -> bool) {
        self.st.fields.borrow_mut().retain(|f| !gone(&f.0));
        self.st.pops.borrow_mut().retain(|p| !gone(&p.0));
        let dropped: Vec<Ctl> = {
            let mut keys = self.st.keys.borrow_mut();
            let mut out = Vec::new();
            for (i, entry) in keys.iter_mut().enumerate() {
                if entry.1.is_some() && gone(&entry.0) {
                    entry.1 = None;
                    out.push(i as Ctl);
                }
            }
            out
        };
        for c in dropped {
            self.host.remove(self.surface, c);
        }
    }

    pub fn has(&self, k: K) -> bool {
        self.st
            .keys
            .borrow()
            .iter()
            .any(|x| x.0 == k && x.1.is_some())
    }

    pub fn shown(&self, k: K) -> bool {
        self.has(k) && self.host.shown(self.surface, self.st.ctl(k))
    }

    pub fn show(&self, k: K, on: bool) {
        self.host.show(self.surface, self.st.ctl(k), on);
    }

    pub fn enable(&self, k: K, on: bool) {
        self.host.enable(self.surface, self.st.ctl(k), on);
    }

    pub fn checked(&self, k: K) -> bool {
        self.host.checked(self.surface, self.st.ctl(k))
    }

    pub fn set_check(&self, k: K, on: bool) {
        self.host.set_check(self.surface, self.st.ctl(k), on);
    }

    pub fn set_title(&self, k: K, t: &str) {
        self.host.set_title(self.surface, self.st.ctl(k), t);
    }

    /// The key that presses button `k`: "\r" for the default button, "\u{1b}" for Cancel.
    pub fn key_equivalent(&self, k: K, key: &str) {
        self.host.key_equivalent(self.surface, self.st.ctl(k), key);
    }

    /// A label's text, or a field's: a field keeps text the screen has not heard yet.
    pub fn set_text(&self, k: K, t: &str) {
        let c = self.st.ctl(k);
        if self.kind(k) != Some(Kind::Field) {
            self.host.set_text(self.surface, c, t);
            return;
        }
        if self.typing(k) {
            return;
        }
        {
            let mut fields = self.st.fields.borrow_mut();
            let entry = match fields.iter_mut().find(|f| f.0 == k) {
                Some(e) => e,
                None => return,
            };
            let now = self.host.text(self.surface, c);
            if now != entry.1 && now != t {
                return;
            }
            entry.1 = plain(t);
        }
        // The borrow ends first: the host may report the set, and that report reads the fields.
        self.host.set_text(self.surface, c, t);
    }

    /// Set a line's text and tone; hidden when there is none.
    pub fn line(&self, k: K, text: Option<&str>, tone: Option<Tone>) {
        self.set_text(k, text.unwrap_or(""));
        self.show(k, text.is_some());
        self.set_tone(k, tone);
    }

    pub fn set_tone(&self, k: K, tone: Option<Tone>) {
        self.host.set_tone(self.surface, self.st.ctl(k), tone);
    }

    pub fn text(&self, k: K) -> String {
        self.host.text(self.surface, self.st.ctl(k))
    }

    /// The field has the keyboard: what is typed there wins over the controller's copy.
    pub fn typing(&self, k: K) -> bool {
        self.host.typing(self.surface, self.st.ctl(k))
    }

    pub fn picked(&self, k: K) -> Option<usize> {
        self.host.picked(self.surface, self.st.ctl(k))
    }

    pub fn select(&self, k: K, i: usize) {
        let c = self.st.ctl(k);
        if self.host.picked(self.surface, c) != Some(i) {
            self.host.select(self.surface, c, i);
        }
        self.mark_pick(k);
    }

    /// The pop-up's pick now is the screen's own, not one to report.
    fn mark_pick(&self, k: K) {
        let now = self.host.picked(self.surface, self.st.ctl(k));
        for entry in self.st.pops.borrow_mut().iter_mut() {
            if entry.0 == k {
                entry.1 = now;
            }
        }
    }

    /// Replace a pop-up's items, only when they changed.
    pub fn set_items(&self, k: K, items: &[String]) {
        let c = self.st.ctl(k);
        let want: Vec<String> = items.iter().map(|i| plain(i)).collect();
        if self.host.items(self.surface, c) != want {
            self.host.set_items(self.surface, c, &want);
        }
        self.mark_pick(k);
    }

    /// Lay `tree` out in `area` and move each view to its place.
    pub fn place(&self, tree: &Node<K>, area: layout::Rect) {
        for (k, r) in layout::layout(tree, area, 96) {
            self.host.move_to(self.surface, self.st.ctl(k), r);
        }
    }

    /// Prune `tree` of hidden views and lay it out at its page's width: taller than the view, the
    /// page scrolls.
    pub fn place_page(&self, tree: Node<K>) {
        let keep = |k: &K| self.shown(*k);
        let tree = match tree.prune(&keep) {
            Some(t) => t,
            None => return,
        };
        let (w, visible) = match self.host.page_viewport(self.surface) {
            Some(v) => v,
            None => return,
        };
        let h = visible.max(tree.min_height());
        self.host.set_surface_size(self.surface, w, h);
        self.place(&tree, layout::Rect { x: 0, y: 0, w, h });
    }

    pub fn tooltip(&self, k: K, text: &str) {
        self.host.tooltip(self.surface, self.st.ctl(k), text);
    }

    /// Prune `tree` of hidden views, then place it in `area`.
    pub fn place_shown(&self, tree: Node<K>, area: layout::Rect) {
        let keep = |k: &K| self.shown(*k);
        if let Some(tree) = tree.prune(&keep) {
            self.place(&tree, area);
        }
    }
}

impl<K: Copy + PartialEq + 'static> Drop for Form<K> {
    fn drop(&mut self) {
        self.host.release(self.surface);
    }
}
