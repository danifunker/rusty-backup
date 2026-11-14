//! A read-only text window a controller holds open: Check results, CHD Info.
//!
//! The owner rebuilds nothing: it hands the window its text on every refresh, and the window
//! sends the owner's dismiss when the user closes it. An optional tick box and button cover
//! "Show debug messages" and "Repair"; they, and Close, send their controller id to `command`.

use std::rc::Rc;

use super::form::Form;
use super::host::{Host, Surface};
use super::layout::{self, Node, Size};

/// What a report shows now.
pub struct Shown {
    pub headline: String,
    pub stats: String,
    pub lines: Vec<String>,
    /// The tick box's state, when the report has one.
    pub toggle: Option<bool>,
    /// Whether the action button shows.
    pub button: bool,
}

/// What each view of the window is, for layout.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Part {
    Headline,
    Stats,
    Text,
    Toggle,
    Button,
    Close,
}

pub struct ReportWindow {
    ui: Rc<dyn Host>,
    window: Surface,
    form: Form<Part>,
    has_toggle: bool,
    has_button: bool,
}

impl ReportWindow {
    /// `toggle` and `button` are `(label, id)`; `close_id` is what the Close button sends.
    pub fn open(
        ui: Rc<dyn Host>,
        title: &str,
        toggle: Option<(&str, &str)>,
        button: Option<(&str, &str)>,
        close_id: &str,
        on_close: Box<dyn Fn()>,
        command: Rc<dyn Fn(&str)>,
    ) -> ReportWindow {
        let window = ui.window(title, 560, 440, on_close);
        let ids: Vec<(Part, String)> = vec![
            (
                Part::Toggle,
                toggle.map(|t| t.1.to_string()).unwrap_or_default(),
            ),
            (
                Part::Button,
                button.map(|b| b.1.to_string()).unwrap_or_default(),
            ),
            (Part::Close, close_id.to_string()),
        ];
        let form = Form::with_host(
            ui.clone(),
            window,
            Rc::new(move |k: Part| {
                if let Some((_, id)) = ids.iter().find(|(p, _)| *p == k) {
                    command(id);
                }
            }),
        );
        form.label(Part::Headline, "");
        // Long stats (a filesystem's counters) wrap onto a second line.
        form.wrap(Part::Stats, "");
        form.text_view(Part::Text, "");
        if let Some((label, id)) = toggle {
            form.check(Part::Toggle, label, id);
        }
        if let Some((label, id)) = button {
            form.button(Part::Button, label, id);
        }
        form.button(Part::Close, "Close", close_id);
        let w = ReportWindow {
            ui,
            window,
            form,
            has_toggle: toggle.is_some(),
            has_button: button.is_some(),
        };
        w.layout();
        w.ui.present(window);
        w
    }

    pub fn show(&self, s: &Shown) {
        let f = &self.form;
        f.set_text(Part::Headline, &s.headline);
        f.set_text(Part::Stats, &s.stats);
        f.set_text_view(Part::Text, &s.lines.join("\n"));
        if self.has_toggle {
            if let Some(on) = s.toggle {
                f.set_check(Part::Toggle, on);
            }
            f.show(Part::Toggle, s.toggle.is_some());
        }
        if self.has_button {
            f.show(Part::Button, s.button);
        }
        self.layout();
    }

    /// The tick box's state, for the owner's toggle action.
    pub fn toggled(&self) -> bool {
        self.has_toggle && self.form.checked(Part::Toggle)
    }

    fn layout(&self) {
        let f = &self.form;
        let mut rows = Vec::new();
        if !f.text(Part::Headline).is_empty() {
            rows.push(Node::leaf(
                Part::Headline,
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 4),
            ));
        }
        if !f.text(Part::Stats).is_empty() {
            rows.push(Node::leaf(
                Part::Stats,
                Size::Stretch(1),
                Size::Fixed(2 * (layout::ROW - 6) + 4),
            ));
        }
        rows.push(Node::leaf(Part::Text, Size::Stretch(1), Size::Stretch(1)));
        rows.push(Node::row(vec![
            Node::button(Part::Toggle, 200),
            Node::Space(Size::Stretch(1)),
            Node::button(Part::Button, 100),
            Node::button(Part::Close, 90),
        ]));
        let keep = |p: &Part| match p {
            Part::Toggle => self.has_toggle && f.shown(Part::Toggle),
            Part::Button => self.has_button && f.shown(Part::Button),
            _ => true,
        };
        let tree = match Node::inset(Node::column(rows), layout::MARGIN).prune(&keep) {
            Some(t) => t,
            None => return,
        };
        f.place(&tree, self.ui.area(self.window));
    }
}

impl Drop for ReportWindow {
    fn drop(&mut self) {
        self.ui.release(self.window);
    }
}
