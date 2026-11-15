//! The disk-layout bar: a canvas drawn from the controller's segments, its legend on a line
//! below. A click on a segment selects its partition; the widths and hit-testing are the
//! controller's, the palette is the one the egui, GTK and Win32 shells share.

use std::cell::RefCell;
use std::rc::Rc;

use rusty_backup::controller::inspect::{
    segment_at, segment_widths, LayoutSegment, SegmentKind, BAR_HEIGHT, MIN_SEGMENT_PX,
};

use super::form::{plain, Form};
use super::host::{Align, Ctl, Host, Ink, Painter, Pen, Surface};

/// The legend line under the bar, in points.
const LEGEND: f64 = 20.0;

/// The bar and its legend's height in points.
pub const HEIGHT: i32 = BAR_HEIGHT as i32 + 6 + LEGEND as i32;

/// The bar alone, without its legend.
pub const BAR_ONLY: i32 = BAR_HEIGHT as i32;

const DARK_FILLS: [(u8, u8, u8); 8] = [
    (62, 118, 166),
    (74, 129, 52),
    (151, 103, 56),
    (170, 87, 155),
    (52, 125, 125),
    (127, 116, 46),
    (129, 100, 184),
    (172, 94, 94),
];
const LIGHT_FILLS: [(u8, u8, u8); 8] = [
    (76, 140, 196),
    (106, 182, 76),
    (196, 136, 76),
    (194, 99, 176),
    (76, 176, 176),
    (196, 180, 76),
    (148, 116, 209),
    (196, 108, 108),
];

fn fill(kind: SegmentKind, dark: bool) -> Ink {
    let (r, g, b) = match kind {
        SegmentKind::Partition { color_index } => {
            let t = if dark { &DARK_FILLS } else { &LIGHT_FILLS };
            t[color_index % t.len()]
        }
        SegmentKind::Free if dark => (60, 60, 60),
        SegmentKind::Free => (225, 225, 228),
        SegmentKind::Dimmed if dark => (45, 45, 45),
        SegmentKind::Dimmed => (238, 238, 240),
    };
    Ink::Rgb(r, g, b)
}

struct State {
    segments: Vec<LayoutSegment>,
    selected: Option<usize>,
    /// The part of the width the bar spans, so two bars share one scale.
    scale: f32,
    legend: bool,
}

/// A bar on a form: its state, painted by the host's canvas.
#[derive(Clone)]
pub struct Bar {
    state: Rc<RefCell<State>>,
    host: Rc<dyn Host>,
    surface: Surface,
    ctl: Ctl,
}

impl Bar {
    /// Add a bar to `form` as `k`; a click on it reports `k`, and `picked` says which partition.
    pub fn add<K: Copy + PartialEq + 'static>(form: &Form<K>, k: K, id: &str) -> Bar {
        let state = Rc::new(RefCell::new(State {
            segments: Vec::new(),
            selected: None,
            scale: 1.0,
            legend: true,
        }));
        let drawn = state.clone();
        let painter: Painter = Rc::new(move |pen: &dyn Pen, w: f64, _h: f64, dark: bool| {
            paint(&drawn.borrow(), pen, w, dark);
        });
        form.canvas(k, id, painter);
        Bar {
            state,
            host: form.host().clone(),
            surface: form.surface(),
            ctl: form.ctl(k),
        }
    }

    /// Draw across `scale` of the width (a smaller disk beside a larger one), with or without a legend.
    pub fn set_options(&self, scale: f32, legend: bool) {
        {
            let mut s = self.state.borrow_mut();
            if (s.scale - scale).abs() < f32::EPSILON && s.legend == legend {
                return;
            }
            s.scale = scale;
            s.legend = legend;
        }
        self.host.redraw(self.surface, self.ctl);
    }

    /// Draw `segments` with `selected` outlined; only a change redraws.
    pub fn show(&self, segments: Vec<LayoutSegment>, selected: Option<usize>) {
        {
            let mut s = self.state.borrow_mut();
            if s.segments == segments && s.selected == selected {
                return;
            }
            s.segments = segments;
            s.selected = selected;
        }
        self.host.redraw(self.surface, self.ctl);
    }

    /// The partition under the last click, `Some(None)` for a click off every partition, `None`
    /// for a click below the bar (on the legend), which selects nothing.
    pub fn picked(&self) -> Option<Option<usize>> {
        let (x, y, w, _) = self.host.clicked(self.surface, self.ctl)?;
        if y > BAR_HEIGHT as f64 {
            return None;
        }
        let s = self.state.borrow();
        let widths = segment_widths(&s.segments, w as f32 * s.scale, MIN_SEGMENT_PX);
        let hit = segment_at(&widths, x as f32).and_then(|i| s.segments.get(i));
        Some(hit.and_then(|seg| seg.partition))
    }
}

fn paint(state: &State, pen: &dyn Pen, width: f64, dark: bool) {
    let bar_h = BAR_HEIGHT as f64;
    let widths = segment_widths(&state.segments, width as f32 * state.scale, MIN_SEGMENT_PX);
    let mut x = 0.0f64;
    let mut picked = None;
    for (seg, w) in state.segments.iter().zip(&widths) {
        let w = f64::from(*w);
        pen.fill(x, 0.0, w, bar_h, fill(seg.kind, dark));
        if let Some(text) = seg.inline_label(w as f32) {
            let tw = (w - 8.0).max(0.0);
            pen.text(
                (x + 4.0, (bar_h - 16.0) / 2.0, tw, 16.0),
                &plain(&text),
                Ink::OnFill,
                Align::Center,
            );
        }
        if seg.partition.is_some() && seg.partition == state.selected {
            picked = Some((x, w));
        }
        x += w;
        pen.line(x, 0.0, x, bar_h, Ink::Separator);
    }
    if let Some((px, pw)) = picked {
        pen.frame(
            px + 1.5,
            1.5,
            (pw - 3.0).max(0.0),
            bar_h - 3.0,
            3.0,
            Ink::Label,
        );
    }
    if !state.legend {
        return;
    }
    // The legend: a swatch and a name per segment, on one line under the bar.
    let top = bar_h + 6.0;
    let mut lx = 0.0f64;
    for seg in &state.segments {
        pen.fill(lx, top + 4.0, 12.0, 12.0, fill(seg.kind, dark));
        let text = plain(&seg.legend_text());
        let tw = pen.text_width(&text);
        let tx = lx + 18.0;
        pen.text(
            (tx, top + 1.0, tw + 2.0, LEGEND),
            &text,
            Ink::Label,
            Align::Left,
        );
        lx = tx + tw + 16.0;
        if lx >= width {
            break;
        }
    }
}
