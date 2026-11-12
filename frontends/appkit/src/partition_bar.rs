//! The disk-layout bar: a custom `NSView` drawn from the controller's segments, its legend on a
//! line below. A click on a segment selects its partition; the widths and hit-testing are the
//! controller's, the palette is the one the egui, GTK and Win32 shells share.

use std::cell::RefCell;
use std::rc::Rc;

use objc2::rc::Retained;
use objc2::runtime::{AnyObject, NSObjectProtocol};
use objc2::{define_class, msg_send, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAppearance, NSAppearanceCustomization, NSBezierPath, NSColor, NSEvent, NSFont,
    NSFontAttributeName, NSForegroundColorAttributeName, NSLineBreakMode, NSMutableParagraphStyle,
    NSParagraphStyleAttributeName, NSStringDrawing, NSTextAlignment, NSView,
};
use objc2_foundation::{NSDictionary, NSPoint, NSRect, NSSize, NSString};
use rusty_backup::controller::inspect::{
    segment_at, segment_widths, LayoutSegment, SegmentKind, BAR_HEIGHT, MIN_SEGMENT_PX,
};

use crate::ui::{self, ns};

/// The legend line under the bar, in points.
const LEGEND: f64 = 20.0;

/// The bar and its legend's height in points.
pub const HEIGHT: i32 = BAR_HEIGHT as i32 + 6 + LEGEND as i32;

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

fn rgb((r, g, b): (u8, u8, u8)) -> Retained<NSColor> {
    NSColor::colorWithSRGBRed_green_blue_alpha(
        f64::from(r) / 255.0,
        f64::from(g) / 255.0,
        f64::from(b) / 255.0,
        1.0,
    )
}

fn fill(kind: SegmentKind, dark: bool) -> Retained<NSColor> {
    rgb(match kind {
        SegmentKind::Partition { color_index } => {
            let t = if dark { &DARK_FILLS } else { &LIGHT_FILLS };
            t[color_index % t.len()]
        }
        SegmentKind::Free if dark => (60, 60, 60),
        SegmentKind::Free => (225, 225, 228),
        SegmentKind::Dimmed if dark => (45, 45, 45),
        SegmentKind::Dimmed => (238, 238, 240),
    })
}

#[derive(Default)]
pub struct State {
    segments: Vec<LayoutSegment>,
    selected: Option<usize>,
    on_select: Option<Rc<dyn Fn(Option<usize>)>>,
}

define_class!(
    #[unsafe(super(NSView))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBPartitionBar"]
    #[ivars = RefCell<State>]
    pub struct Bar;

    unsafe impl NSObjectProtocol for Bar {}

    impl Bar {
        #[unsafe(method(isFlipped))]
        fn is_flipped(&self) -> bool {
            true
        }

        #[unsafe(method(drawRect:))]
        fn draw_rect(&self, _dirty: NSRect) {
            self.paint();
        }

        #[unsafe(method(mouseDown:))]
        fn mouse_down(&self, event: &NSEvent) {
            let at = self.convertPoint_fromView(event.locationInWindow(), None);
            if at.y > BAR_HEIGHT as f64 {
                return;
            }
            let (picked, f) = {
                let state = self.ivars().borrow();
                let widths = segment_widths(&state.segments, self.bounds().size.width as f32, MIN_SEGMENT_PX);
                let hit = segment_at(&widths, at.x as f32).and_then(|i| state.segments.get(i));
                (hit.and_then(|s| s.partition), state.on_select.clone())
            };
            if let Some(f) = f {
                f(picked);
            }
        }
    }
);

impl Bar {
    pub fn new(mtm: MainThreadMarker, id: &str) -> Retained<Bar> {
        let this = mtm.alloc::<Bar>().set_ivars(RefCell::default());
        // SAFETY: NSView's designated initialiser.
        let bar: Retained<Bar> = unsafe { msg_send![super(this), initWithFrame: NSRect::ZERO] };
        ui::set_id(&bar, id);
        bar
    }

    pub fn on_select(&self, f: Rc<dyn Fn(Option<usize>)>) {
        self.ivars().borrow_mut().on_select = Some(f);
    }

    /// Draw `segments` with `selected` outlined; only a change redraws.
    pub fn show(&self, segments: Vec<LayoutSegment>, selected: Option<usize>) {
        {
            let mut state = self.ivars().borrow_mut();
            if state.segments == segments && state.selected == selected {
                return;
            }
            state.segments = segments;
            state.selected = selected;
        }
        self.setNeedsDisplay(true);
    }

    fn dark(&self) -> bool {
        let look: Retained<NSAppearance> = self.effectiveAppearance();
        look.name().to_string().contains("Dark")
    }

    fn paint(&self) {
        let state = self.ivars().borrow();
        let dark = self.dark();
        let bounds = self.bounds();
        let bar_h = BAR_HEIGHT as f64;
        let widths = segment_widths(&state.segments, bounds.size.width as f32, MIN_SEGMENT_PX);
        let on_fill = if dark {
            NSColor::whiteColor()
        } else {
            NSColor::blackColor()
        };
        let centred = attributes(&on_fill, NSTextAlignment::Center);
        let mut x = 0.0f64;
        let mut picked = None;
        for (seg, w) in state.segments.iter().zip(&widths) {
            let w = f64::from(*w);
            let r = rect(x, 0.0, w, bar_h);
            fill(seg.kind, dark).setFill();
            NSBezierPath::fillRect(r);
            if let Some(text) = seg.inline_label(w as f32) {
                let tr = rect(x + 4.0, (bar_h - 16.0) / 2.0, (w - 8.0).max(0.0), 16.0);
                draw(&ui::plain(&text), tr, &centred);
            }
            if seg.partition.is_some() && seg.partition == state.selected {
                picked = Some(r);
            }
            x += w;
            NSColor::separatorColor().setStroke();
            NSBezierPath::strokeLineFromPoint_toPoint(NSPoint::new(x, 0.0), NSPoint::new(x, bar_h));
        }
        if let Some(r) = picked {
            NSColor::labelColor().setStroke();
            let path = NSBezierPath::bezierPathWithRect(rect(
                r.origin.x + 1.5,
                1.5,
                (r.size.width - 3.0).max(0.0),
                bar_h - 3.0,
            ));
            path.setLineWidth(3.0);
            path.stroke();
        }
        // The legend: a swatch and a name per segment, on one line under the bar.
        let top = bar_h + 6.0;
        let plain = attributes(&NSColor::labelColor(), NSTextAlignment::Left);
        let mut lx = 0.0f64;
        for seg in &state.segments {
            fill(seg.kind, dark).setFill();
            NSBezierPath::fillRect(rect(lx, top + 4.0, 12.0, 12.0));
            let text = ns(&ui::plain(&seg.legend_text()));
            // SAFETY: drawing a string with attributes made above, inside drawRect:.
            let size = unsafe { text.sizeWithAttributes(Some(&plain)) };
            let tx = lx + 18.0;
            let r = rect(tx, top + 1.0, size.width + 2.0, LEGEND);
            // SAFETY: as above.
            unsafe { text.drawInRect_withAttributes(r, Some(&plain)) };
            lx = tx + size.width + 16.0;
            if lx >= bounds.size.width {
                break;
            }
        }
    }
}

fn rect(x: f64, y: f64, w: f64, h: f64) -> NSRect {
    NSRect::new(NSPoint::new(x, y), NSSize::new(w, h))
}

/// The system font in `color`, aligned, cut with an ellipsis where it does not fit.
fn attributes(
    color: &NSColor,
    align: NSTextAlignment,
) -> Retained<NSDictionary<NSString, AnyObject>> {
    let font = NSFont::systemFontOfSize(11.0);
    let style = NSMutableParagraphStyle::new();
    style.setAlignment(align);
    style.setLineBreakMode(NSLineBreakMode::ByTruncatingTail);
    // SAFETY: AppKit's attribute-name constants.
    let keys = unsafe {
        [
            NSFontAttributeName,
            NSForegroundColorAttributeName,
            NSParagraphStyleAttributeName,
        ]
    };
    let values: [&AnyObject; 3] = [font.as_ref(), color.as_ref(), style.as_ref()];
    NSDictionary::from_slices(&keys, &values)
}

fn draw(text: &str, r: NSRect, attrs: &NSDictionary<NSString, AnyObject>) {
    // SAFETY: drawing inside drawRect: with attributes of the documented types.
    unsafe { ns(text).drawInRect_withAttributes(r, Some(attrs)) };
}
