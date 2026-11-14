//! The canvas the shared screens draw on (frontends/mac-screens host::Painter): a flipped
//! `NSView` whose `drawRect:` runs the painter with a pen over `NSBezierPath` and string drawing.

use std::cell::{Cell, RefCell};

use objc2::rc::Retained;
use objc2::runtime::{AnyObject, NSObjectProtocol};
use objc2::{define_class, msg_send, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAppearance, NSAppearanceCustomization, NSBezierPath, NSColor, NSEvent, NSFont,
    NSFontAttributeName, NSForegroundColorAttributeName, NSLineBreakMode, NSMutableParagraphStyle,
    NSParagraphStyleAttributeName, NSStringDrawing, NSTextAlignment, NSView,
};
use objc2_foundation::{NSDictionary, NSPoint, NSRect, NSSize, NSString};

use crate::screens::host::{Align, Ink, Painter, Pen};
use crate::ui::{self, ns};

pub struct State {
    painter: Option<Painter>,
    /// The last click and the canvas's size then.
    click: Cell<Option<(f64, f64, f64, f64)>>,
    on_click: Option<Box<dyn Fn()>>,
}

define_class!(
    #[unsafe(super(NSView))]
    #[thread_kind = MainThreadOnly]
    #[name = "RBCanvas"]
    #[ivars = RefCell<State>]
    pub struct Canvas;

    unsafe impl NSObjectProtocol for Canvas {}

    impl Canvas {
        #[unsafe(method(isFlipped))]
        fn is_flipped(&self) -> bool {
            true
        }

        #[unsafe(method(drawRect:))]
        fn draw_rect(&self, _dirty: NSRect) {
            let painter = self.ivars().borrow().painter.clone();
            if let Some(p) = painter {
                let b = self.bounds();
                let dark = self.dark();
                p(&AppKitPen { dark }, b.size.width, b.size.height, dark);
            }
        }

        #[unsafe(method(mouseDown:))]
        fn mouse_down(&self, event: &NSEvent) {
            let at = self.convertPoint_fromView(event.locationInWindow(), None);
            let b = self.bounds();
            self.ivars()
                .borrow()
                .click
                .set(Some((at.x, at.y, b.size.width, b.size.height)));
            // Out of the borrow: the screen reads the click back through the host.
            let state = self.ivars().borrow();
            if let Some(f) = &state.on_click {
                f();
            }
        }
    }
);

impl Canvas {
    pub fn new(
        mtm: MainThreadMarker,
        id: &str,
        painter: Painter,
        on_click: Box<dyn Fn()>,
    ) -> Retained<Canvas> {
        let state = State {
            painter: Some(painter),
            click: Cell::new(None),
            on_click: Some(on_click),
        };
        let this = mtm.alloc::<Canvas>().set_ivars(RefCell::new(state));
        // SAFETY: NSView's designated initialiser.
        let v: Retained<Canvas> = unsafe { msg_send![super(this), initWithFrame: NSRect::ZERO] };
        if !id.is_empty() {
            ui::set_id(&v, id);
        }
        v
    }

    pub fn clicked(&self) -> Option<(f64, f64, f64, f64)> {
        self.ivars().borrow().click.get()
    }

    fn dark(&self) -> bool {
        let look: Retained<NSAppearance> = self.effectiveAppearance();
        look.name().to_string().contains("Dark")
    }
}

struct AppKitPen {
    dark: bool,
}

fn rect(x: f64, y: f64, w: f64, h: f64) -> NSRect {
    NSRect::new(NSPoint::new(x, y), NSSize::new(w, h))
}

impl AppKitPen {
    fn color(&self, ink: Ink) -> Retained<NSColor> {
        match ink {
            Ink::Rgb(r, g, b) => NSColor::colorWithSRGBRed_green_blue_alpha(
                f64::from(r) / 255.0,
                f64::from(g) / 255.0,
                f64::from(b) / 255.0,
                1.0,
            ),
            Ink::Label => NSColor::labelColor(),
            Ink::Separator => NSColor::separatorColor(),
            Ink::OnFill if self.dark => NSColor::whiteColor(),
            Ink::OnFill => NSColor::blackColor(),
        }
    }

    /// The small system font in `color`, aligned, cut with an ellipsis where it does not fit.
    fn attributes(&self, ink: Ink, align: Align) -> Retained<NSDictionary<NSString, AnyObject>> {
        let font = NSFont::systemFontOfSize(11.0);
        let style = NSMutableParagraphStyle::new();
        style.setAlignment(match align {
            Align::Left => NSTextAlignment::Left,
            Align::Center => NSTextAlignment::Center,
        });
        style.setLineBreakMode(NSLineBreakMode::ByTruncatingTail);
        let color = self.color(ink);
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
}

impl Pen for AppKitPen {
    fn fill(&self, x: f64, y: f64, w: f64, h: f64, ink: Ink) {
        self.color(ink).setFill();
        NSBezierPath::fillRect(rect(x, y, w, h));
    }

    fn line(&self, x0: f64, y0: f64, x1: f64, y1: f64, ink: Ink) {
        self.color(ink).setStroke();
        NSBezierPath::strokeLineFromPoint_toPoint(NSPoint::new(x0, y0), NSPoint::new(x1, y1));
    }

    fn frame(&self, x: f64, y: f64, w: f64, h: f64, width: f64, ink: Ink) {
        self.color(ink).setStroke();
        let path = NSBezierPath::bezierPathWithRect(rect(x, y, w, h));
        path.setLineWidth(width);
        path.stroke();
    }

    fn text(&self, (x, y, w, h): (f64, f64, f64, f64), text: &str, ink: Ink, align: Align) {
        let attrs = self.attributes(ink, align);
        // SAFETY: drawing inside drawRect: with attributes of the documented types.
        unsafe { ns(text).drawInRect_withAttributes(rect(x, y, w, h), Some(&attrs)) };
    }

    fn text_width(&self, text: &str) -> f64 {
        let attrs = self.attributes(Ink::Label, Align::Left);
        // SAFETY: measuring with attributes of the documented types.
        unsafe { ns(text).sizeWithAttributes(Some(&attrs)) }.width
    }
}
