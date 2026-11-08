//! The disk-layout bar: a cairo `DrawingArea` and its legend. Click a segment to select its partition.

use std::cell::RefCell;
use std::rc::Rc;

use adw::prelude::*;
use rusty_backup::controller::inspect::{
    ids, segment_at, segment_widths, LayoutSegment, SegmentKind, BAR_HEIGHT, MIN_SEGMENT_PX,
};

use crate::theme::{self, Rgb};

fn fill(kind: SegmentKind) -> Rgb {
    match kind {
        SegmentKind::Partition { color_index } => theme::partition_fill(color_index),
        SegmentKind::Free => theme::free_space(),
        SegmentKind::Dimmed => theme::dimmed(),
    }
}

#[derive(Default)]
struct Drawn {
    segments: Vec<LayoutSegment>,
    selected: Option<usize>,
    /// The part of the width left blank, for bars drawn to one scale.
    blank: f32,
}

impl Drawn {
    fn widths(&self, width: i32) -> Vec<f32> {
        let width = width as f32 * (1.0 - self.blank);
        segment_widths(&self.segments, width, MIN_SEGMENT_PX)
    }

    /// The segment under `x` on a bar `width` wide.
    fn at(&self, width: i32, x: f64) -> Option<&LayoutSegment> {
        let i = segment_at(&self.widths(width), x as f32)?;
        self.segments.get(i)
    }
}

pub struct PartitionBar {
    root: gtk::Box,
    area: gtk::DrawingArea,
    legend: gtk::FlowBox,
    drawn: Rc<RefCell<Drawn>>,
}

impl PartitionBar {
    /// `on_select` gets the clicked segment's partition, or `None` for free space.
    pub fn build(on_select: impl Fn(Option<usize>) + 'static) -> PartitionBar {
        let drawn = Rc::new(RefCell::new(Drawn::default()));
        let area = gtk::DrawingArea::builder()
            .content_height(BAR_HEIGHT as i32)
            .hexpand(true)
            .has_tooltip(true)
            .build();
        area.set_widget_name(ids::PARTITION_BAR);

        let d = drawn.clone();
        area.set_draw_func(move |area, cr, width, height| {
            draw(area, cr, &d.borrow(), width, height);
        });

        let click = gtk::GestureClick::new();
        let d = drawn.clone();
        let target = area.clone();
        click.connect_pressed(move |_, _, x, _| {
            let picked = {
                let drawn = d.borrow();
                let segment = drawn.at(target.width(), x);
                segment.map(|s| s.partition)
            };
            if let Some(partition) = picked {
                on_select(partition);
            }
        });
        area.add_controller(click);

        let d = drawn.clone();
        area.connect_query_tooltip(move |area, x, _, _, tooltip| {
            let drawn = d.borrow();
            let Some(segment) = drawn.at(area.width(), f64::from(x)) else {
                return false;
            };
            tooltip.set_text(Some(&segment.tooltip_text()));
            true
        });

        let target = area.clone();
        adw::StyleManager::default().connect_dark_notify(move |_| target.queue_draw());

        let legend = gtk::FlowBox::builder()
            .selection_mode(gtk::SelectionMode::None)
            .homogeneous(false)
            .max_children_per_line(12)
            .column_spacing(12)
            .build();
        let root = gtk::Box::new(gtk::Orientation::Vertical, 4);
        root.append(&area);
        root.append(&legend);
        PartitionBar {
            root,
            area,
            legend,
            drawn,
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    /// Draw into `scale` of the width, so two bars can share a byte-per-pixel scale.
    pub fn set_scale(&self, scale: f32) {
        let blank = 1.0 - scale.clamp(0.0, 1.0);
        let mut drawn = self.drawn.borrow_mut();
        if drawn.blank != blank {
            drawn.blank = blank;
            self.area.queue_draw();
        }
    }

    pub fn hide_legend(&self) {
        self.legend.set_visible(false);
    }

    pub fn show(&self, segments: Vec<LayoutSegment>, selected: Option<usize>) {
        let mut drawn = self.drawn.borrow_mut();
        if drawn.segments == segments && drawn.selected == selected {
            return;
        }
        if drawn.segments != segments {
            self.legend.remove_all();
            for segment in &segments {
                self.legend.append(&legend_item(segment));
            }
        }
        drawn.segments = segments;
        drawn.selected = selected;
        self.area.queue_draw();
    }
}

fn legend_item(segment: &LayoutSegment) -> gtk::Box {
    let color = fill(segment.kind);
    let swatch = gtk::DrawingArea::builder()
        .content_width(12)
        .content_height(12)
        .valign(gtk::Align::Center)
        .build();
    swatch.set_draw_func(move |_, cr, width, height| {
        theme::set_source(cr, color);
        cr.rectangle(0.0, 0.0, f64::from(width), f64::from(height));
        let _ = cr.fill();
    });
    let label = gtk::Label::new(Some(&segment.legend_text()));
    if !matches!(segment.kind, SegmentKind::Partition { .. }) {
        label.add_css_class("dim-label");
    }
    let item = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    item.append(&swatch);
    item.append(&label);
    item
}

fn draw(area: &gtk::DrawingArea, cr: &gtk::cairo::Context, drawn: &Drawn, width: i32, height: i32) {
    let h = f64::from(height);
    let ink = area.color();
    let mut x = 0.0_f64;
    let mut selected_rect = None;
    for (segment, w) in drawn.segments.iter().zip(drawn.widths(width)) {
        let w = f64::from(w);
        theme::set_source(cr, fill(segment.kind));
        cr.rectangle(x, 0.0, w, h);
        let _ = cr.fill();

        if let Some(text) = segment.inline_label(w as f32) {
            let layout = area.create_pango_layout(Some(&text));
            layout.set_width((w - 8.0).max(0.0) as i32 * gtk::pango::SCALE);
            layout.set_ellipsize(gtk::pango::EllipsizeMode::End);
            layout.set_alignment(gtk::pango::Alignment::Center);
            let (_, text_h) = layout.pixel_size();
            theme::set_source(cr, theme::on_partition());
            cr.move_to(x + 4.0, (h - f64::from(text_h)) / 2.0);
            pangocairo::functions::show_layout(cr, &layout);
        }

        if segment.partition.is_some() && segment.partition == drawn.selected {
            selected_rect = Some((x, w));
        }
        x += w;
        cr.set_source_rgba(
            f64::from(ink.red()),
            f64::from(ink.green()),
            f64::from(ink.blue()),
            0.35,
        );
        cr.set_line_width(1.0);
        cr.move_to(x, 0.0);
        cr.line_to(x, h);
        let _ = cr.stroke();
    }
    if let Some((x, w)) = selected_rect {
        cr.set_source_rgb(
            f64::from(ink.red()),
            f64::from(ink.green()),
            f64::from(ink.blue()),
        );
        cr.set_line_width(3.0);
        cr.rectangle(x + 1.5, 1.5, (w - 3.0).max(1.0), h - 3.0);
        let _ = cr.stroke();
    }
}
