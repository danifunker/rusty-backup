//! Rows, columns and label-and-field pairs for the Win32 shell (docs/win32_shell_plan.md slice 0a).
//!
//! Raw Win32 places every control by hand. A screen describes its controls once as a tree of
//! [`Node`]s in device-independent pixels (96 per inch), and [`layout`] turns the tree into one
//! rectangle per control for the window's client area at its DPI. The shell runs it on `WM_SIZE`
//! and `WM_DPICHANGED` and moves the controls; nothing here touches a window, so it is tested on
//! plain rectangles.

/// A rectangle in physical pixels, relative to the parent's client area.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rect {
    pub x: i32,
    pub y: i32,
    pub w: i32,
    pub h: i32,
}

/// How much of the parent's main axis a node takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Size {
    /// This many device-independent pixels.
    Fixed(i32),
    /// A share of what the fixed nodes leave, by weight.
    Stretch(u32),
}

/// One node of a screen's layout tree; `T` names a control (an id the shell maps to its `HWND`).
#[derive(Clone, Debug, PartialEq)]
pub enum Node<T> {
    /// A control, `width` by `height` where the parent lets it choose.
    Leaf { id: T, width: Size, height: Size },
    /// Room left empty.
    Space(Size),
    /// Children left to right, `gap` DIPs apart.
    Row { children: Vec<Node<T>>, gap: i32 },
    /// Children top to bottom, `gap` DIPs apart.
    Column { children: Vec<Node<T>>, gap: i32 },
    /// A label before its field: the label takes `label_width` DIPs and is centred on the field.
    #[allow(dead_code)] // the screens' forms, from slice 1 on
    Pair {
        label: T,
        field: Box<Node<T>>,
        label_width: i32,
        height: Size,
    },
    /// `child` inset by `margin` DIPs on every side.
    Inset { child: Box<Node<T>>, margin: i32 },
}

/// The standard heights of a Windows dialog row, in DIPs.
pub const ROW: i32 = 23;
pub const GAP: i32 = 7;
pub const MARGIN: i32 = 11;

/// `dips` at `dpi`, rounded to the nearest pixel (what `MulDiv(dips, dpi, 96)` gives).
pub fn scale(dips: i32, dpi: u32) -> i32 {
    let v = i64::from(dips) * i64::from(dpi);
    let r = if v >= 0 { (v + 48) / 96 } else { (v - 48) / 96 };
    r as i32
}

impl<T> Node<T> {
    pub fn leaf(id: T, width: Size, height: Size) -> Self {
        Node::Leaf { id, width, height }
    }

    /// A control one row tall that takes the width its parent gives it.
    pub fn field(id: T) -> Self {
        Node::Leaf {
            id,
            width: Size::Stretch(1),
            height: Size::Fixed(ROW),
        }
    }

    /// A button: `width` DIPs wide and one row tall.
    pub fn button(id: T, width: i32) -> Self {
        Node::Leaf {
            id,
            width: Size::Fixed(width),
            height: Size::Fixed(ROW),
        }
    }

    pub fn row(children: Vec<Node<T>>) -> Self {
        Node::Row { children, gap: GAP }
    }

    pub fn column(children: Vec<Node<T>>) -> Self {
        Node::Column { children, gap: GAP }
    }

    #[allow(dead_code)]
    pub fn pair(label: T, label_width: i32, field: Node<T>) -> Self {
        Node::Pair {
            label,
            field: Box::new(field),
            label_width,
            height: Size::Fixed(ROW),
        }
    }

    pub fn inset(child: Node<T>, margin: i32) -> Self {
        Node::Inset {
            child: Box::new(child),
            margin,
        }
    }

    /// What the node asks of a row's main axis (its width).
    fn width(&self) -> Size {
        match self {
            Node::Leaf { width, .. } => *width,
            Node::Space(s) => *s,
            Node::Row { children, gap } => sum(children.iter().map(Node::width), *gap),
            Node::Column { children, .. } => widest(across(children).map(Node::width)),
            Node::Pair {
                field, label_width, ..
            } => match field.width() {
                Size::Fixed(w) => Size::Fixed(label_width + GAP + w),
                s @ Size::Stretch(_) => s,
            },
            Node::Inset { child, margin } => grow(child.width(), *margin),
        }
    }

    /// The fewest DIPs tall the node can be with every control whole: a stretching leaf (a list)
    /// keeps three rows, a stretching space none.
    pub fn min_height(&self) -> i32 {
        match self {
            Node::Leaf { height, .. } => match height {
                Size::Fixed(v) => *v,
                Size::Stretch(_) => 3 * ROW,
            },
            Node::Space(Size::Fixed(v)) => *v,
            Node::Space(Size::Stretch(_)) => 0,
            Node::Row { children, .. } => {
                across(children).map(Node::min_height).max().unwrap_or(0)
            }
            Node::Column { children, gap } => {
                let n = children.len() as i32;
                children.iter().map(Node::min_height).sum::<i32>() + gap * (n - 1).max(0)
            }
            Node::Pair { height, field, .. } => match height {
                Size::Fixed(v) => *v,
                Size::Stretch(_) => field.min_height(),
            },
            Node::Inset { child, margin } => child.min_height() + 2 * margin,
        }
    }

    /// The tree without the controls `keep` turns down: a pair goes with its field, and a row or
    /// column left with only spaces goes too, so hidden controls take no room.
    pub fn prune(self, keep: &dyn Fn(&T) -> bool) -> Option<Node<T>> {
        let solid = |n: &Node<T>| !matches!(n, Node::Space(_));
        match self {
            Node::Leaf { ref id, .. } => keep(id).then_some(self),
            Node::Space(_) => Some(self),
            Node::Row { children, gap } => {
                let children: Vec<_> = children.into_iter().filter_map(|c| c.prune(keep)).collect();
                children.iter().any(solid).then_some(Node::Row { children, gap })
            }
            Node::Column { children, gap } => {
                let children: Vec<_> = children.into_iter().filter_map(|c| c.prune(keep)).collect();
                children.iter().any(solid).then_some(Node::Column { children, gap })
            }
            Node::Pair {
                label,
                field,
                label_width,
                height,
            } => field.prune(keep).map(|field| Node::Pair {
                label,
                field: Box::new(field),
                label_width,
                height,
            }),
            Node::Inset { child, margin } => child.prune(keep).map(|child| Node::Inset {
                child: Box::new(child),
                margin,
            }),
        }
    }

    /// What the node asks of a column's main axis (its height).
    fn height(&self) -> Size {
        match self {
            Node::Leaf { height, .. } => *height,
            Node::Space(s) => *s,
            Node::Row { children, .. } => widest(across(children).map(Node::height)),
            Node::Column { children, gap } => sum(children.iter().map(Node::height), *gap),
            Node::Pair { height, .. } => *height,
            Node::Inset { child, margin } => grow(child.height(), *margin),
        }
    }
}

/// The children that have a size across their parent's axis: a space only has one along it.
fn across<T>(children: &[Node<T>]) -> impl Iterator<Item = &Node<T>> {
    children.iter().filter(|c| !matches!(c, Node::Space(_)))
}

/// Children in a line: fixed if they all are, else a stretch of their summed weights.
fn sum(sizes: impl Iterator<Item = Size>, gap: i32) -> Size {
    let (mut fixed, mut weight, mut n) = (0, 0, 0);
    for s in sizes {
        n += 1;
        match s {
            Size::Fixed(v) => fixed += v,
            Size::Stretch(w) => weight += w.max(1),
        }
    }
    if weight > 0 {
        Size::Stretch(weight)
    } else {
        Size::Fixed(fixed + gap * (n - 1).max(0))
    }
}

/// Children side by side across the axis: the largest fixed size, or a stretch if any stretches.
fn widest(sizes: impl Iterator<Item = Size>) -> Size {
    let mut best = Size::Fixed(0);
    for s in sizes {
        best = match (best, s) {
            (Size::Fixed(a), Size::Fixed(b)) => Size::Fixed(a.max(b)),
            (Size::Stretch(a), Size::Stretch(b)) => Size::Stretch(a.max(b)),
            (Size::Stretch(a), Size::Fixed(_)) | (Size::Fixed(_), Size::Stretch(a)) => {
                Size::Stretch(a)
            }
        };
    }
    best
}

fn grow(s: Size, margin: i32) -> Size {
    match s {
        Size::Fixed(v) => Size::Fixed(v + 2 * margin),
        s @ Size::Stretch(_) => s,
    }
}

/// Lay `node` out in `area` at `dpi`: one rectangle per control, in tree order.
pub fn layout<T: Copy>(node: &Node<T>, area: Rect, dpi: u32) -> Vec<(T, Rect)> {
    let mut out = Vec::new();
    place(node, area, dpi, &mut out);
    out
}

fn place<T: Copy>(node: &Node<T>, area: Rect, dpi: u32, out: &mut Vec<(T, Rect)>) {
    match node {
        Node::Leaf { id, width, height } => {
            let w = cross(*width, area.w, dpi);
            let h = cross(*height, area.h, dpi);
            out.push((
                *id,
                Rect {
                    x: area.x,
                    y: area.y,
                    w,
                    h,
                },
            ));
        }
        Node::Space(_) => {}
        Node::Row { children, gap } => {
            let sizes: Vec<Size> = children.iter().map(Node::width).collect();
            let spans = split(&sizes, area.w, scale(*gap, dpi), dpi);
            for (child, (at, len)) in children.iter().zip(spans) {
                let r = Rect {
                    x: area.x + at,
                    y: area.y,
                    w: len,
                    h: area.h,
                };
                place(child, r, dpi, out);
            }
        }
        Node::Column { children, gap } => {
            let sizes: Vec<Size> = children.iter().map(Node::height).collect();
            let spans = split(&sizes, area.h, scale(*gap, dpi), dpi);
            for (child, (at, len)) in children.iter().zip(spans) {
                let r = Rect {
                    x: area.x,
                    y: area.y + at,
                    w: area.w,
                    h: len,
                };
                place(child, r, dpi, out);
            }
        }
        Node::Pair {
            label,
            field,
            label_width,
            height,
        } => {
            let h = cross(*height, area.h, dpi);
            let lw = scale(*label_width, dpi).min(area.w);
            let gap = scale(GAP, dpi);
            // A static control draws its text at the top, so it is centred on the field by its own height.
            let text = scale(ROW - 8, dpi).min(h);
            let ly = area.y + (h - text) / 2;
            out.push((
                *label,
                Rect {
                    x: area.x,
                    y: ly,
                    w: lw,
                    h: text,
                },
            ));
            let fx = (lw + gap).min(area.w);
            let r = Rect {
                x: area.x + fx,
                y: area.y,
                w: area.w - fx,
                h,
            };
            place(field, r, dpi, out);
        }
        Node::Inset { child, margin } => {
            let m = scale(*margin, dpi);
            let r = Rect {
                x: area.x + m,
                y: area.y + m,
                w: (area.w - 2 * m).max(0),
                h: (area.h - 2 * m).max(0),
            };
            place(child, r, dpi, out);
        }
    }
}

/// A size across the parent's axis: a fixed size up to what there is, a stretch fills it.
fn cross(s: Size, avail: i32, dpi: u32) -> i32 {
    match s {
        Size::Fixed(v) => scale(v, dpi).min(avail).max(0),
        Size::Stretch(_) => avail.max(0),
    }
}

/// Offsets and lengths along an axis of `total` pixels; stretches share what the rest leave.
fn split(sizes: &[Size], total: i32, gap: i32, dpi: u32) -> Vec<(i32, i32)> {
    let gaps = gap * (sizes.len() as i32 - 1).max(0);
    let fixed: i32 = sizes
        .iter()
        .map(|s| match s {
            Size::Fixed(v) => scale(*v, dpi),
            Size::Stretch(_) => 0,
        })
        .sum();
    let weight: u32 = sizes
        .iter()
        .map(|s| match s {
            Size::Fixed(_) => 0,
            Size::Stretch(w) => (*w).max(1),
        })
        .sum();
    let mut left = (total - fixed - gaps).max(0);
    let mut weight_left = weight;
    let mut at = 0;
    let mut out = Vec::with_capacity(sizes.len());
    for s in sizes {
        let len = match s {
            Size::Fixed(v) => scale(*v, dpi),
            Size::Stretch(w) => {
                // The last stretch takes the remainder, so rounding never leaves a gap at the end.
                let w = (*w).max(1);
                let len = if w == weight_left {
                    left
                } else {
                    left * w as i32 / weight_left as i32
                };
                left -= len;
                weight_left -= w;
                len
            }
        };
        let len = len.min((total - at).max(0));
        out.push((at, len));
        at += len + gap;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(x: i32, y: i32, w: i32, h: i32) -> Rect {
        Rect { x, y, w, h }
    }

    #[test]
    fn scaling_rounds_like_muldiv() {
        assert_eq!(scale(23, 96), 23);
        assert_eq!(scale(23, 144), 35);
        assert_eq!(scale(7, 120), 9);
        assert_eq!(scale(-7, 120), -9);
    }

    #[test]
    fn a_row_gives_stretches_what_fixed_controls_leave() {
        let n = Node::row(vec![
            Node::button(1, 75),
            Node::field(2),
            Node::button(3, 75),
        ]);
        let out = layout(&n, r(0, 0, 400, 23), 96);
        assert_eq!(
            out,
            vec![
                (1, r(0, 0, 75, 23)),
                (2, r(82, 0, 236, 23)),
                (3, r(325, 0, 75, 23))
            ]
        );
    }

    #[test]
    fn weights_share_the_rest_and_the_last_takes_the_remainder() {
        let n: Node<u8> = Node::Row {
            children: vec![
                Node::leaf(1, Size::Stretch(1), Size::Stretch(1)),
                Node::leaf(2, Size::Stretch(2), Size::Stretch(1)),
            ],
            gap: 0,
        };
        let out = layout(&n, r(0, 0, 100, 10), 96);
        assert_eq!(out, vec![(1, r(0, 0, 33, 10)), (2, r(33, 0, 67, 10))]);
    }

    #[test]
    fn a_column_of_pairs_lines_labels_up_and_a_list_takes_the_height() {
        let n = Node::inset(
            Node::column(vec![
                Node::pair(10, 100, Node::field(11)),
                Node::pair(12, 100, Node::field(13)),
                Node::leaf(14, Size::Stretch(1), Size::Stretch(1)),
            ]),
            MARGIN,
        );
        let out = layout(&n, r(0, 0, 500, 300), 96);
        assert_eq!(out[0], (10, r(11, 15, 100, 15)));
        assert_eq!(out[1], (11, r(118, 11, 371, 23)));
        assert_eq!(out[3], (13, r(118, 41, 371, 23)));
        assert_eq!(out[4], (14, r(11, 71, 478, 218)));
    }

    #[test]
    fn everything_scales_with_the_dpi() {
        let n = Node::inset(Node::row(vec![Node::button(1, 75), Node::field(2)]), MARGIN);
        let out = layout(&n, r(0, 0, 600, 100), 144);
        assert_eq!(out, vec![(1, r(17, 17, 113, 35)), (2, r(141, 17, 442, 35))]);
    }

    #[test]
    fn a_window_too_small_clips_rather_than_overlapping() {
        let n = Node::row(vec![Node::button(1, 75), Node::button(2, 75)]);
        let out = layout(&n, r(0, 0, 100, 23), 96);
        assert_eq!(out, vec![(1, r(0, 0, 75, 23)), (2, r(82, 0, 18, 23))]);
        let out = layout(&n, r(0, 0, 0, 0), 96);
        assert!(out.iter().all(|(_, rc)| rc.w >= 0 && rc.h >= 0));
    }

    #[test]
    fn a_stretching_space_does_not_make_its_row_tall() {
        let bar = Node::row(vec![Node::button(1, 75), Node::Space(Size::Stretch(1))]);
        assert_eq!(bar.height(), Size::Fixed(ROW));
        let n = Node::column(vec![bar, Node::leaf(2, Size::Stretch(1), Size::Stretch(1))]);
        let out = layout(&n, r(0, 0, 200, 200), 96);
        assert_eq!(out, vec![(1, r(0, 0, 75, 23)), (2, r(0, 30, 200, 170))]);
    }

    #[test]
    fn min_height_counts_every_row_and_keeps_a_list_visible() {
        let n = Node::inset(
            Node::column(vec![
                Node::row(vec![Node::button(1, 75), Node::Space(Size::Stretch(1))]),
                Node::leaf(2, Size::Stretch(1), Size::Stretch(1)),
                Node::Space(Size::Stretch(1)),
                Node::button(3, 75),
            ]),
            MARGIN,
        );
        assert_eq!(n.min_height(), ROW + 3 * ROW + ROW + 3 * GAP + 2 * MARGIN);
    }

    #[test]
    fn an_indenting_space_does_not_make_its_row_tall() {
        let indented = Node::row(vec![Node::Space(Size::Fixed(120)), Node::button(1, 75)]);
        assert_eq!(indented.min_height(), ROW);
        let n = Node::column(vec![indented, Node::Space(Size::Fixed(10))]);
        assert_eq!(n.min_height(), ROW + GAP + 10);
    }

    #[test]
    fn pruning_drops_hidden_controls_and_the_rows_they_leave_empty() {
        let n = Node::column(vec![
            Node::button(1, 75),
            Node::row(vec![Node::button(2, 75), Node::Space(Size::Stretch(1))]),
            Node::pair(3, 80, Node::field(4)),
            Node::Space(Size::Stretch(1)),
            Node::button(5, 75),
        ]);
        let shown = n.prune(&|id| *id != 2 && *id != 4).unwrap();
        assert_eq!(shown.min_height(), 2 * ROW + 2 * GAP);
        let out = layout(&shown, r(0, 0, 200, 200), 96);
        assert_eq!(out, vec![(1, r(0, 0, 75, 23)), (5, r(0, 177, 75, 23))]);
    }

    #[test]
    fn nested_sizes_report_up_the_tree() {
        let pair = Node::pair(1, 80, Node::button(2, 75));
        assert_eq!(pair.width(), Size::Fixed(80 + GAP + 75));
        let col = Node::column(vec![Node::field(3), Node::field(4)]);
        assert_eq!(col.height(), Size::Fixed(ROW * 2 + GAP));
        assert_eq!(
            Node::inset(col, 5).height(),
            Size::Fixed(ROW * 2 + GAP + 10)
        );
    }
}
