//! A flat, depth-annotated row list mirrored into a `gtk::TreeListModel`.
//!
//! The controller owns the tree: its rows arrive flattened in display order, so with every
//! expansion applied a row's list position is its index here. Rows change in place, and a row is
//! only spliced when its siblings changed, so scroll position and open folders survive a sync.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use gtk::prelude::*;
use gtk::{gio, glib};

/// One row of the tree.
#[derive(Clone, PartialEq)]
pub struct Node<T> {
    pub id: String,
    pub depth: usize,
    pub expandable: bool,
    pub expanded: bool,
    pub data: T,
}

type Stores = Rc<RefCell<HashMap<String, gio::ListStore>>>;
/// Each parent's rows as last synced, keyed by the parent's id (`None`: the root).
type Groups<T> = HashMap<Option<String>, Vec<Node<T>>>;

pub struct FlatTree<T> {
    root: gio::ListStore,
    model: gtk::TreeListModel,
    stores: Stores,
    groups: RefCell<Groups<T>>,
    open: RefCell<HashMap<String, gtk::TreeListRow>>,
}

fn boxed<T: Clone + 'static>(node: &Node<T>) -> glib::BoxedAnyObject {
    glib::BoxedAnyObject::new(node.clone())
}

impl<T: Clone + PartialEq + 'static> FlatTree<T> {
    pub fn new() -> FlatTree<T> {
        let root = gio::ListStore::new::<glib::BoxedAnyObject>();
        let stores: Stores = Rc::default();
        let children = stores.clone();
        let model = gtk::TreeListModel::new(root.clone(), false, false, move |item| {
            let item = item.downcast_ref::<glib::BoxedAnyObject>()?;
            let node = item.borrow::<Node<T>>();
            node.expandable.then(|| {
                let mut stores = children.borrow_mut();
                let store = stores
                    .entry(node.id.clone())
                    .or_insert_with(gio::ListStore::new::<glib::BoxedAnyObject>);
                store.clone().upcast()
            })
        });
        FlatTree {
            root,
            model,
            stores,
            groups: RefCell::default(),
            open: RefCell::default(),
        }
    }

    pub fn model(&self) -> &gtk::TreeListModel {
        &self.model
    }

    /// The user toggled `row`; the model already shows it, so a later sync has nothing to redo.
    pub fn note_toggled(&self, row: &gtk::TreeListRow, id: String) {
        let mut open = self.open.borrow_mut();
        if row.is_expanded() {
            open.insert(id, row.clone());
        } else {
            open.remove(&id);
        }
    }

    /// Make the model show `nodes`; run it inside a sync, as it fires `expanded` notifications.
    pub fn sync(&self, mut nodes: Vec<Node<T>>) {
        // A row with rows below it is open, whatever it says: a staged folder shows its adds so.
        for i in 0..nodes.len() {
            if nodes.get(i + 1).is_some_and(|n| n.depth > nodes[i].depth) {
                nodes[i].expandable = true;
                nodes[i].expanded = true;
            }
        }
        let mut groups: Groups<T> = HashMap::new();
        let mut order: Vec<Option<String>> = vec![None];
        let mut stack: Vec<(usize, String)> = Vec::new();
        let mut want: Vec<(u32, String)> = Vec::new();
        groups.insert(None, Vec::new());
        for (i, node) in nodes.into_iter().enumerate() {
            while stack.last().is_some_and(|(d, _)| *d >= node.depth) {
                stack.pop();
            }
            let parent = stack.last().map(|(_, id)| id.clone());
            if node.expanded {
                want.push((i as u32, node.id.clone()));
                stack.push((node.depth, node.id.clone()));
                let group = Some(node.id.clone());
                if groups.insert(group.clone(), Vec::new()).is_none() {
                    order.push(group);
                }
            }
            groups.entry(parent).or_default().push(node);
        }

        let wanted: HashSet<&str> = want.iter().map(|(_, id)| id.as_str()).collect();
        let gone: Vec<String> = self
            .open
            .borrow()
            .keys()
            .filter(|id| !wanted.contains(id.as_str()))
            .cloned()
            .collect();
        for id in gone {
            let row = self.open.borrow_mut().remove(&id);
            if let Some(row) = row {
                row.set_expanded(false);
            }
        }

        let mut shadow = self.groups.take();
        for key in &order {
            let store = match key {
                None => self.root.clone(),
                Some(id) => {
                    let mut stores = self.stores.borrow_mut();
                    let store = stores
                        .entry(id.clone())
                        .or_insert_with(gio::ListStore::new::<glib::BoxedAnyObject>);
                    store.clone()
                }
            };
            let old = shadow.remove(key).unwrap_or_default();
            update_store(&store, &old, &groups[key]);
        }
        // What is left belongs to folders no longer open.
        for key in shadow.into_keys().flatten() {
            if let Some(store) = self.stores.borrow_mut().remove(&key) {
                store.remove_all();
            }
        }
        *self.groups.borrow_mut() = groups;

        for (position, id) in want {
            let alive = self
                .open
                .borrow()
                .get(&id)
                .is_some_and(|r| r.item().is_some() && r.is_expanded());
            if alive {
                continue;
            }
            let Some(row) = self.model.row(position) else {
                continue;
            };
            row.set_expanded(true);
            self.open.borrow_mut().insert(id, row);
        }
    }
}

/// Bring `store` from `old` to `new`, touching only what changed.
fn update_store<T: Clone + PartialEq + 'static>(
    store: &gio::ListStore,
    old: &[Node<T>],
    new: &[Node<T>],
) {
    let same = |a: &Node<T>, b: &Node<T>| a.id == b.id && a.expandable == b.expandable;
    let head = old.iter().zip(new).take_while(|(a, b)| same(a, b)).count();
    let tail = old[head..]
        .iter()
        .rev()
        .zip(new[head..].iter().rev())
        .take_while(|(a, b)| same(a, b))
        .count();
    let renew = |at: u32, node: &Node<T>| {
        let item = store.item(at).and_downcast::<glib::BoxedAnyObject>();
        if let Some(item) = item {
            *item.borrow_mut::<Node<T>>() = node.clone();
        }
    };
    for i in 0..head {
        if old[i] != new[i] {
            renew(i as u32, &new[i]);
        }
    }
    for k in 0..tail {
        let (a, b) = (old.len() - 1 - k, new.len() - 1 - k);
        if old[a] != new[b] {
            renew(a as u32, &new[b]);
        }
    }
    let removed = old.len() - head - tail;
    let added = &new[head..new.len() - tail];
    if removed > 0 || !added.is_empty() {
        let objects: Vec<_> = added.iter().map(boxed).collect();
        store.splice(head as u32, removed as u32, &objects);
    }
}
