use crate::flightdeck::layout::{LayoutItem, LayoutItemBuilder, UpdateAction};
use crate::flightdeck::observation::Observation;
use crate::flightdeck::Manager;
use indexmap::map::Entry;
use indexmap::IndexMap;
use indicatif::ProgressBar;
use std::collections::HashMap;

/// ============ 3. ProgressManager ============
///
/// The manager maintains:
///   - A single MultiProgress to which bars are added
///   - An IndexMap of builders in DFS order (type_key -> builder)
///   - A HashMap mapping each type_key to an ordered map (IndexMap) of items keyed by `id`.
///
/// The `observe()` method finds or creates an item. If newly created,
/// it checks if the visible limit allows attaching a bar. If so,
/// it calls `item.create_and_store_bar()` then inserts that bar in
/// MultiProgress after the last visible bar among all builder types
/// up to this type in the DFS ordering.
pub struct ProgressManager {
    multi: indicatif::MultiProgress,

    /// Builders, in DFS order. Each entry is `(type_key -> builder)`.
    /// The insertion order in the IndexMap determines the type ordering.
    builders: IndexMap<String, Box<dyn LayoutItemBuilder>>,

    /// Per-type ordered map of items, keyed by optional ID.
    items: HashMap<String, IndexMap<Option<String>, Box<dyn LayoutItem + Send + Sync>>>,
}

impl Manager for ProgressManager {
    fn observe(&mut self, _: log::Level, obs: Observation) {
        self.observe(obs)
    }

    fn finish(&self) {
        self.multi.suspend(|| {})
    }
}

pub struct LayoutItemBuilderNode {
    pub(crate) builder: Box<dyn LayoutItemBuilder>,
    pub(crate) children: Vec<LayoutItemBuilderNode>,
}

impl LayoutItemBuilderNode {
    pub fn with_children<I>(self, children: I) -> Self
    where
        I: IntoIterator<Item = LayoutItemBuilderNode>,
    {
        Self {
            builder: self.builder,
            children: children.into_iter().collect(),
        }
    }

    pub fn add_child(self, child: impl Into<LayoutItemBuilderNode>) -> Self {
        let mut children = self.children;
        children.push(child.into());
        Self {
            builder: self.builder,
            children,
        }
    }
}

impl From<Box<dyn LayoutItemBuilder>> for LayoutItemBuilderNode {
    fn from(builder: Box<dyn LayoutItemBuilder>) -> Self {
        Self {
            builder,
            children: vec![],
        }
    }
}

impl ProgressManager {
    /// Construct a manager from top-level builders in DFS order.
    /// We'll insert them recursively to get a single IndexMap, ensuring an overall ordering.
    pub(crate) fn new<I>(root_builders: I) -> Self
    where
        I: IntoIterator<Item = LayoutItemBuilderNode>,
    {
        let multi = indicatif::MultiProgress::new();
        let mut builders = IndexMap::new();
        let mut items = HashMap::new();

        // Helper to do DFS insertion
        fn insert_builder_dfs(
            LayoutItemBuilderNode {
                mut builder,
                children,
            }: LayoutItemBuilderNode,
            map: &mut IndexMap<String, Box<dyn LayoutItemBuilder>>,
            items: &mut HashMap<
                String,
                IndexMap<Option<String>, Box<dyn LayoutItem + Send + Sync>>,
            >,
            depth: usize,
        ) {
            builder.set_depth(depth);
            let type_key = builder.type_key().to_owned();
            if !map.contains_key(&type_key) {
                map.insert(type_key.clone(), builder);
                items.insert(type_key.clone(), IndexMap::new());
            }
            for child in children {
                insert_builder_dfs(child, map, items, depth + 1);
            }
        }

        for b in root_builders {
            insert_builder_dfs(b, &mut builders, &mut items, 0);
        }

        Self {
            multi,
            builders,
            items,
        }
    }

    /// Process a new observation: find or create the item, then update it.
    /// If the item is newly created, we check the visible limit.
    /// If update signals `FinishedRemove(pb)`, we remove it from the manager.
    /// If update signals `FinishedKeep`, we do nothing extra.
    fn observe(&mut self, obs: Observation) {
        let type_key = obs.type_key.clone();
        let builder = match self.builders.get(&type_key) {
            None => {
                return;
            } // not known, so ignored
            Some(b) => b,
        };
        let items_map = self.items.entry(type_key.clone()).or_default();
        let id = obs.id.clone();

        match items_map.entry(id.clone()) {
            Entry::Occupied(mut entry) => {
                // Item exists
                let item = entry.get_mut();
                let action = item.update(&obs);
                match action {
                    UpdateAction::FinishedRemove => {
                        let pb = item.get_bar();
                        if let Some(pb) = pb {
                            self.multi.remove(pb);
                        }
                        let map = self.items.get_mut(&type_key).unwrap();
                        map.shift_remove(&id); // TODO: slow!?

                        // Freed a slot => unhide if possible
                        self.maybe_show_bar(&type_key);
                    }
                    UpdateAction::FinishedKeep => { /* keep the item & bar */ }
                    UpdateAction::Continue => { /* do nothing */ }
                }
            }
            Entry::Vacant(vac) => {
                // Create new item
                let new_item = builder.build_item(&obs);
                let _ = vac.insert(new_item);
                // Possibly attach bar if there's room
                self.maybe_show_bar(&type_key);
            }
        }
    }

    /// If a new item was created, we check if we can attach a bar
    /// for items that lack one, up to the visible limit.
    fn maybe_show_bar(&mut self, type_key: &str) {
        let builder = self.builders.get(type_key).unwrap();
        let map = self.items.get_mut(type_key).unwrap();

        let visible_count = map.values().filter(|i| i.get_bar().is_some()).count();
        let can_add = match builder.visible_limit() {
            Some(limit) => visible_count < limit,
            None => true,
        };
        if can_add {
            for (_k, item) in map.iter_mut() {
                if item.get_bar().is_none() {
                    let bar = ProgressBar::hidden();
                    item.set_bar(bar.clone());
                    let id = item.id().clone();
                    self.attach_to_multi_progress(type_key, id, bar.clone());
                    break; // attach only for one new item
                }
            }
        }
    }

    /// The insertion strategy: find the index of `type_key` in `builders`,
    /// gather the last visible bar among all type keys up to that index,
    /// and call `insert_after`; if none is found, just `add`.
    fn attach_to_multi_progress(
        &self,
        type_key: &str,
        id: Option<String>,
        new_bar: indicatif::ProgressBar,
    ) {
        // find index in self.builders
        let idx = match self.builders.get_index_of(type_key) {
            Some(i) => i,
            None => {
                self.multi.add(new_bar);
                return;
            }
        };

        // find last visible bar among type keys from 0..= idx
        let mut candidate = None;
        for i in 0..=idx {
            let (k, _) = self.builders.get_index(i).unwrap();
            if let Some(map) = self.items.get(k) {
                for (_id, it) in map.iter() {
                    if !(it.type_key() == type_key && it.id() == id) {
                        if let Some(pb) = it.get_bar() {
                            candidate = Some(pb);
                        }
                    }
                }
            }
        }
        if let Some(existing) = candidate {
            self.multi.insert_after(existing, new_bar);
        } else {
            self.multi.add(new_bar);
        }
    }
}
