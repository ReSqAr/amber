use crate::flightdeck::base;
use crate::flightdeck::layout::{LayoutItem, LayoutItemBuilder, UpdateAction};
use crate::flightdeck::observation::Observation;
use indexmap::IndexMap;
use indexmap::map::Entry;
use std::collections::{HashMap, HashSet};
use tokio::task;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
enum Key {
    Body(String),
    Footer(String),
}

enum Builder {
    Body(Box<dyn LayoutItemBuilder>),
    Footer { depth: usize },
}

enum Item {
    Body(Box<dyn LayoutItem>),
    Footer(FooterLayoutItem),
}

impl Item {
    pub(crate) fn id(&self) -> Option<String> {
        match self {
            Item::Body(item) => item.id(),
            Item::Footer(_) => None,
        }
    }

    pub(crate) fn tick(&mut self) {
        match self {
            Item::Body(item) => item.tick(),
            Item::Footer(item) => item.tick(),
        }
    }

    pub(crate) fn get_bar(&self) -> Option<&indicatif::ProgressBar> {
        match self {
            Item::Body(item) => item.get_bar(),
            Item::Footer(item) => item.get_bar(),
        }
    }

    pub(crate) fn set_bar(&mut self, pb: indicatif::ProgressBar) {
        match self {
            Item::Body(item) => item.set_bar(pb),
            Item::Footer(item) => item.set_bar(pb),
        }
    }
}

pub struct ProgressBarPipe {
    multi: indicatif::MultiProgress,

    /// Builders, in DFS order. Each entry is `(type_key -> builder)`.
    /// The insertion order in the IndexMap determines the type ordering.
    builders: IndexMap<Key, Builder>,

    /// Per-type ordered map of items, keyed by optional ID.
    items: HashMap<Key, IndexMap<Option<String>, Item>>,

    type_keys: HashSet<String>,
    progress_bars_to_delete: Vec<indicatif::ProgressBar>,
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

struct FooterLayoutItem {
    depth: usize,
    visible_count: u64,
    total_count: u64,
    pb: Option<indicatif::ProgressBar>,
}

impl FooterLayoutItem {
    fn new(depth: usize, visible_count: u64, total_count: u64) -> Self {
        Self {
            depth,
            visible_count,
            total_count,
            pb: None,
        }
    }

    fn tick(&self) {
        if let Some(pb) = &self.pb {
            self.update_bar(pb);
        }
    }

    fn update_bar(&self, bar: &indicatif::ProgressBar) {
        let hidden_count = self.total_count.saturating_sub(self.visible_count);
        let msg = format!("[{} hidden]", hidden_count);
        bar.set_message(msg);
    }

    fn set_bar(&mut self, bar: indicatif::ProgressBar) {
        bar.set_style(indicatif::ProgressStyle::with_template("{prefix}{msg}").unwrap());
        bar.set_prefix(base::prefix_from_depth(self.depth));
        self.update_bar(&bar);
        self.pb = Some(bar);
    }

    fn set(&mut self, visible_count: u64, total_count: u64) {
        self.visible_count = visible_count;
        self.total_count = total_count;
        if let Some(pb) = &self.pb {
            self.update_bar(pb);
        }
    }

    fn get_bar(&self) -> Option<&indicatif::ProgressBar> {
        self.pb.as_ref()
    }
}

impl ProgressBarPipe {
    /// Construct a manager from top-level builders in DFS order.
    /// We'll insert them recursively to get a single IndexMap, ensuring an overall ordering.
    pub(crate) fn new<I>(multi: indicatif::MultiProgress, root_builders: I) -> Self
    where
        I: IntoIterator<Item = LayoutItemBuilderNode>,
    {
        let mut builders = IndexMap::new();
        let mut items = HashMap::new();

        // Helper to do DFS insertion
        fn insert_builder_dfs(
            LayoutItemBuilderNode {
                mut builder,
                children,
            }: LayoutItemBuilderNode,
            map: &mut IndexMap<Key, Builder>,
            items: &mut HashMap<Key, IndexMap<Option<String>, Item>>,
            depth: usize,
        ) {
            builder.set_depth(depth);
            let type_key = builder.type_key().to_owned();

            let key = Key::Body(type_key.clone());
            if !map.contains_key(&key) {
                map.insert(key.clone(), Builder::Body(builder));
                items.insert(key, IndexMap::new());
            }

            let key = Key::Footer(type_key.clone());
            if !map.contains_key(&key) {
                map.insert(key.clone(), Builder::Footer { depth });
                items.insert(key, IndexMap::new());
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
            type_keys: HashSet::new(),
            progress_bars_to_delete: vec![],
        }
    }

    /// Process a new observation: find or create the item, then update it.
    /// If the item is newly created, we check the visible limit.
    /// If update signals `FinishedRemove(pb)`, we remove it from the manager.
    /// If update signals `FinishedKeep`, we do nothing extra.
    pub(crate) fn observe(&mut self, _: log::Level, obs: Observation) {
        let type_key = obs.type_key.clone();
        let key = Key::Body(type_key.clone());
        let id = obs.id.clone();

        // manage LayoutItem lifecycle
        let items_map = self.items.entry(key.clone()).or_default();
        match items_map.entry(id.clone()) {
            Entry::Occupied(mut entry) => {
                if let Item::Body(item) = entry.get_mut() {
                    let action = item.update(&obs);
                    match action {
                        UpdateAction::FinishedRemove => {
                            let pb = item.get_bar().cloned();
                            let map = self.items.get_mut(&key).unwrap();
                            map.shift_remove(&id); // maybe this is slow!?

                            self.type_keys.insert(type_key);
                            if let Some(pb) = pb {
                                self.progress_bars_to_delete.push(pb)
                            }
                        }
                        UpdateAction::FinishedKeep => {}
                        UpdateAction::Continue => {}
                    }
                }
            }
            Entry::Vacant(vac) => {
                let builder = match self.builders.get(&key) {
                    None => return,
                    Some(Builder::Body(b)) => b,
                    Some(Builder::Footer { .. }) => return,
                };
                let new_item = builder.build_item(&obs);
                let _ = vac.insert(Item::Body(new_item));
                self.type_keys.insert(type_key);
            }
        };
    }

    pub(crate) async fn flush(&mut self) {
        for items in self.items.values_mut() {
            for item in items.values_mut() {
                item.tick();
            }
        }

        if self.type_keys.is_empty() {
            return;
        }

        let multi = self.multi.clone();
        let pbs = &self.progress_bars_to_delete;
        for pb in pbs {
            multi.remove(pb);
        }
        self.progress_bars_to_delete = vec![];

        let type_keys = self.type_keys.clone();
        self.type_keys.clear();
        for type_key in type_keys {
            let key = Key::Body(type_key.clone());
            let visible_limit = {
                let builder = match self.builders.get(&key) {
                    None => return,
                    Some(Builder::Body(b)) => b,
                    Some(Builder::Footer { .. }) => return,
                };
                builder.visible_limit()
            };

            self.process_type_key(&type_key, visible_limit).await;
        }
    }

    pub(crate) async fn finish(&mut self) {
        self.flush().await;

        let multi = self.multi.clone();
        task::spawn_blocking(move || multi.suspend(|| {}))
            .await
            .expect("couldn't join finish");
    }

    /// If a new item was created, we check if we can attach a bar
    /// for items that lack one, up to the visible limit.
    async fn process_type_key(&mut self, type_key: &str, visible_limit: Option<usize>) {
        let key = Key::Body(type_key.to_string());

        while {
            let map = self.items.get_mut(&key).unwrap();
            let total_count = map.values().count();
            let visible_count = map.values().filter(|i| i.get_bar().is_some()).count();

            match visible_limit {
                Some(limit) => visible_count < total_count && visible_count < limit,
                None => visible_count < total_count,
            }
        } {
            for (_k, item) in self.items.get_mut(&key).unwrap().iter_mut() {
                if item.get_bar().is_none() {
                    let bar = indicatif::ProgressBar::hidden();
                    item.set_bar(bar.clone());
                    let id = item.id().clone();
                    self.attach_to_multi_progress(&key, id, bar.clone()).await;
                    break; // attach only one new item
                }
            }
        }

        if let Some(limit) = visible_limit {
            self.process_type_key_footer(type_key, limit).await;
        }
    }

    async fn process_type_key_footer(&mut self, type_key: &str, limit: usize) {
        let key = Key::Body(type_key.to_string());
        let map = self.items.get_mut(&key).unwrap();
        let visible_count = map.values().filter(|i| i.get_bar().is_some()).count();
        let total_count = map.values().count();
        let footer_required = visible_count >= limit;
        let key = Key::Footer(type_key.to_string());

        let items_map = self.items.entry(key.clone()).or_default();
        match items_map.entry(None) {
            Entry::Occupied(mut entry) => {
                if let Item::Footer(item) = entry.get_mut() {
                    if footer_required {
                        item.set(visible_count as u64, total_count as u64);
                    } else {
                        let pb = item.get_bar();
                        if let Some(pb) = pb {
                            self.multi.remove(pb);
                        }
                        let map = self.items.get_mut(&key).unwrap();
                        map.shift_remove(&None);
                    }
                }
            }
            Entry::Vacant(vac) => {
                if footer_required {
                    let depth = match self.builders.get(&key) {
                        None => return,
                        Some(Builder::Body(_)) => return,
                        Some(Builder::Footer { depth }) => depth,
                    };

                    let mut item =
                        FooterLayoutItem::new(*depth, visible_count as u64, total_count as u64);
                    let bar = indicatif::ProgressBar::hidden();
                    item.set_bar(bar.clone());
                    let _ = vac.insert(Item::Footer(item));
                    self.attach_to_multi_progress(&key, None, bar).await;
                }
            }
        }
    }

    /// The insertion strategy: find the index of `type_key` in `builders`,
    /// gather the last visible bar among all type keys up to that index,
    /// and call `insert_after`; if none is found, just `add`.
    #[allow(clippy::collapsible_if)]
    async fn attach_to_multi_progress(
        &self,
        key: &Key,
        id: Option<String>,
        new_bar: indicatif::ProgressBar,
    ) {
        // find index in self.builders
        let idx = match self.builders.get_index_of(key) {
            Some(i) => i,
            None => {
                self.multi.add(new_bar.clone());
                new_bar.tick();
                return;
            }
        };

        // find last visible bar among type keys from 0..= idx
        let mut candidate = None;
        for i in 0..=idx {
            let (k, _) = self.builders.get_index(i).unwrap();
            if let Some(map) = self.items.get(k) {
                for (_id, it) in map.iter() {
                    if !(k == key && it.id() == id) {
                        if let Some(pb) = it.get_bar() {
                            candidate = Some(pb);
                        }
                    }
                }
            }
        }
        if let Some(existing) = candidate {
            self.multi.insert_after(existing, new_bar.clone());
        } else {
            self.multi.insert(0, new_bar.clone());
        }
        task::spawn_blocking(move || {
            new_bar.tick();
        });
    }
}

#[allow(clippy::indexing_slicing)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::flightdeck::base::{BaseLayoutBuilderBuilder, StateTransformer, TerminationAction};
    use crate::flightdeck::layout::{LayoutItem, LayoutItemBuilder, UpdateAction};
    use crate::flightdeck::observation::{Data, Observation, Value};

    use chrono::Utc;
    use indicatif::{MultiProgress, ProgressBar};
    use std::sync::{Arc, Mutex};

    fn create_observation(
        type_key: &str,
        id: Option<&str>,
        state: &str,
        is_terminal: bool,
    ) -> Observation {
        Observation {
            type_key: type_key.to_string(),
            id: id.map(|s| s.to_string()),
            timestamp: Utc::now(),
            is_terminal,
            data: vec![Data {
                key: "state".to_string(),
                value: Value::String(state.to_string()),
            }],
        }
    }

    fn create_test_layout_builder(type_key: &str) -> Box<dyn LayoutItemBuilder> {
        BaseLayoutBuilderBuilder::default()
            .type_key(type_key)
            .termination_action(TerminationAction::Keep)
            .state_transformer(StateTransformer::Identity)
            .infallible_build()
            .boxed()
    }

    #[test]
    fn test_layout_item_builder_node_creation() {
        let builder = create_test_layout_builder("task");
        let node = LayoutItemBuilderNode::from(builder);

        assert!(node.children.is_empty());
        assert_eq!(node.builder.type_key(), "task");
    }

    #[test]
    fn test_layout_item_builder_node_with_children() {
        let parent_builder = create_test_layout_builder("parent");
        let child1_builder = create_test_layout_builder("child1");
        let child2_builder = create_test_layout_builder("child2");

        let node = LayoutItemBuilderNode::from(parent_builder).with_children(vec![
            LayoutItemBuilderNode::from(child1_builder),
            LayoutItemBuilderNode::from(child2_builder),
        ]);

        assert_eq!(node.children.len(), 2);
        assert_eq!(node.builder.type_key(), "parent");
        assert_eq!(node.children[0].builder.type_key(), "child1");
        assert_eq!(node.children[1].builder.type_key(), "child2");
    }

    #[test]
    fn test_layout_item_builder_node_add_child() {
        let parent_builder = create_test_layout_builder("parent");
        let child_builder = create_test_layout_builder("child");

        let node = LayoutItemBuilderNode::from(parent_builder)
            .add_child(LayoutItemBuilderNode::from(child_builder));

        assert_eq!(node.children.len(), 1);
        assert_eq!(node.builder.type_key(), "parent");
        assert_eq!(node.children[0].builder.type_key(), "child");
    }

    #[test]
    fn test_progress_bar_pipe_initialization() {
        let parent_builder = create_test_layout_builder("parent");
        let child1_builder = create_test_layout_builder("child1");
        let child2_builder = create_test_layout_builder("child2");

        let root = LayoutItemBuilderNode::from(parent_builder).with_children(vec![
            LayoutItemBuilderNode::from(child1_builder),
            LayoutItemBuilderNode::from(child2_builder),
        ]);

        let multi = MultiProgress::new();
        let mut pipe = ProgressBarPipe::new(multi, vec![root]);

        let parent_obs = create_observation("parent", Some("1"), "running", false);
        let child1_obs = create_observation("child1", Some("11"), "starting", false);

        pipe.observe(log::Level::Info, parent_obs);
        pipe.observe(log::Level::Info, child1_obs);
    }

    struct MockItem {
        id: Option<String>,
        bar: Option<ProgressBar>,
        update_count: Arc<Mutex<usize>>,
        tick_count: Arc<Mutex<usize>>,
        update_action: UpdateAction,
    }

    impl MockItem {
        fn new(id: Option<String>, update_action: UpdateAction) -> Self {
            Self {
                id,
                bar: None,
                update_count: Arc::new(Mutex::new(0)),
                tick_count: Arc::new(Mutex::new(0)),
                update_action,
            }
        }
    }

    impl LayoutItem for MockItem {
        fn id(&self) -> Option<String> {
            self.id.clone()
        }

        fn update(&mut self, _obs: &Observation) -> UpdateAction {
            if let Ok(mut count) = self.update_count.lock() {
                *count += 1;
            }
            self.update_action.clone()
        }

        fn tick(&mut self) {
            if let Ok(mut count) = self.tick_count.lock() {
                *count += 1;
            }
            if let Some(bar) = &self.bar {
                bar.tick();
            }
        }

        fn set_bar(&mut self, bar: ProgressBar) {
            self.bar = Some(bar);
        }

        fn get_bar(&self) -> Option<&ProgressBar> {
            self.bar.as_ref()
        }
    }

    struct MockBuilder {
        type_key: String,
        depth: usize,
        limit: Option<usize>,
        mock_item: Arc<Mutex<Option<MockItem>>>,
    }

    impl MockBuilder {
        fn new(type_key: &str, depth: usize, limit: Option<usize>, item: MockItem) -> Self {
            Self {
                type_key: type_key.to_string(),
                depth,
                limit,
                mock_item: Arc::new(Mutex::new(Some(item))),
            }
        }

        fn boxed(self) -> Box<dyn LayoutItemBuilder> {
            Box::new(self)
        }
    }

    impl LayoutItemBuilder for MockBuilder {
        fn type_key(&self) -> &str {
            &self.type_key
        }

        fn set_depth(&mut self, depth: usize) {
            self.depth = depth;
        }

        fn visible_limit(&self) -> Option<usize> {
            self.limit
        }

        fn build_item(&self, _obs: &Observation) -> Box<dyn LayoutItem> {
            if let Ok(mut mock_item) = self.mock_item.lock() {
                Box::new(mock_item.take().expect("MockItem already taken"))
            } else {
                panic!("Failed to lock mock_item");
            }
        }
    }

    #[tokio::test]
    async fn test_progress_bar_pipe_observable_lifecycle() {
        let mock_item = MockItem::new(Some("item1".to_string()), UpdateAction::Continue);
        let update_count = mock_item.update_count.clone();
        let tick_count = mock_item.tick_count.clone();

        let builder = MockBuilder::new("test_type", 0, None, mock_item).boxed();
        let root = LayoutItemBuilderNode::from(builder);

        let multi = MultiProgress::new();
        let mut pipe = ProgressBarPipe::new(multi, vec![root]);

        let obs = create_observation("test_type", Some("item1"), "running", false);
        pipe.observe(log::Level::Info, obs.clone());
        pipe.flush().await;

        assert_eq!(*update_count.lock().unwrap(), 0);
        assert_eq!(*tick_count.lock().unwrap(), 1);

        pipe.observe(log::Level::Info, obs);
        pipe.flush().await;

        assert_eq!(*update_count.lock().unwrap(), 1);
        assert_eq!(*tick_count.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn test_progress_bar_pipe_item_removal() {
        let mock_item = MockItem::new(Some("item1".to_string()), UpdateAction::FinishedRemove);
        let update_count = mock_item.update_count.clone();

        let builder = MockBuilder::new("test_type", 0, None, mock_item).boxed();
        let root = LayoutItemBuilderNode::from(builder);

        let multi = MultiProgress::new();
        let mut pipe = ProgressBarPipe::new(multi, vec![root]);

        let obs1 = create_observation("test_type", Some("item1"), "running", false);
        pipe.observe(log::Level::Info, obs1);

        let obs2 = create_observation("test_type", Some("item1"), "completed", true);
        pipe.observe(log::Level::Info, obs2);
        pipe.flush().await;

        assert_eq!(*update_count.lock().unwrap(), 1);

        let mock_item2 = MockItem::new(Some("item1".to_string()), UpdateAction::Continue);
        let builder2 = MockBuilder::new("test_type", 0, None, mock_item2).boxed();

        pipe.builders
            .insert(Key::Body("test_type".to_string()), Builder::Body(builder2));

        let obs3 = create_observation("test_type", Some("item1"), "running", false);
        pipe.observe(log::Level::Info, obs3);
    }
}
