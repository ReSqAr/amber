use crate::flightdeck::observation::Observation;

/// Command pattern returned by a LayoutItem's update function.
#[derive(Debug)]
pub enum UpdateAction {
    /// The item is still ongoing.
    Continue,
    /// The item is finished and does not have a final message => remove it from the manager.
    /// Returns the associated ProgressBar so that the manager can remove it from MultiProgress.
    FinishedRemove,
    /// The item is finished but the progress bar should remain visible (e.g. final message).
    FinishedKeep,
}

/// ============ 2. LayoutItem & LayoutItemBuilder Traits ============

/// A single displayable item, e.g. a task or sub-task.
pub trait LayoutItem {
    /// Return the type key (e.g. "download", "upload").
    fn type_key(&self) -> &str;
    /// Return the optional ID, if any.
    fn id(&self) -> Option<String>;

    /// Update internal state from the observation, returning how the manager should proceed.
    fn update(&mut self, obs: &Observation) -> UpdateAction;

    /// Set the current `ProgressBar`.
    fn set_bar(&mut self, bar: indicatif::ProgressBar);

    /// Return the current `ProgressBar`, if any.
    fn get_bar(&self) -> Option<&indicatif::ProgressBar>;
}

/// A builder that can create LayoutItems of a particular `type_key`.
/// It also has a `visible_limit`, controlling how many items of this type can show at once.
pub trait LayoutItemBuilder {
    /// The type key (e.g. "download") used for routing.
    fn type_key(&self) -> &str;

    fn set_depth(&mut self, depth: usize);

    /// The maximum number of visible items, or None if unlimited.
    fn visible_limit(&self) -> Option<usize>;

    /// Build a new item from an observation if no matching item was found.
    fn build_item(&self, obs: &Observation) -> Box<dyn LayoutItem + Send + Sync>;

    /// Child builders, for a depth-first builder structure.
    fn children(&self) -> Vec<Box<dyn LayoutItemBuilder + Send + Sync>> {
        Vec::new()
    }
}
