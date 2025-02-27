use crate::flightdeck::observation::Observation;

#[derive(Clone, Debug)]
pub enum UpdateAction {
    Continue,
    FinishedRemove,
    FinishedKeep,
}

pub trait LayoutItem: Send + Sync {
    fn id(&self) -> Option<String>;
    fn update(&mut self, obs: &Observation) -> UpdateAction;
    fn tick(&mut self);
    fn set_bar(&mut self, bar: indicatif::ProgressBar);
    fn get_bar(&self) -> Option<&indicatif::ProgressBar>;
}

pub trait LayoutItemBuilder: Send + Sync {
    fn type_key(&self) -> &str;
    fn set_depth(&mut self, depth: usize);
    fn visible_limit(&self) -> Option<usize>;
    fn build_item(&self, obs: &Observation) -> Box<dyn LayoutItem>;
}
