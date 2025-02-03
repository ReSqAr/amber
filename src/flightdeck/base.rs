use crate::flightdeck::layout::{LayoutItem, LayoutItemBuilder, UpdateAction};
use crate::flightdeck::observation::{Data, Observation, Value};
use crate::flightdeck::observer::Observable;
use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use std::marker::PhantomData;

#[derive(Clone)]
pub enum BaseObservation {
    State(String),
    TerminalState(String),
    Position(u64),
    Length(u64),
}

#[derive(Clone)]
pub struct BaseObservable {
    type_key: String,
    id: Option<String>,
    is_terminal: bool,
}

impl BaseObservable {
    pub fn without_id(type_key: String) -> Self {
        Self {
            type_key,
            id: None,
            is_terminal: false,
        }
    }

    pub fn with_id(type_key: String, id: Option<String>) -> Self {
        Self {
            type_key,
            id,
            is_terminal: false,
        }
    }
}

impl Observable for BaseObservable {
    type Observation = BaseObservation;
    fn generate_observation(&mut self, observation: Option<Self::Observation>) -> Observation {
        let data = match observation.clone() {
            None => vec![],
            Some(BaseObservation::State(s)) => vec![Data {
                key: "state".into(),
                value: s.into(),
            }],
            Some(BaseObservation::TerminalState(s)) => vec![Data {
                key: "state".into(),
                value: s.into(),
            }],
            Some(BaseObservation::Position(p)) => vec![Data {
                key: "position".into(),
                value: p.into(),
            }],
            Some(BaseObservation::Length(l)) => vec![Data {
                key: "length".into(),
                value: l.into(),
            }],
        };

        self.is_terminal |= match observation {
            None => false,
            Some(BaseObservation::State(_)) => false,
            Some(BaseObservation::TerminalState(_)) => true,
            Some(BaseObservation::Position(_)) => false,
            Some(BaseObservation::Length(_)) => false,
        };

        Observation {
            type_key: self.type_key.clone(),
            id: self.id.clone(),
            timestamp: Utc::now(),
            is_terminal: self.is_terminal,
            data,
        }
    }
    fn is_in_terminal_state(&self) -> bool {
        self.is_terminal
    }
}

trait ProgressBarManager: Send + Sync {
    fn observe(&mut self, data: &[Data], is_terminal: bool);
    fn update_progress_bar(&mut self, pb: &ProgressBar);
}

#[derive(Debug, Default)]
struct PGPositionManager {
    last_position: Option<u64>,
    last_length: Option<u64>,
}

impl ProgressBarManager for PGPositionManager {
    fn observe(&mut self, data: &[Data], _is_terminal: bool) {
        for d in data {
            let value = match d.value {
                Value::String(_) => continue,
                Value::U64(value) => value,
            };
            match d.key.as_str() {
                "position" => self.last_position = Some(value),
                "length" => self.last_length = Some(value),
                &_ => {}
            }
        }
    }

    fn update_progress_bar(&mut self, pb: &ProgressBar) {
        if let Some(position) = self.last_position {
            pb.set_position(position);
        }
        if let Some(length) = self.last_length {
            pb.set_length(length);
        }
    }
}
#[derive(Debug, Default)]
struct PGMessageManager {
    last_state: Option<String>,
    is_terminal: bool,
}

impl ProgressBarManager for PGMessageManager {
    fn observe(&mut self, data: &[Data], is_terminal: bool) {
        self.is_terminal |= is_terminal;
        for d in data {
            let value = match &d.value {
                Value::String(value) => value,
                Value::U64(_) => continue,
            };
            if d.key == "state" {
                self.last_state = Some(value.clone())
            }
        }
    }

    fn update_progress_bar(&mut self, pb: &ProgressBar) {
        match self.is_terminal {
            false => {
                if let Some(state) = &self.last_state {
                    pb.set_message(state.clone())
                }
            }
            true => {
                if let Some(state) = &self.last_state {
                    pb.finish_with_message(state.clone())
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct PGStyleManager {
    already_initialised: bool,
    is_terminal: bool,
}

impl ProgressBarManager for PGStyleManager {
    fn observe(&mut self, _data: &[Data], is_terminal: bool) {
        self.is_terminal |= is_terminal;
    }

    fn update_progress_bar(&mut self, pb: &ProgressBar) {
        if !self.already_initialised {
            self.already_initialised = true;
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{prefix}{spinner:.green} {msg} [{bar:20.cyan/blue}] {pos}/{len}")
                    .unwrap(),
            );
        }
    }
}

pub trait BaseLikeLayoutItem: LayoutItem + 'static {
    fn new(type_key: String, obs: Observation, depth: usize) -> Self;
}

#[derive(Debug)]
pub enum TerminationAction {
    Remove,
    Keep,
}

pub struct BaseLayoutItem {
    type_key: String,
    id: Option<String>,
    pb: Option<ProgressBar>,
    termination_action: Box<dyn Fn(Vec<Data>) -> TerminationAction + Send + Sync>,
    depth: usize,
    managers: Vec<Box<dyn ProgressBarManager>>,
}

impl BaseLikeLayoutItem for BaseLayoutItem {
    fn new(type_key: String, obs: Observation, depth: usize) -> Self {
        // TODO: add state transformer + termination_action
        Self {
            type_key,
            id: obs.id.clone(),
            pb: None,
            termination_action: Box::new(|_| TerminationAction::Keep),
            depth,
            managers: vec![
                Box::new(PGPositionManager::default()),
                Box::new(PGMessageManager::default()),
                Box::new(PGStyleManager::default()),
            ],
        }
    }
}

impl BaseLayoutItem {
    fn update_progress_bar(&mut self, pb: &ProgressBar) {
        for manager in &mut self.managers {
            manager.update_progress_bar(pb);
        }
    }
}

impl LayoutItem for BaseLayoutItem {
    fn type_key(&self) -> &str {
        self.type_key.as_str()
    }
    fn id(&self) -> Option<String> {
        self.id.clone()
    }
    fn update(&mut self, obs: &Observation) -> UpdateAction {
        for manager in &mut self.managers {
            manager.observe(&obs.data, obs.is_terminal);
            if let Some(pb) = &self.pb {
                manager.update_progress_bar(pb);
            }
        }

        if obs.is_terminal {
            let termination_action = &self.termination_action;
            match termination_action(obs.data.clone()) {
                TerminationAction::Remove => UpdateAction::FinishedRemove,
                TerminationAction::Keep => UpdateAction::FinishedKeep,
            }
        } else {
            UpdateAction::Continue
        }
    }

    fn set_bar(&mut self, pb: ProgressBar) {
        pb.set_prefix(" ".repeat(2 * self.depth));
        self.update_progress_bar(&pb);
        self.pb = Some(pb);
    }

    fn get_bar(&self) -> Option<&ProgressBar> {
        self.pb.as_ref()
    }
}

pub struct BaseLayoutBuilder<L: BaseLikeLayoutItem> {
    type_key: String,
    depth: usize,
    limit: Option<usize>,
    children: Box<dyn Fn() -> Vec<Box<dyn LayoutItemBuilder>> + Send + Sync>,
    phantom: PhantomData<L>,
}

impl<L: BaseLikeLayoutItem> BaseLayoutBuilder<L> {
    pub fn new(type_key: String) -> Self {
        Self::with(type_key, None, Box::new(Vec::new))
    }

    pub fn with_limit(type_key: String, limit: usize) -> Self {
        Self::with(type_key, Some(limit), Box::new(Vec::new))
    }

    pub fn with_children(
        type_key: String,
        children: Box<dyn Fn() -> Vec<Box<dyn LayoutItemBuilder>> + Send + Sync>,
    ) -> Self {
        Self::with(type_key, None, children)
    }

    pub fn with(
        // TODO: add state transformer + termination_action
        type_key: String,
        limit: Option<usize>,
        children: Box<dyn Fn() -> Vec<Box<dyn LayoutItemBuilder>> + Send + Sync>,
    ) -> Self {
        Self {
            type_key,
            depth: 0,
            limit,
            children,
            phantom: PhantomData,
        }
    }
}

impl<L: BaseLikeLayoutItem> LayoutItemBuilder for BaseLayoutBuilder<L> {
    fn type_key(&self) -> &str {
        self.type_key.as_str()
    }
    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }
    fn visible_limit(&self) -> Option<usize> {
        self.limit
    }
    fn build_item(&self, obs: &Observation) -> Box<dyn LayoutItem> {
        Box::new(L::new(self.type_key.clone(), obs.clone(), self.depth))
    }

    fn children(&self) -> Vec<Box<dyn LayoutItemBuilder>> {
        let children = &self.children;
        children()
    }
}
