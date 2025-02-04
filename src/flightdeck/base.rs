use crate::flightdeck::layout::{LayoutItem, LayoutItemBuilder, UpdateAction};
use crate::flightdeck::observation::{Data, Observation, Value};
use crate::flightdeck::observer::Observable;
use chrono::Utc;
use derive_builder::Builder;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;

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

    pub fn with_id(type_key: String, id: String) -> Self {
        Self {
            type_key,
            id: Some(id),
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

type StateTransformerFn = Box<dyn Fn(bool, Option<String>) -> String + Sync + Send>;

struct PGMessageManager {
    state_transformer: Arc<StateTransformerFn>,
    last_state: Option<String>,
    is_terminal: bool,
}

impl PGMessageManager {
    fn new(state_transformer: Arc<StateTransformerFn>) -> Self {
        Self {
            state_transformer,
            last_state: None,
            is_terminal: false,
        }
    }
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
        let state_transformer = self.state_transformer.clone();
        let message = state_transformer(self.is_terminal, self.last_state.clone());

        match self.is_terminal {
            false => pb.set_message(message),
            true => pb.finish_with_message(message),
        }
    }
}

#[derive(Clone)]
struct PGStyle {
    in_progress: ProgressStyle,
    done: ProgressStyle,
}

struct PGStyleManager {
    already_initialised: bool,
    is_terminal: bool,
    style: PGStyle,
}

impl PGStyleManager {
    fn new(style: PGStyle) -> Self {
        Self {
            already_initialised: false,
            is_terminal: false,
            style,
        }
    }
}

impl ProgressBarManager for PGStyleManager {
    fn observe(&mut self, _data: &[Data], is_terminal: bool) {
        if is_terminal && !self.is_terminal {
            self.is_terminal = true;
            self.already_initialised = false;
        }
    }

    fn update_progress_bar(&mut self, pb: &ProgressBar) {
        if !self.already_initialised {
            self.already_initialised = true;
            pb.set_style(match self.is_terminal {
                false => self.style.in_progress.clone(),
                true => self.style.done.clone(),
            });
        }
    }
}

type TerminationActionFn = Box<dyn Fn(Vec<Data>) -> TerminationActionIndicator + Send + Sync>;

#[derive(Debug)]
pub enum TerminationActionIndicator {
    Remove,
    Keep,
}

pub struct BaseLayoutItem {
    type_key: String,
    id: Option<String>,
    pb: Option<ProgressBar>,
    termination_action: Arc<TerminationActionFn>,
    managers: Vec<Box<dyn ProgressBarManager>>,
    depth: usize,
}

impl BaseLayoutItem {
    fn new(
        type_key: String,
        obs: Observation,
        termination_action: Arc<TerminationActionFn>,
        managers: Vec<Box<dyn ProgressBarManager>>,
        depth: usize,
    ) -> Self {
        Self {
            type_key,
            id: obs.id.clone(),
            pb: None,
            termination_action,
            depth,
            managers,
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
                TerminationActionIndicator::Remove => UpdateAction::FinishedRemove,
                TerminationActionIndicator::Keep => UpdateAction::FinishedKeep,
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

pub enum TerminationAction {
    Remove,
    Keep,
    Fn(TerminationActionFn),
}

impl TerminationAction {
    fn boxed(self) -> TerminationActionFn {
        match self {
            TerminationAction::Remove => Box::new(|_| TerminationActionIndicator::Remove),
            TerminationAction::Keep => Box::new(|_| TerminationActionIndicator::Keep),
            TerminationAction::Fn(f) => f,
        }
    }
}

pub enum StateTransformer {
    Identity,
    Static { msg: String, done: String },
    StateFn(StateTransformerFn),
}

impl StateTransformer {
    fn boxed(self) -> StateTransformerFn {
        match self {
            StateTransformer::Static { msg, done } => Box::new(move |d, _| match d {
                true => done.clone(),
                false => msg.clone(),
            }),
            StateTransformer::Identity => Box::new(|done, s| match (done, s) {
                (true, None) => "done".to_string(),
                (false, None) => "in progress".to_string(),
                (true, Some(s)) => s,
                (false, Some(s)) => s,
            }),
            StateTransformer::StateFn(f) => f,
        }
    }
}

pub enum Style {
    Default,
    Style(ProgressStyle),
    Template { in_progress: String, done: String },
}

impl From<Style> for PGStyle {
    fn from(val: Style) -> Self {
        match val {
            Style::Default => PGStyle {
                in_progress: ProgressStyle::with_template(
                    "{prefix}{spinner:.green} {msg} [{bar:20.cyan/blue}]",
                )
                .unwrap(),
                done: ProgressStyle::with_template("{prefix}  {msg} [{bar:20.cyan/blue}]").unwrap(),
            },
            Style::Style(style) => PGStyle {
                in_progress: style.clone(),
                done: style,
            },
            Style::Template { in_progress, done } => PGStyle {
                in_progress: ProgressStyle::with_template(in_progress.as_str()).unwrap(),
                done: ProgressStyle::with_template(done.as_str()).unwrap(),
            },
        }
    }
}

#[derive(Builder)]
#[builder(
    pattern = "owned",
    build_fn(error = "Box<dyn std::error::Error + Send + Sync>")
)]
pub struct BaseLayoutBuilder {
    #[builder(setter(into))]
    type_key: String,
    #[builder(default)]
    depth: usize,
    #[builder(default, setter(strip_option))]
    limit: Option<usize>,
    #[builder(
        setter(custom),
        default = "Arc::new(Box::new(TerminationAction::Keep.boxed()))"
    )]
    termination_action: Arc<TerminationActionFn>,
    #[builder(
        setter(custom),
        default = "Arc::new(Box::new(StateTransformer::Identity.boxed()))"
    )]
    state_transformer: Arc<StateTransformerFn>,
    #[builder(setter(into), default = "Style::Default.into()")]
    style: PGStyle,
}

impl BaseLayoutBuilder {
    pub fn boxed(self) -> Box<dyn LayoutItemBuilder> {
        Box::new(self)
    }
}

impl BaseLayoutBuilderBuilder {
    pub fn termination_action(self, termination_action: TerminationAction) -> Self {
        Self {
            type_key: self.type_key,
            depth: self.depth,
            limit: self.limit,
            termination_action: Some(Arc::new(termination_action.boxed())),
            state_transformer: self.state_transformer,
            style: self.style,
        }
    }
    pub fn state_transformer(self, state_transformer: StateTransformer) -> Self {
        Self {
            type_key: self.type_key,
            depth: self.depth,
            limit: self.limit,
            termination_action: self.termination_action,
            state_transformer: Some(Arc::new(state_transformer.boxed())),
            style: self.style,
        }
    }
}

impl LayoutItemBuilder for BaseLayoutBuilder {
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
        let managers: Vec<Box<dyn ProgressBarManager>> = vec![
            Box::new(PGStyleManager::new(self.style.clone())),
            Box::new(PGPositionManager::default()),
            Box::new(PGMessageManager::new(self.state_transformer.clone())),
        ];
        Box::new(BaseLayoutItem::new(
            self.type_key.clone(),
            obs.clone(),
            self.termination_action.clone(),
            managers,
            self.depth,
        ))
    }
}
