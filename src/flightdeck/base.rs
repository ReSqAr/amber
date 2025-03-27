use crate::flightdeck::layout::{LayoutItem, LayoutItemBuilder, UpdateAction};
use crate::flightdeck::observation::{Data, Observation, Value};
use crate::flightdeck::observer::{Observable, Observer};
use chrono::Utc;
use derive_builder::Builder;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
pub enum BaseObservation {
    State(String),
    StateWithData {
        state: String,
        data: HashMap<String, Value>,
    },
    TerminalState(String),
    TerminalStateWithData {
        state: String,
        data: HashMap<String, Value>,
    },
    Position(u64),
    Length(u64),
}

#[derive(Clone)]
pub struct BaseObservable {
    type_key: String,
    id: Option<String>,
    is_terminal: Arc<AtomicBool>,
}

impl BaseObservable {
    pub fn without_id(type_key: impl Into<String>) -> Self {
        Self {
            type_key: type_key.into(),
            id: None,
            is_terminal: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_id(type_key: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            type_key: type_key.into(),
            id: Some(id.into()),
            is_terminal: Arc::new(AtomicBool::new(false)),
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
            Some(BaseObservation::StateWithData { state, data }) => {
                let mut d = vec![Data {
                    key: "state".into(),
                    value: state.into(),
                }];
                d.extend(data.into_iter().map(Data::from));
                d
            }
            Some(BaseObservation::TerminalState(s)) => vec![Data {
                key: "state".into(),
                value: s.into(),
            }],
            Some(BaseObservation::TerminalStateWithData { state, data }) => {
                let mut d = vec![Data {
                    key: "state".into(),
                    value: state.into(),
                }];
                d.extend(data.into_iter().map(Data::from));
                d
            }
            Some(BaseObservation::Position(p)) => vec![Data {
                key: "position".into(),
                value: p.into(),
            }],
            Some(BaseObservation::Length(l)) => vec![Data {
                key: "length".into(),
                value: l.into(),
            }],
        };

        let is_terminal = self.is_terminal.load(Ordering::Relaxed)
            | match observation {
                None => false,
                Some(BaseObservation::State(_)) => false,
                Some(BaseObservation::StateWithData { .. }) => false,
                Some(BaseObservation::TerminalState(_)) => true,
                Some(BaseObservation::TerminalStateWithData { .. }) => true,
                Some(BaseObservation::Position(_)) => false,
                Some(BaseObservation::Length(_)) => false,
            };
        if is_terminal {
            self.is_terminal.store(is_terminal, Ordering::Relaxed);
        }

        Observation {
            type_key: self.type_key.clone(),
            id: self.id.clone(),
            timestamp: Utc::now(),
            is_terminal,
            data,
        }
    }
    fn is_in_terminal_state(&self) -> bool {
        self.is_terminal.load(Ordering::Relaxed)
    }
}

pub type BaseObserver = Observer<BaseObservable>;

impl Observer<BaseObservable> {
    pub fn without_id(type_key: impl Into<String>) -> Self {
        Self::new(BaseObservable::without_id(type_key))
    }

    pub fn with_id(type_key: impl Into<String>, id: impl Into<String>) -> Self {
        Self::new(BaseObservable::with_id(type_key, id))
    }

    pub fn observe_state(&mut self, level: log::Level, s: impl Into<String>) -> &mut Self {
        self.observe(level, BaseObservation::State(s.into()))
    }
    pub fn observe_state_ext<T: Into<Value>>(
        &mut self,
        level: log::Level,
        s: impl Into<String>,
        data: impl Into<HashMap<String, T>>,
    ) -> &mut Self {
        self.observe(
            level,
            BaseObservation::StateWithData {
                state: s.into(),
                data: data
                    .into()
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            },
        )
    }
    pub fn observe_termination(&mut self, level: log::Level, s: impl Into<String>) -> &mut Self {
        self.observe(level, BaseObservation::TerminalState(s.into()))
    }
    pub fn observe_termination_ext<T: Into<Value>>(
        &mut self,
        level: log::Level,
        s: impl Into<String>,
        data: impl Into<HashMap<String, T>>,
    ) -> &mut Self {
        self.observe(
            level,
            BaseObservation::TerminalStateWithData {
                state: s.into(),
                data: data
                    .into()
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            },
        )
    }
    pub fn observe_length(&mut self, level: log::Level, pos: u64) -> &mut Self {
        self.observe(level, BaseObservation::Length(pos))
    }
    pub fn observe_position(&mut self, level: log::Level, pos: u64) -> &mut Self {
        self.observe(level, BaseObservation::Position(pos))
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
                Value::Bool(_) => continue,
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

type IdStateTransformerFn =
    Box<dyn Fn(bool, Option<String>, Option<String>) -> String + Sync + Send>;
type IdTransformerFn = Box<dyn Fn(bool, Option<String>) -> String + Sync + Send>;
type StateTransformerFn = Box<dyn Fn(bool, Option<String>) -> String + Sync + Send>;

struct PGMessageManager {
    id: Option<String>,
    state_transformer: Arc<IdStateTransformerFn>,
    last_state: Option<String>,
    is_terminal: bool,
}

impl PGMessageManager {
    fn new(id: Option<String>, state_transformer: Arc<IdStateTransformerFn>) -> Self {
        Self {
            id,
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
                Value::Bool(_) => continue,
            };
            if d.key == "state" {
                self.last_state = Some(value.clone())
            }
        }
    }

    fn update_progress_bar(&mut self, pb: &ProgressBar) {
        let state_transformer = self.state_transformer.clone();
        let message = state_transformer(self.is_terminal, self.id.clone(), self.last_state.clone());

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
    id: Option<String>,
    pb: Option<ProgressBar>,
    termination_action: Arc<TerminationActionFn>,
    managers: Vec<Box<dyn ProgressBarManager>>,
    depth: usize,
}

impl BaseLayoutItem {
    fn new(
        obs: Observation,
        termination_action: Arc<TerminationActionFn>,
        managers: Vec<Box<dyn ProgressBarManager>>,
        depth: usize,
    ) -> Self {
        Self {
            id: obs.id.clone(),
            pb: None,
            termination_action,
            depth,
            managers,
        }
    }
}

pub(crate) fn prefix_from_depth(depth: usize) -> String {
    " ".repeat(2 * depth)
}

impl BaseLayoutItem {
    fn update_progress_bar(&mut self, pb: &ProgressBar) {
        for manager in &mut self.managers {
            manager.update_progress_bar(pb);
        }
    }
}

impl LayoutItem for BaseLayoutItem {
    fn id(&self) -> Option<String> {
        self.id.clone()
    }
    fn update(&mut self, obs: &Observation) -> UpdateAction {
        for manager in &mut self.managers {
            manager.observe(&obs.data, obs.is_terminal);
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

    fn tick(&mut self) {
        for manager in &mut self.managers {
            if let Some(pb) = &self.pb {
                manager.update_progress_bar(pb);
                pb.tick();
            }
        }
    }

    fn set_bar(&mut self, pb: ProgressBar) {
        pb.set_prefix(prefix_from_depth(self.depth));
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
    #[allow(dead_code)] // TODO
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
    IdStateFn(IdStateTransformerFn),
    IdFn(IdTransformerFn),
}

impl StateTransformer {
    fn boxed(self) -> IdStateTransformerFn {
        match self {
            StateTransformer::Static { msg, done } => Box::new(move |d, _, _| match d {
                true => done.clone(),
                false => msg.clone(),
            }),
            StateTransformer::Identity => Box::new(|done, _, s| match (done, s) {
                (true, None) => "done".to_string(),
                (false, None) => "in progress".to_string(),
                (_, Some(s)) => s,
            }),
            StateTransformer::StateFn(f) => Box::new(move |done, _, s| f(done, s)),
            StateTransformer::IdStateFn(f) => f,
            StateTransformer::IdFn(f) => Box::new(move |done, id, _| f(done, id)),
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub enum Style {
    Default,
    #[allow(dead_code)] // TODO
    Raw {
        in_progress: ProgressStyle,
        done: ProgressStyle,
    },
    Template {
        in_progress: String,
        done: String,
    },
}

impl From<Style> for PGStyle {
    fn from(val: Style) -> Self {
        match val {
            Style::Default => PGStyle {
                in_progress: ProgressStyle::with_template(
                    "{prefix}{spinner:.green} {msg} [{bar:20.cyan/blue}]",
                )
                .unwrap(),
                done: ProgressStyle::with_template("{prefix} {msg} [{bar:20.cyan/blue}]").unwrap(),
            },
            Style::Raw { in_progress, done } => PGStyle { in_progress, done },
            Style::Template { in_progress, done } => PGStyle {
                in_progress: ProgressStyle::with_template(in_progress.as_str()).unwrap(),
                done: ProgressStyle::with_template(done.as_str()).unwrap(),
            },
        }
    }
}

#[derive(Builder)]
#[builder(pattern = "owned", build_fn(error = "std::convert::Infallible"))]
pub struct BaseLayoutBuilder {
    #[builder(default, setter(into))]
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
    state_transformer: Arc<IdStateTransformerFn>,
    #[builder(setter(into), default = "Style::Default.into()")]
    style: PGStyle,
}

impl BaseLayoutBuilder {
    pub fn boxed(self) -> Box<dyn LayoutItemBuilder> {
        Box::new(self)
    }
}

impl BaseLayoutBuilderBuilder {
    pub fn infallible_build(self) -> BaseLayoutBuilder {
        let Ok(build) = self.build();
        build
    }

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
            Box::new(PGMessageManager::new(
                obs.id.clone(),
                self.state_transformer.clone(),
            )),
        ];
        Box::new(BaseLayoutItem::new(
            obs.clone(),
            self.termination_action.clone(),
            managers,
            self.depth,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::flightdeck::base::{
        BaseLayoutBuilderBuilder, BaseObservable, BaseObservation, BaseObserver, StateTransformer,
        Style, TerminationAction,
    };
    use crate::flightdeck::layout::{LayoutItemBuilder, UpdateAction};
    use crate::flightdeck::observation::{Data, Observation, Value};
    use crate::flightdeck::observer::Observable;
    use indicatif::ProgressBar;
    use std::collections::HashMap;

    #[test]
    fn test_base_observable() {
        let mut observable = BaseObservable::with_id("task", "123");

        let initial_obs = observable.generate_observation(None);
        assert_eq!(initial_obs.type_key, "task");
        assert_eq!(initial_obs.id, Some("123".to_string()));
        assert!(!initial_obs.is_terminal);
        assert_eq!(initial_obs.data.len(), 0);

        let state_obs =
            observable.generate_observation(Some(BaseObservation::State("running".into())));
        assert!(!state_obs.is_terminal);
        assert_eq!(state_obs.data.len(), 1);
        assert_eq!(state_obs.data[0].key, "state");
        assert!(matches!(state_obs.data[0].value, Value::String(ref s) if s == "running"));

        let mut data = HashMap::new();
        data.insert("progress".to_string(), 75u64.into());
        let state_data_obs =
            observable.generate_observation(Some(BaseObservation::StateWithData {
                state: "processing".into(),
                data,
            }));
        assert_eq!(state_data_obs.data.len(), 2);

        let terminal_obs = observable
            .generate_observation(Some(BaseObservation::TerminalState("finished".into())));
        assert!(terminal_obs.is_terminal);
        assert!(observable.is_in_terminal_state());
    }

    #[test]
    fn test_base_observer() {
        let mut observer = BaseObserver::with_id("task", "123");

        observer.observe_state(log::Level::Info, "starting");

        let mut data = HashMap::new();
        data.insert("progress".to_string(), 50u64);
        observer.observe_state_ext(log::Level::Info, "in_progress", data);

        observer.observe_position(log::Level::Debug, 500);
        observer.observe_length(log::Level::Debug, 1000);

        observer.observe_termination(log::Level::Info, "done");
    }

    #[test]
    fn test_base_layout_builder() {
        let builder = BaseLayoutBuilderBuilder::default()
            .type_key("task")
            .depth(1)
            .limit(10)
            .termination_action(TerminationAction::Remove)
            .state_transformer(StateTransformer::Identity)
            .style(Style::Default)
            .infallible_build();

        assert_eq!(builder.type_key(), "task");
        assert_eq!(builder.visible_limit(), Some(10));

        let observation = Observation {
            type_key: "task".to_string(),
            id: Some("123".to_string()),
            timestamp: chrono::Utc::now(),
            is_terminal: false,
            data: vec![Data {
                key: "state".to_string(),
                value: Value::String("running".to_string()),
            }],
        };

        let mut item = builder.build_item(&observation);

        assert_eq!(item.id(), Some("123".to_string()));

        let update_result = item.update(&observation);
        assert!(matches!(update_result, UpdateAction::Continue));

        let terminal_observation = Observation {
            type_key: "task".to_string(),
            id: Some("123".to_string()),
            timestamp: chrono::Utc::now(),
            is_terminal: true,
            data: vec![Data {
                key: "state".to_string(),
                value: Value::String("completed".to_string()),
            }],
        };

        let update_result = item.update(&terminal_observation);
        assert!(matches!(update_result, UpdateAction::FinishedRemove));

        // Test setting a progress bar
        let pb = ProgressBar::new(100);
        item.set_bar(pb);
        assert!(item.get_bar().is_some());
    }

    #[test]
    fn test_state_transformer() {
        let identity = StateTransformer::Identity.boxed();
        assert_eq!(identity(false, None, None), "in progress");
        assert_eq!(identity(true, None, None), "done");
        assert_eq!(
            identity(false, None, Some("running".to_string())),
            "running"
        );

        let static_transformer = StateTransformer::Static {
            msg: "working...".to_string(),
            done: "finished!".to_string(),
        }
        .boxed();

        assert_eq!(static_transformer(false, None, None), "working...");
        assert_eq!(static_transformer(true, None, None), "finished!");

        let custom = StateTransformer::StateFn(Box::new(|done, state| match (done, state) {
            (true, _) => "COMPLETE".to_string(),
            (false, Some(s)) => format!("RUNNING: {}", s),
            (false, None) => "STARTING".to_string(),
        }))
        .boxed();

        assert_eq!(custom(false, None, None), "STARTING");
        assert_eq!(
            custom(false, None, Some("step 1".to_string())),
            "RUNNING: step 1"
        );
        assert_eq!(custom(true, None, Some("step 2".to_string())), "COMPLETE");
    }
}
