use crate::flightdeck::global;
use crate::flightdeck::observation::Observation;

pub trait Observable: Send + Sync {
    type Observation: Clone;

    // mutation
    fn generate_observation(&mut self, observation: Option<Self::Observation>) -> Observation;

    fn is_in_terminal_state(&self) -> bool;
}

pub struct Observer<T: Observable> {
    inner: T,
    default_terminal_log: Option<(log::Level, T::Observation)>,
}

impl<T: Observable> Drop for Observer<T> {
    fn drop(&mut self) {
        if let Some((level, observation)) = self.default_terminal_log.clone() {
            if !self.inner.is_in_terminal_state() {
                self.observe(level, observation);
            }
        }
    }
}

impl<T: Observable> Observer<T> {
    pub(crate) fn with(
        mut inner: T,
        default_terminal_log: Option<(log::Level, T::Observation)>,
    ) -> Self {
        global::send(log::Level::Trace, inner.generate_observation(None));
        Self {
            inner,
            default_terminal_log,
        }
    }

    pub fn new(inner: T) -> Self {
        Self::with(inner, None)
    }

    pub fn with_auto_termination(
        inner: T,
        level: log::Level,
        default_terminal_observation: T::Observation,
    ) -> Self {
        Self::with(inner, Some((level, default_terminal_observation)))
    }

    pub fn observe(&mut self, level: log::Level, observation: T::Observation) -> &mut Self {
        let observation = self.inner.generate_observation(Some(observation));
        global::send(level, observation);
        self
    }

    pub fn observe_error<E, U>(&mut self, map: U) -> impl FnOnce(E) -> E + use<'_, E, T, U>
    where
        U: FnOnce(&E) -> T::Observation,
    {
        |e| {
            self.observe(log::Level::Error, map(&e));
            e // the idea is: this is used can cheaply hijack `?` by replacing it with: `.map_err(obs.observe_error(|e| State::Error(e)))?`
        }
    }
}
