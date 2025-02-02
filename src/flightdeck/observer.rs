use crate::flightdeck::global;
use crate::flightdeck::observation::Observation;

pub trait State {
    fn is_terminal(&self) -> bool;
}

pub trait Observable {
    type State: State + Clone;

    // mutation
    fn update(&mut self, state: Option<Self::State>, message: Option<String>) -> &Self;

    // getter
    fn generate_observation(&self) -> Observation;
    fn state(&self) -> &Self::State;

    // config
    fn default_terminal_state(&self) -> Option<Self::State> {
        None
    }
    fn default_error_state(&self) -> Option<Self::State> {
        None
    }
}

pub struct Observer<T: Observable> {
    inner: T,
}

impl<T: Observable> Drop for Observer<T> {
    fn drop(&mut self) {
        if !self.inner.state().is_terminal() && self.inner.default_terminal_state().is_some() {
            self.observe(log::Level::Trace, self.inner.default_terminal_state(), None);
        }
    }
}

impl<T: Observable> Observer<T> {
    pub fn new(inner: T) -> Self {
        global::send(log::Level::Trace, inner.generate_observation());
        Self { inner }
    }

    pub fn observe(&mut self, level: log::Level, state: Option<T::State>, message: Option<String>) {
        let observation = self.inner.update(state, message).generate_observation();
        global::send(level, observation)
    }

    pub fn error<E: std::error::Error>(&mut self, error: E) -> E {
        self.observe(
            log::Level::Error,
            self.inner.default_error_state(),
            Some(error.to_string()),
        );
        error // the idea is: this is used can cheaply hijack `?` by replacing it with: `.map_err(obs.error)?`
    }
}
