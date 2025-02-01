use crate::flightdeck::global;
use crate::flightdeck::observation::Observation;

pub trait State {
    fn is_terminal(&self) -> bool;
}

pub trait Observable {
    type State: State + Clone;

    // mutation
    fn update(&mut self, s: Self::State, message: Option<String>) -> &Self;

    // getter
    fn generate_observation(&self) -> Observation;
    fn final_observation(&self) -> Observation;
    fn state(&self) -> &Self::State;
}

pub trait ObservableDefaultErrorState: Observable {
    // for internal management
    fn default_error_state(&self) -> Self::State;
}

pub struct Observer<T: Observable> {
    inner: T,
}

impl<T: Observable> Drop for Observer<T> {
    fn drop(&mut self) {
        if !self.inner.state().is_terminal() {
            global::send(log::Level::Debug, self.inner.final_observation())
        }
    }
}

impl<T: Observable> Observer<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn observe(&mut self, level: log::Level, state: T::State, message: Option<String>) {
        let observation = self.inner.update(state, message).generate_observation();
        global::send(level, observation)
    }
}

impl<T: ObservableDefaultErrorState> Observer<T> {
    pub fn error<E: std::error::Error>(&mut self, error: E) -> E {
        // TODO: don't like the return type - but what can one do?
        self.observe(
            log::Level::Error,
            self.inner.default_error_state(),
            Some(error.to_string()),
        );
        error // the idea is: this is used can cheaply hijack `?` by replacing it with: `.map_err(obs.error)?`
    }
}
