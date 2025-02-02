use crate::flightdeck::global;
use crate::flightdeck::observation::Observation;
use std::fmt::Display;

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
}

pub struct Observer<T: Observable> {
    inner: T,
    default_terminal_state: Option<T::State>,
}

impl<T: Observable> Drop for Observer<T> {
    fn drop(&mut self) {
        if let Some(default_terminal_state) = &self.default_terminal_state {
            if !self.inner.state().is_terminal() {
                self.observe(
                    log::Level::Trace,
                    Some(default_terminal_state.clone()),
                    None,
                );
            }
        }
    }
}

impl<T: Observable> Observer<T> {
    pub fn new(inner: T) -> Self {
        global::send(log::Level::Trace, inner.generate_observation());
        Self {
            inner,
            default_terminal_state: None,
        }
    }

    pub fn with_terminal_state(inner: T, default_terminal_state: T::State) -> Self {
        global::send(log::Level::Trace, inner.generate_observation());
        Self {
            inner,
            default_terminal_state: Some(default_terminal_state),
        }
    }

    pub fn observe(&mut self, level: log::Level, state: Option<T::State>, message: Option<String>) {
        let observation = self.inner.update(state, message).generate_observation();
        global::send(level, observation)
    }

    pub fn observe_error<E: Display>(
        &mut self,
        error_state: T::State,
    ) -> impl FnOnce(E) -> E + use<'_, E, T> {
        |e| {
            self.observe(log::Level::Error, Some(error_state), Some(e.to_string()));
            e // the idea is: this is used can cheaply hijack `?` by replacing it with: `.map_err(obs.observe_error(State::Error))?`
        }
    }
}
