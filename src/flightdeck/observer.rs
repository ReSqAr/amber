use crate::flightdeck::global;
use crate::flightdeck::observation::Observation;

pub trait Observable {
    type Observation: Clone;

    // mutation
    fn update(&mut self, observation: Self::Observation) -> &Self;

    // getter
    fn generate_observation(&self) -> Observation;

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
    pub fn new(inner: T) -> Self {
        global::send(log::Level::Trace, inner.generate_observation());
        Self {
            inner,
            default_terminal_log: None,
        }
    }

    pub fn with_terminal_state(
        inner: T,
        level: log::Level,
        default_terminal_observation: T::Observation,
    ) -> Self {
        global::send(log::Level::Trace, inner.generate_observation());
        Self {
            inner,
            default_terminal_log: Some((level, default_terminal_observation)),
        }
    }

    pub fn observe(&mut self, level: log::Level, observation: T::Observation) {
        let observation = self.inner.update(observation).generate_observation();
        global::send(level, observation)
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
