use crate::flightdeck::global;
use crate::flightdeck::observation::Observation;

pub trait Observable: Clone + Send + Sync {
    type Observation: Clone;

    // mutation
    fn generate_observation(&mut self, observation: Option<Self::Observation>) -> Observation;

    fn is_in_terminal_state(&self) -> bool;
}

#[derive(Clone)]
pub struct Observer<T: Observable> {
    inner: T,
    default_termination_observation: Option<(log::Level, T::Observation)>,
}

impl<T: Observable> Drop for Observer<T> {
    fn drop(&mut self) {
        if let Some((level, observation)) = self.default_termination_observation.clone() {
            if !self.inner.is_in_terminal_state() {
                self.observe(level, observation);
            }
        }
    }
}

impl<T: Observable> Observer<T> {
    pub(crate) fn with(
        mut inner: T,
        default_termination_observation: Option<(log::Level, T::Observation)>,
    ) -> Self {
        global::send(log::Level::Trace, inner.generate_observation(None));
        Self {
            inner,
            default_termination_observation,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flightdeck::observation::{self, Observation};
    use log::Level;

    // A minimal Observable implementation that records observations
    #[derive(Clone, Debug)]
    struct MockObservable {
        state: String,
        is_terminal: bool,
        observations: Vec<String>,
    }

    impl MockObservable {
        fn new(state: &str) -> Self {
            Self {
                state: state.to_string(),
                is_terminal: false,
                observations: Vec::new(),
            }
        }

        fn set_terminal(&mut self, is_terminal: bool) {
            self.is_terminal = is_terminal;
        }
    }

    impl Observable for MockObservable {
        type Observation = String;

        fn generate_observation(&mut self, observation: Option<Self::Observation>) -> Observation {
            let msg = if let Some(obs) = observation {
                self.observations.push(obs.clone());
                format!("{}:{}", self.state, obs)
            } else {
                self.state.to_string()
            };

            Observation {
                type_key: "".to_string(),
                id: None,
                timestamp: Default::default(),
                is_terminal: false,
                data: vec![observation::Data {
                    key: "msg".into(),
                    value: observation::Value::String(msg),
                }],
            }
        }

        fn is_in_terminal_state(&self) -> bool {
            self.is_terminal
        }
    }

    #[test]
    fn test_with_auto_termination() {
        // Create observable
        let observable = MockObservable::new("test_state");

        {
            // Create observer with auto-termination
            let _observer = Observer::with_auto_termination(
                observable,
                Level::Warn,
                "auto_terminated".to_string(),
            );

            // Observer is dropped here, should call observe with auto_terminated
        }

        // No direct assertions - this is mostly a compile/crash test
        // The actual behavior would require mocking global::send
    }

    #[test]
    fn test_with_auto_termination_already_terminal() {
        // Create observable that's already in terminal state
        let mut observable = MockObservable::new("terminal_state");
        observable.set_terminal(true);

        {
            // Create observer with auto-termination
            let _observer = Observer::with_auto_termination(
                observable,
                Level::Warn,
                "should_not_log".to_string(),
            );

            // Observer is dropped here, but since is_terminal is true,
            // no observation should be generated
        }

        // No direct assertions - this is mostly a compile/crash test
        // The actual behavior would require mocking global::send
    }

    #[test]
    fn test_observe() {
        // Create observable
        let observable = MockObservable::new("initial");
        let mut observer = Observer::new(observable);

        // Test chaining works
        let chain_result = observer
            .observe(Level::Debug, "first".to_string())
            .observe(Level::Error, "second".to_string());

        // Verify we get the observer back for chaining
        assert!(std::ptr::eq(chain_result, &observer));

        // Check the observations were recorded in the inner observable
        assert_eq!(observer.inner.observations.len(), 2);
        assert_eq!(observer.inner.observations[0], "first");
        assert_eq!(observer.inner.observations[1], "second");
    }

    #[test]
    fn test_observe_error() {
        // Define error type
        #[allow(dead_code)]
        #[derive(Debug)]
        struct TestError(&'static str);

        // Function that returns error
        fn operation_that_fails() -> Result<(), TestError> {
            Err(TestError("something went wrong"))
        }

        // Create observable and observer
        let observable = MockObservable::new("error_test");
        let mut observer = Observer::new(observable);

        // Use observe_error
        let handler = observer.observe_error(|e: &TestError| format!("Error occurred: {:?}", e));
        let result = operation_that_fails().map_err(handler);

        // Verify error was propagated
        assert!(result.is_err());

        // Check the observation was recorded in the inner observable
        assert_eq!(observer.inner.observations.len(), 1);
        assert!(observer.inner.observations[0].contains("Error occurred"));
        assert!(observer.inner.observations[0].contains("something went wrong"));
    }

    #[test]
    fn test_observe_error_with_result_operator() {
        // Define error
        #[derive(Debug, PartialEq)]
        struct CustomError(&'static str);

        // Function that returns error
        fn fallible_function() -> Result<(), CustomError> {
            Err(CustomError("operation failed"))
        }

        // Create observable and observer
        let observable = MockObservable::new("error_test");
        let mut observer = Observer::new(observable);

        // Function that uses ? operator
        fn process(obs: &mut Observer<MockObservable>) -> Result<(), CustomError> {
            let handler = obs.observe_error(|e: &CustomError| format!("Failed: {:?}", e));
            fallible_function().map_err(handler)?;
            Ok(())
        }

        // Call function
        let result = process(&mut observer);

        // Verify error was propagated
        assert_eq!(result, Err(CustomError("operation failed")));

        // Check the observation was recorded in the inner observable
        assert_eq!(observer.inner.observations.len(), 1);
        assert!(observer.inner.observations[0].contains("Failed"));
        assert!(observer.inner.observations[0].contains("operation failed"));
    }
}
