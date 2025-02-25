/// ============ 1. Observation & UpdateAction ============

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    U64(u64),
    Bool(bool),
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::U64(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

#[derive(Debug, Clone)]
pub struct Data {
    pub key: String,
    pub value: Value,
}

impl From<(String, Value)> for Data {
    fn from((key, value): (String, Value)) -> Self {
        Self { key, value }
    }
}

#[derive(Debug, Clone)]
pub struct Observation {
    pub type_key: String,
    pub id: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub is_terminal: bool,
    pub data: Vec<Data>,
}

#[derive(Debug, Clone)]
pub(crate) struct Message {
    pub(crate) level: log::Level,
    pub(crate) observation: Observation,
}

#[cfg(test)]
mod tests {
    use crate::flightdeck::observation::{Data, Observation, Value};
    use crate::flightdeck::observer::{Observable, Observer};
    use chrono::Utc;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    enum TestObservation {
        Started,
        Progress(u64),
        Completed,
    }

    struct TestObservable {
        type_key: String,
        id: Option<String>,
        is_terminal: bool,
        observations: Arc<Mutex<Vec<Observation>>>,
    }

    impl TestObservable {
        fn new(
            type_key: impl Into<String>,
            id: Option<String>,
            observations: Arc<Mutex<Vec<Observation>>>,
        ) -> Self {
            Self {
                type_key: type_key.into(),
                id,
                is_terminal: false,
                observations,
            }
        }
    }

    impl Observable for TestObservable {
        type Observation = TestObservation;

        fn generate_observation(&mut self, observation: Option<Self::Observation>) -> Observation {
            let data = match observation {
                None => vec![],
                Some(TestObservation::Started) => vec![Data {
                    key: "state".into(),
                    value: "started".to_string().into(),
                }],
                Some(TestObservation::Progress(p)) => vec![
                    Data {
                        key: "state".into(),
                        value: "in_progress".to_string().into(),
                    },
                    Data {
                        key: "progress".into(),
                        value: p.into(),
                    },
                ],
                Some(TestObservation::Completed) => {
                    self.is_terminal = true;
                    vec![Data {
                        key: "state".into(),
                        value: "completed".to_string().into(),
                    }]
                }
            };

            let obs = Observation {
                type_key: self.type_key.clone(),
                id: self.id.clone(),
                timestamp: Utc::now(),
                is_terminal: self.is_terminal,
                data,
            };

            if let Ok(mut observations) = self.observations.lock() {
                observations.push(obs.clone());
            }

            obs
        }

        fn is_in_terminal_state(&self) -> bool {
            self.is_terminal
        }
    }

    #[tokio::test]
    async fn test_observer_basic_functionality() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observable = TestObservable::new("test_task", Some("123".into()), observations.clone());

        let mut observer = Observer::new(observable);
        observer.observe(log::Level::Info, TestObservation::Started);
        observer.observe(log::Level::Info, TestObservation::Progress(50));
        observer.observe(log::Level::Info, TestObservation::Completed);

        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 4);

        assert_eq!(observations[0].type_key, "test_task");
        assert_eq!(observations[0].id, Some("123".into()));
        assert!(!observations[0].is_terminal);
        assert_eq!(observations[0].data.len(), 0);

        assert_eq!(observations[1].type_key, "test_task");
        assert_eq!(observations[1].data.len(), 1);
        let state = &observations[1].data[0];
        assert_eq!(state.key, "state");
        assert!(matches!(state.value, Value::String(ref s) if s == "started"));

        assert_eq!(observations[2].data.len(), 2);
        let progress = &observations[2].data[1];
        assert_eq!(progress.key, "progress");
        assert!(matches!(progress.value, Value::U64(50)));

        assert!(observations[3].is_terminal);
        let completed = &observations[3].data[0];
        assert!(matches!(completed.value, Value::String(ref s) if s == "completed"));
    }

    #[tokio::test]
    async fn test_observer_auto_termination() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observable = TestObservable::new("test_task", Some("123".into()), observations.clone());

        {
            let _observer = Observer::with_auto_termination(
                observable,
                log::Level::Info,
                TestObservation::Completed,
            );
        }

        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 2);
        assert!(observations[1].is_terminal);
        let state = &observations[1].data[0];
        assert!(matches!(state.value, Value::String(ref s) if s == "completed"));
    }
}
