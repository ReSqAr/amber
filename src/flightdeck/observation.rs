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
