/// ============ 1. Observation & UpdateAction ============

#[derive(Debug, Clone)]
pub struct Observation {
    /// A type key used to route to a particular builder/item list.
    pub type_key: String,
    /// Optional ID to identify or group the item within this type.
    pub id: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Whether the item is finished.
    pub state: String,
    /// Whether the item is finished.
    pub is_terminal: bool,
    /// Optional message for display or logging.
    pub message: Option<String>,
    /// Current progress position.
    pub position: Option<u64>,
    /// Optional total length for determinate progress.
    pub length: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct Message {
    pub(crate) level: log::Level,
    pub(crate) observation: Observation,
}
