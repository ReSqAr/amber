use crate::utils::control_flow::Message::Data;

pub enum Message<T> {
    Data(T),
    Shutdown,
}

impl<T> From<T> for Message<T> {
    fn from(value: T) -> Self {
        Data(value)
    }
}
