pub enum Flow<T> {
    Data(T),
    Shutdown,
}

impl<T> From<T> for Flow<T> {
    fn from(value: T) -> Self {
        Flow::Data(value)
    }
}

pub enum ExtFlow<T> {
    Data(T),
    Shutdown(T),
}
