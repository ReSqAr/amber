pub enum Flow<T> {
    Data(T),
    Shutdown,
}

impl<T> From<T> for Flow<T> {
    fn from(value: T) -> Self {
        Flow::Data(value)
    }
}

#[derive(Debug)]
pub enum ExtFlow<T> {
    Data(T),
    Shutdown(T),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_flow_from() {
        let data: Flow<i32> = 42.into();
        match data {
            Flow::Data(x) => assert_eq!(x, 42),
            _ => panic!("Expected Flow::Data"),
        }
    }
}
