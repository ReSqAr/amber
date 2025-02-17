use crate::utils::errors::InternalError;

pub(crate) async fn find_available_port() -> Result<u16, InternalError> {
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_find_available_port() {
        let port = find_available_port().await.unwrap();
        assert!(port > 0);
    }
}
