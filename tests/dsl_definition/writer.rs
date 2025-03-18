use std::io::Write;
use tokio::sync::mpsc::UnboundedSender;

// escape hatch of all escape hatches
pub struct ChannelWriter {
    sender: UnboundedSender<Vec<u8>>,
}

impl ChannelWriter {
    pub fn new(sender: UnboundedSender<Vec<u8>>) -> Self {
        Self { sender }
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let data = buf.to_vec();
        self.sender
            .send(data)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Channel closed"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
