use crate::utils::errors::InternalError;
use log::{debug, error, info};
use ssh2::{ErrorCode, Session};
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::raw::c_int;
use std::sync::{Arc, Mutex};
use std::thread;

const ERR_WOULD_BLOCK: c_int = -37;

// A select-based forwarder that listens on `local_addr` and forwards data to `remote_addr`
// over an existing SSH session. We also require the underlying TCP socket (`session_socket`).
pub fn port_forward(
    session: Arc<Mutex<Session>>,
    session_socket: Arc<TcpStream>,
    local_host: String,
    local_port: u16,
    remote_host: String,
    remote_port: u16,
) -> Result<(), InternalError> {
    let local_addr = format!("{local_host}:{local_port}");

    let listener = TcpListener::bind(&local_addr)?;
    info!("listening on {}", local_addr);

    loop {
        let (stream, addr) = listener.accept()?;
        debug!("new connection from {addr}");

        let session = Arc::clone(&session);
        let session_socket = Arc::clone(&session_socket);
        let remote_host_clone = remote_host.clone();

        thread::spawn(move || {
            if let Err(err) = forward_connection(
                stream,
                session,
                session_socket,
                &remote_host_clone,
                remote_port,
            ) {
                error!("connection {addr} error: {err}");
            }
        });
    }
}

fn forward_connection(
    mut local_stream: TcpStream,
    session: Arc<Mutex<Session>>,
    session_socket: Arc<TcpStream>,
    remote_host: &str,
    remote_port: u16,
) -> Result<(), InternalError> {
    debug!("forwarding connection to {remote_host}:{remote_port}");

    local_stream
        .set_nonblocking(true)
        .map_err(|e| InternalError::Ssh(format!("local_stream.set_nonblocking(): {e}")))?;
    session_socket
        .set_nonblocking(true)
        .map_err(|e| InternalError::Ssh(format!("session_socket.set_nonblocking(): {e}")))?;

    {
        let sess = session.lock().unwrap();
        sess.set_blocking(false);
    }

    let mut channel = loop {
        let sess = session.lock().unwrap();
        match sess.channel_direct_tcpip(remote_host, remote_port, None) {
            Ok(ch) => break ch,
            Err(e) if e.code() == ErrorCode::Session(ERR_WOULD_BLOCK) => {
                thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            Err(e) => {
                return Err(InternalError::Ssh(format!(
                    "sess.channel_direct_tcpip(): {e}"
                )))
            }
        }
    };

    // two buffers: local->remote & remote->local
    let mut buf_l2r = vec![0u8; 8192];
    let mut buf_r2l = vec![0u8; 8192];
    let mut len_l2r = 0;
    let mut len_r2l = 0;

    loop {
        // try to read from local stream
        if len_l2r < buf_l2r.len() {
            match local_stream.read(&mut buf_l2r[len_l2r..]) {
                Ok(0) => {
                    debug!("local stream closed");
                    channel
                        .send_eof()
                        .map_err(|e| InternalError::Ssh(format!("channel.send_eof(): {e}")))?;
                    break;
                }
                Ok(n) => {
                    len_l2r += n;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(e) => {
                    error!("local read error: {e}");
                    break;
                }
            }
        }

        // try to write to channel
        if len_l2r > 0 {
            match channel.write(&buf_l2r[..len_l2r]) {
                Ok(written) => {
                    if written < len_l2r {
                        buf_l2r.copy_within(written..len_l2r, 0);
                    }
                    len_l2r -= written;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(e) => {
                    error!("remote write error: {e}");
                    break;
                }
            }
        }

        // try to read from channel
        if len_r2l < buf_r2l.len() {
            match channel.read(&mut buf_r2l[len_r2l..]) {
                Ok(0) => {
                    debug!("channel closed");
                    local_stream
                        .shutdown(Shutdown::Write)
                        .map_err(|e| InternalError::Ssh(format!("local_stream.shutdown(): {e}")))?;
                    break;
                }
                Ok(n) => {
                    len_r2l += n;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(e) => {
                    error!("remote read error: {e}");
                    break;
                }
            }
        }

        // try to write to local stream
        if len_r2l > 0 {
            match local_stream.write(&buf_r2l[..len_r2l]) {
                Ok(written) => {
                    if written < len_r2l {
                        buf_r2l.copy_within(written..len_r2l, 0);
                    }
                    len_r2l -= written;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(e) => {
                    error!("local write error: {e}");
                    break;
                }
            }
        }

        if channel.eof() && len_l2r == 0 {
            debug!("channel EOF and no pending data l2r, breaking loop");
            break;
        }

        // prevent busy-looping
        if len_l2r == 0 && len_r2l == 0 {
            thread::sleep(std::time::Duration::from_millis(1)); // Reduce CPU usage when idle
        }
    }

    debug!("closing channel");
    loop {
        match channel.close() {
            Ok(_) => break,
            Err(e) if e.code() == ErrorCode::Session(ERR_WOULD_BLOCK) => {
                thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            Err(e) => return Err(InternalError::Ssh(format!("channel.close(): {e}"))),
        }
    }
    debug!("channel closed");

    Ok(())
}
