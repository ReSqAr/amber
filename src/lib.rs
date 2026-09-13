pub mod cli;
pub mod commands;
mod connection;
mod db;
pub mod flightdeck;
mod grpc;
mod logic;
mod repository;
mod utils;

/// Points SSH host key verification at a specific `known_hosts` file.
///
/// The programmatic equivalent of the `AMBER_SSH_KNOWN_HOSTS` environment
/// variable, which it overrides. Takes effect only before the first SSH
/// connection is established.
pub fn set_ssh_known_hosts_path(path: std::path::PathBuf) {
    connection::ssh::set_known_hosts_path(path);
}
