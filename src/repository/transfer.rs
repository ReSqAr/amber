use crate::repository::local::LocalRepository;

pub(crate) struct Transfer {
    transfer_id: u32,
    local: LocalRepository,
    source: (), // TODO
    target: (), // TODO
}

impl Transfer {}
