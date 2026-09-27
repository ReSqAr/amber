//! The name a new upload gets in a store.
//!
//! An upload is named after its file ([`encode_path`]). But a store keeps a
//! blob under the name it was uploaded with, even after the file is renamed or
//! removed, so that name may already hold another blob - and rclone would
//! overwrite the store's only copy of it. A name the store already uses, or
//! that an earlier upload of the same push took, therefore gets part of the
//! blob's ID added before its extension: `Photo.jpg` becomes
//! `Photo (3f2a91c0).jpg`.
//!
//! Names are compared regardless of ASCII case, since some targets (macOS,
//! Windows, OneDrive) hold `Photo.jpg` and `photo.jpg` as one file. Encoded
//! names are ASCII, so that is all the folding they need.

use super::path_encoding::encode_path;
use crate::db::models::{FileTransferItem, FileTransferRequest, RclonePath};
use crate::db::stores::kv::{Upsert, UpsertAction};

/// The key names are compared by.
pub(crate) fn fold(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// `name` with ` (tag)` added before the extension of its last component.
fn with_tag(name: &str, tag: &str) -> String {
    let (dir, file) = match name.rsplit_once('/') {
        Some((dir, file)) => (Some(dir), file),
        None => (None, name),
    };
    let tagged = match file.rfind('.') {
        Some(i) if i > 0 => format!("{} ({tag}){}", &file[..i], &file[i..]),
        _ => format!("{file} ({tag})"),
    };
    match dir {
        Some(dir) => format!("{dir}/{tagged}"),
        None => tagged,
    }
}

/// A new upload claiming its name. It is keyed by the name it would get
/// without a tag; the value lists the tagged names already handed out under
/// that key, folded.
#[derive(Clone)]
pub(crate) struct Claim {
    name: String,
    request: FileTransferRequest,
}

impl Claim {
    pub(crate) fn new(request: FileTransferRequest) -> Self {
        Self {
            name: encode_path(&request.path).0,
            request,
        }
    }

    /// The name this upload gets, given the tagged names already handed out
    /// under its key - `None` if its untagged name is still free.
    fn pick(&self, claimed: Option<&[String]>) -> String {
        let Some(claimed) = claimed else {
            return self.name.clone();
        };
        let id = &self.request.blob_id.0;
        [8, 16]
            .into_iter()
            .filter_map(|len| id.get(..len))
            .chain([id.as_str()])
            .map(|tag| with_tag(&self.name, tag))
            .find(|name| !claimed.contains(&fold(name)))
            .unwrap_or_else(|| with_tag(&self.name, id))
    }

    /// The transfer item for this upload, given what [`Upsert::upsert`] saw
    /// under its key.
    pub(crate) fn into_transfer_item(
        self,
        transfer_id: u32,
        claimed: Option<Vec<String>>,
    ) -> FileTransferItem {
        let name = self.pick(claimed.as_deref());
        FileTransferItem {
            transfer_id,
            blob_id: self.request.blob_id,
            blob_size: self.request.blob_size,
            path: RclonePath(name),
        }
    }
}

impl Upsert for Claim {
    type K = String;
    type V = Vec<String>;

    fn key(&self) -> Self::K {
        fold(&self.name)
    }

    fn upsert(self, claimed: Option<Self::V>) -> UpsertAction<Self::V> {
        let name = self.pick(claimed.as_deref());
        match claimed {
            None => UpsertAction::Change(vec![]),
            Some(mut claimed) => {
                claimed.push(fold(&name));
                UpsertAction::Change(claimed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::{BlobID, BlobLocation, Path};

    fn claim(path: &str, blob_id: &str) -> Claim {
        Claim::new(FileTransferRequest {
            path: Path(path.into()),
            blob_id: BlobID(blob_id.into()),
            blob_size: 1,
            source_location: BlobLocation::Repository,
        })
    }

    /// Runs claims in order against a store that already uses `taken`, the
    /// way the key-value store would, and returns the names they get.
    fn names(taken: &[&str], claims: Vec<Claim>) -> Vec<String> {
        let mut store: std::collections::HashMap<String, Vec<String>> =
            taken.iter().map(|name| (fold(name), vec![])).collect();
        claims
            .into_iter()
            .map(|claim| {
                let previous = store.get(&claim.key()).cloned();
                if let UpsertAction::Change(v) = claim.clone().upsert(previous.clone()) {
                    store.insert(claim.key(), v);
                }
                claim.into_transfer_item(0, previous).path.0
            })
            .collect()
    }

    #[test]
    fn a_free_name_is_used_as_it_is() {
        assert_eq!(
            names(&["other.txt"], vec![claim("Übersicht/a.txt", "3f2a91c0aa")]),
            ["--c3-9cbersicht/a.txt"]
        );
    }

    #[test]
    fn a_name_the_store_uses_gets_a_tag() {
        assert_eq!(
            names(
                &["photos/a.jpg"],
                vec![claim("photos/a.jpg", "3f2a91c0aabbccdd00")]
            ),
            ["photos/a (3f2a91c0).jpg"]
        );
    }

    #[test]
    fn names_differing_in_case_do_not_share_a_file() {
        assert_eq!(
            names(
                &[],
                vec![
                    claim("Photo.JPG", "1111111111111111ff"),
                    claim("photo.jpg", "2222222222222222ff")
                ]
            ),
            ["Photo.JPG", "photo (22222222).jpg"]
        );
    }

    #[test]
    fn a_taken_tag_falls_back_to_a_longer_one() {
        assert_eq!(
            names(
                &["a.txt"],
                vec![
                    claim("a.txt", "3f2a91c0aaaaaaaa11"),
                    claim("a.txt", "3f2a91c0bbbbbbbb22"),
                    claim("a.txt", "3f2a91c0bbbbbbbb33"),
                ]
            ),
            [
                "a (3f2a91c0).txt",
                "a (3f2a91c0bbbbbbbb).txt",
                "a (3f2a91c0bbbbbbbb33).txt"
            ]
        );
    }

    #[test]
    fn tags_go_before_the_extension() {
        assert_eq!(with_tag("a/b.tar.gz", "t"), "a/b.tar (t).gz");
        assert_eq!(with_tag("README", "t"), "README (t)");
        assert_eq!(with_tag(".hidden", "t"), ".hidden (t)");
        assert_eq!(
            with_tag("d.x/-trailing dot-2e", "t"),
            "d.x/-trailing dot-2e (t)"
        );
    }
}
