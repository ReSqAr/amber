//! The name a new upload gets in a store.
//!
//! An upload is named after its file ([`encode_path`]). But a store keeps a
//! blob under the name it was uploaded with, even after the file is renamed or
//! removed, so that name may already hold another blob - and rclone would
//! overwrite the store's only copy of it. A name the store already uses, or
//! that an earlier upload of the same push took, therefore gets part of the
//! blob's ID added before its extensions: `Photo.jpg` becomes
//! `Photo.3f2a91c0.jpg`, `b.tar.gz` becomes `b.3f2a91c0.tar.gz`, and `README`
//! becomes `README.3f2a91c0`.
//!
//! Every name in use - whether the store holds it or this push took it, tagged
//! or not - is one key in a scratch store, and an upload takes the first of
//! its candidate names whose key is free.
//!
//! Names are compared regardless of ASCII case, since some targets (macOS,
//! Windows, OneDrive) hold `Photo.jpg` and `photo.jpg` as one file. Encoded
//! names are ASCII, so that is all the folding they need.

use super::path_encoding::encode_path;
use crate::db::models::{FileTransferItem, FileTransferRequest, RclonePath};
use crate::db::stores::kv::{Upsert, UpsertAction};
use std::collections::VecDeque;

/// The key names are compared by.
pub(crate) fn fold(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// `name` with `.tag` added before all extensions of its last component. A
/// leading dot, as in `.hidden`, starts no extension.
fn with_tag(name: &str, tag: &str) -> String {
    let (dir, file) = match name.rsplit_once('/') {
        Some((dir, file)) => (Some(dir), file),
        None => (None, name),
    };
    let tagged = match file.get(1..).and_then(|rest| rest.find('.')) {
        Some(i) => format!("{}.{tag}{}", &file[..=i], &file[i + 1..]),
        None => format!("{file}.{tag}"),
    };
    match dir {
        Some(dir) => format!("{dir}/{tagged}"),
        None => tagged,
    }
}

/// A new upload claiming a name in the store: `name` if it is free, else the
/// first of `rest` that is.
#[derive(Clone)]
pub(crate) struct Claim {
    request: FileTransferRequest,
    name: String,
    rest: VecDeque<String>,
}

impl Claim {
    pub(crate) fn new(request: FileTransferRequest) -> Self {
        let name = encode_path(&request.path).0;
        let id = &request.blob_id.0;
        let mut rest: Vec<String> = [8, 16]
            .into_iter()
            .filter_map(|len| id.get(..len))
            .chain([id.as_str()])
            .map(|tag| with_tag(&name, tag))
            .collect();
        rest.dedup();
        Self {
            request,
            name,
            rest: rest.into(),
        }
    }

    /// The transfer item for this upload, under the name it claimed.
    pub(crate) fn into_transfer_item(self, transfer_id: u32) -> FileTransferItem {
        FileTransferItem {
            transfer_id,
            blob_id: self.request.blob_id,
            blob_size: self.request.blob_size,
            path: RclonePath(self.name),
        }
    }

    /// The file this upload is for, to report it.
    pub(crate) fn path(&self) -> &str {
        &self.request.path.0
    }
}

impl Upsert for Claim {
    type K = String;
    type V = ();

    fn key(&self) -> Self::K {
        fold(&self.name)
    }

    /// Takes the name if it is free, and moves on to the next one if not. With
    /// none left, the claim ends on a name that is taken.
    fn upsert(mut self, taken: Option<()>) -> UpsertAction<(), Self> {
        match taken {
            None => UpsertAction::Change(()),
            Some(()) => match self.rest.pop_front() {
                Some(name) => {
                    self.name = name;
                    UpsertAction::Next(self)
                }
                None => UpsertAction::NoChange,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::error::DBError;
    use crate::db::models::{BlobID, BlobLocation, Path};
    use crate::db::stores::kv::{Store, UpsertedValue};
    use futures::{StreamExt, TryStreamExt, stream};
    use tempfile::TempDir;

    fn claim(path: &str, blob_id: &str) -> Claim {
        Claim::new(FileTransferRequest {
            path: Path(path.into()),
            blob_id: BlobID(blob_id.into()),
            blob_size: 1,
            source_location: BlobLocation::Repository,
        })
    }

    /// Runs `claims` against a store that already holds `taken`, the way a
    /// push does, and returns the names they get - `None` for none.
    async fn names(taken: &[&str], claims: Vec<Claim>) -> Vec<Option<String>> {
        let dir = TempDir::new().unwrap();
        let store = Store::<String, ()>::new(dir.path().join("names"), "names".into())
            .await
            .unwrap();
        let taken: Vec<Result<_, DBError>> = taken
            .iter()
            .map(|name| Ok((fold(name), Some(()))))
            .collect();
        store.apply(stream::iter(taken).boxed()).await.unwrap();

        let claims = stream::iter(claims.into_iter().map(Ok::<_, DBError>)).boxed();
        let (claimed, writes) = store.streaming_upsert(claims);
        let names = claimed
            .map_ok(
                |UpsertedValue {
                     upsert,
                     previous_value,
                 }| {
                    previous_value
                        .is_none()
                        .then(|| upsert.into_transfer_item(0).path.0)
                },
            )
            .try_collect()
            .await
            .unwrap();
        writes.await.unwrap().unwrap();
        store.close().await.unwrap();
        names
    }

    fn some(names: &[&str]) -> Vec<Option<String>> {
        names.iter().map(|name| Some(name.to_string())).collect()
    }

    #[tokio::test]
    async fn a_free_name_is_used_as_it_is() {
        assert_eq!(
            names(&["other.txt"], vec![claim("Übersicht/a.txt", "3f2a91c0aa")]).await,
            some(&["--c3-9cbersicht/a.txt"])
        );
    }

    #[tokio::test]
    async fn a_name_the_store_uses_gets_a_tag() {
        assert_eq!(
            names(
                &["photos/a.jpg"],
                vec![claim("photos/a.jpg", "3f2a91c0aabbccdd00")]
            )
            .await,
            some(&["photos/a.3f2a91c0.jpg"])
        );
    }

    #[tokio::test]
    async fn names_differing_in_case_do_not_share_a_file() {
        assert_eq!(
            names(
                &[],
                vec![
                    claim("Photo.JPG", "1111111111111111ff"),
                    claim("photo.jpg", "2222222222222222ff")
                ]
            )
            .await,
            some(&["Photo.JPG", "photo.22222222.jpg"])
        );
    }

    /// Blob IDs sharing a prefix within one push get longer tags.
    #[tokio::test]
    async fn a_taken_tag_falls_back_to_a_longer_one() {
        assert_eq!(
            names(
                &["a.txt"],
                vec![
                    claim("a.txt", "3f2a91c0aaaaaaaa11"),
                    claim("a.txt", "3f2a91c0bbbbbbbb22"),
                    claim("a.txt", "3f2a91c0bbbbbbbb33"),
                ]
            )
            .await,
            some(&[
                "a.3f2a91c0.txt",
                "a.3f2a91c0bbbbbbbb.txt",
                "a.3f2a91c0bbbbbbbb33.txt"
            ])
        );
    }

    /// A tagged name an earlier push left in the store is taken as well.
    #[tokio::test]
    async fn a_tag_an_earlier_push_used_is_taken() {
        assert_eq!(
            names(
                &["a.txt", "a.3f2a91c0.txt", "a.3f2a91c0bbbbbbbb.txt"],
                vec![claim("a.txt", "3f2a91c0bbbbbbbb33")]
            )
            .await,
            some(&["a.3f2a91c0bbbbbbbb33.txt"])
        );
    }

    /// A file that happens to be named like a tagged name is not overwritten
    /// either.
    #[tokio::test]
    async fn a_tagged_name_a_file_already_has_is_taken() {
        assert_eq!(
            names(
                &["a.txt"],
                vec![
                    claim("a.3f2a91c0.txt", "1111111111111111ff"),
                    claim("a.txt", "3f2a91c0aaaaaaaa11"),
                ]
            )
            .await,
            some(&["a.3f2a91c0.txt", "a.3f2a91c0aaaaaaaa.txt"])
        );
    }

    #[tokio::test]
    async fn every_name_in_use_leaves_no_name() {
        assert_eq!(
            names(
                &["a.txt", "a.3f2a91c0.txt"],
                vec![claim("a.txt", "3f2a91c0")]
            )
            .await,
            [None]
        );
    }

    #[test]
    fn tags_go_before_the_extensions() {
        assert_eq!(with_tag("a/b.tar.gz", "t"), "a/b.t.tar.gz");
        assert_eq!(with_tag("photo.jpg", "t"), "photo.t.jpg");
        assert_eq!(with_tag(".config.json", "t"), ".config.t.json");
        assert_eq!(with_tag("README", "t"), "README.t");
        assert_eq!(with_tag(".hidden", "t"), ".hidden.t");
        assert_eq!(
            with_tag("d.x/-trailing dot-2e", "t"),
            "d.x/-trailing dot-2e.t"
        );
    }
}
