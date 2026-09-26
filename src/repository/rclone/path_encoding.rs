//! How a repository path is named inside an rclone store.
//!
//! Some rclone targets cannot hold every name a local file system can: no
//! UTF-8, no `:` or `?`, no trailing dots or spaces, and so on. The store's
//! metadata keeps the real path, so the name on the target only has to be an
//! unambiguous, reversible function of it. The encoding works on each path
//! component on its own and keeps `/` as the separator:
//!
//! * A component made only of [`SAFE`] characters, which does not start with
//!   `-` or a space and does not end with `.` or a space, is kept as it is.
//!   That covers almost every real file name, so the store stays browsable.
//! * Every other component is written as `-` followed by its UTF-8 bytes,
//!   where each byte that is not [`SAFE`] - and each `-` - becomes `-` and two
//!   lowercase hex digits. A final `.` or space is escaped as well.
//!
//! A leading `-` marks an encoded component, and a kept component never
//! starts with one, so every name decodes back to exactly one path:
//!
//! ```text
//! 2024_05_01 Urlaub.jpg -> 2024_05_01 Urlaub.jpg
//! Übersicht/größe.txt   -> --c3-9cbersicht/-gr-c3-b6-c3-9fe.txt
//! Bericht 2024-05.pdf   -> Bericht 2024-05.pdf
//! Grüße 2024-05.pdf     -> -Gr-c3-bc-c3-9fe 2024-2d05.pdf
//! -notes.txt            -> --2dnotes.txt
//! ```

use crate::utils::errors::{AppError, InternalError};

const ESCAPE: u8 = b'-';

/// Bytes that no rclone backend rewrites and that are fine inside a name.
fn is_safe(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b" _-.,+=()[]!@&'".contains(&b)
}

fn needs_encoding(component: &str) -> bool {
    let bytes = component.as_bytes();
    match (bytes.first(), bytes.last()) {
        (Some(&first), Some(&last)) => {
            first == ESCAPE
                || first == b' '
                || last == b'.'
                || last == b' '
                || !bytes.iter().copied().all(is_safe)
        }
        _ => false,
    }
}

fn encode_component(component: &str) -> String {
    if !needs_encoding(component) {
        return component.to_owned();
    }

    let bytes = component.as_bytes();
    let mut encoded = String::with_capacity(1 + bytes.len() * 3);
    encoded.push(ESCAPE as char);
    for (i, &b) in bytes.iter().enumerate() {
        let is_last = i + 1 == bytes.len();
        if is_safe(b) && b != ESCAPE && !(is_last && (b == b'.' || b == b' ')) {
            encoded.push(b as char);
        } else {
            encoded.push_str(&format!("-{b:02x}"));
        }
    }
    encoded
}

#[allow(clippy::result_large_err)]
fn decode_component(component: &str) -> Result<String, InternalError> {
    let invalid = |message: &str| AppError::Parse {
        message: message.into(),
        raw: component.into(),
    };

    let Some(body) = component.strip_prefix(ESCAPE as char) else {
        return Ok(component.to_owned());
    };

    let mut bytes = Vec::with_capacity(body.len());
    let mut rest = body.as_bytes();
    while let Some((&b, tail)) = rest.split_first() {
        if b == ESCAPE {
            let hex = tail
                .get(..2)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| invalid("expected two hex digits after '-'"))?;
            bytes.push(hex);
            rest = tail.get(2..).unwrap_or_default();
        } else {
            bytes.push(b);
            rest = tail;
        }
    }

    String::from_utf8(bytes).map_err(|_| invalid("not valid UTF-8").into())
}

/// The name under which `path` is stored in an rclone store.
pub(crate) fn encode_path(path: &str) -> String {
    path.split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

/// The repository path stored under the rclone name `name`.
#[allow(clippy::result_large_err)]
pub(crate) fn decode_path(name: &str) -> Result<String, InternalError> {
    Ok(name
        .split('/')
        .map(decode_component)
        .collect::<Result<Vec<_>, _>>()?
        .join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_names_stay_as_they_are() {
        for path in [
            "test.txt",
            "IMG_1234.JPG",
            "_DSC1234.ARW",
            "__init__.py",
            "2024_05_01 Urlaub/Bild (1).jpg",
            "Bericht 2024-05.pdf",
            "a-b/c-d/e_f.tar.gz",
            ".hidden/.config",
            "[draft] notes, v2+final=ok!@&'.md",
        ] {
            assert_eq!(encode_path(path), path);
        }
    }

    #[test]
    fn examples() {
        let cases = [
            (
                "Übersicht/größe.txt",
                "--c3-9cbersicht/-gr-c3-b6-c3-9fe.txt",
            ),
            ("Grüße 2024-05.pdf", "-Gr-c3-bc-c3-9fe 2024-2d05.pdf"),
            ("-notes.txt", "--2dnotes.txt"),
            (" leading space", "- leading space"),
            ("trailing space ", "-trailing space-20"),
            ("trailing dot.", "-trailing dot-2e"),
            ("..", "-.-2e"),
            ("what?.txt", "-what-3f.txt"),
            ("a:b", "-a-3ab"),
            ("100%", "-100-25"),
            ("caf\u{e9}.txt", "-caf-c3-a9.txt"),
            ("cafe\u{301}.txt", "-cafe-cc-81.txt"),
            ("😀.txt", "--f0-9f-98-80.txt"),
        ];
        for (path, name) in cases {
            assert_eq!(encode_path(path), name, "encoding {path:?}");
            assert_eq!(decode_path(name).unwrap(), path, "decoding {name:?}");
        }
    }

    #[test]
    fn encoded_names_are_rclone_safe() {
        for path in [
            "Übersicht/größe.txt",
            "日本語/ファイル.txt",
            "a\\b|c<d>e\"f*g#h%i~j",
            "tab\there",
            " ",
            ".",
        ] {
            let name = encode_path(path);
            for component in name.split('/') {
                assert!(component.bytes().all(is_safe), "{component:?}");
                assert!(!component.ends_with('.') && !component.ends_with(' '));
                assert!(!component.starts_with(' '));
            }
            assert_eq!(decode_path(&name).unwrap(), path);
        }
    }

    #[test]
    fn round_trips_every_single_byte_character() {
        for c in (1u8..=0x7f).map(char::from).chain(['ä', 'ß', '€', '😀']) {
            for path in [
                c.to_string(),
                format!("{c}x"),
                format!("x{c}"),
                format!("x{c}x"),
            ] {
                let name = encode_path(&path);
                assert_eq!(decode_path(&name).unwrap(), path, "{path:?} -> {name:?}");
            }
        }
    }

    #[test]
    fn distinct_paths_get_distinct_names() {
        let paths = [
            "-2d", "--2d", "-", "--", "a-b", "-a-2db", "ä", "-c3-a4", "--c3-a4", "x.", "-x-2e",
        ];
        let names: std::collections::HashSet<_> = paths.iter().map(|p| encode_path(p)).collect();
        assert_eq!(names.len(), paths.len());
    }

    #[test]
    fn rejects_malformed_names() {
        assert!(decode_path("-a-zz").is_err());
        assert!(decode_path("-a-2").is_err());
        assert!(decode_path("-a-").is_err());
        assert!(decode_path("--ff").is_err());
    }
}
