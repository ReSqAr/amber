//! The name a newly uploaded file gets inside an rclone store.
//!
//! Some rclone targets cannot hold every name a local file system can: no
//! UTF-8, no `:` or `?`, no trailing dots or spaces, and so on. The store
//! records where it put each blob, and downloads fetch it from exactly there,
//! so this encoding is only ever applied once: when a file is uploaded. It
//! works on each path component on its own and keeps `/` as the separator:
//!
//! * A component made only of [`is_safe`] characters, which does not start
//!   with `-` and does not end with `.` or a space, is kept as it is. That
//!   covers almost every real file name, so the store stays browsable.
//! * Every other component is written as `-` followed by its UTF-8 bytes,
//!   where each byte that is not [`is_safe`] - and each `-` - becomes `-` and
//!   two lowercase hex digits. A final `.` or space is escaped as well.
//!
//! A leading `-` marks an encoded component, and a kept component never
//! starts with one, so every name decodes back to exactly one path - distinct
//! files never share a name, and a store can be read without amber:
//!
//! ```text
//! 2024_05_01 Urlaub.jpg -> 2024_05_01 Urlaub.jpg
//! Übersicht/größe.txt   -> --c3-9cbersicht/-gr-c3-b6-c3-9fe.txt
//! Bericht 2024-05.pdf   -> Bericht 2024-05.pdf
//! Grüße 2024-05.pdf     -> -Gr-c3-bc-c3-9fe 2024-2d05.pdf
//! -notes.txt            -> --2dnotes.txt
//! ```

use crate::db::models::{Path, RclonePath};

const ESCAPE: u8 = b'-';

/// Bytes that no rclone backend rewrites and that are fine inside a name.
fn is_safe(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b" _-.,+=()[]!@&'".contains(&b)
}

fn needs_encoding(component: &str) -> bool {
    let bytes = component.as_bytes();
    match (bytes.first(), bytes.last()) {
        (Some(&first), Some(&last)) => {
            first == ESCAPE || last == b'.' || last == b' ' || !bytes.iter().copied().all(is_safe)
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

/// The name under which a newly uploaded `path` is stored in an rclone store.
pub(crate) fn encode_path(path: &Path) -> RclonePath {
    RclonePath(encode(&path.0))
}

fn encode(path: &str) -> String {
    path.split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The inverse of [`encode`]. amber never needs it - downloads use the
    /// location the store recorded - but it is what shows the encoding to be
    /// unambiguous.
    fn decode_path(name: &str) -> Option<String> {
        Some(
            name.split('/')
                .map(decode_component)
                .collect::<Option<Vec<_>>>()?
                .join("/"),
        )
    }

    fn decode_component(component: &str) -> Option<String> {
        let Some(body) = component.strip_prefix(ESCAPE as char) else {
            return Some(component.to_owned());
        };

        let mut bytes = Vec::with_capacity(body.len());
        let mut rest = body.as_bytes();
        while let Some((&b, tail)) = rest.split_first() {
            if b == ESCAPE {
                let hex = std::str::from_utf8(tail.get(..2)?).ok()?;
                bytes.push(u8::from_str_radix(hex, 16).ok()?);
                rest = tail.get(2..)?;
            } else {
                bytes.push(b);
                rest = tail;
            }
        }
        String::from_utf8(bytes).ok()
    }

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
            " leading space",
        ] {
            assert_eq!(encode(path), path);
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
            assert_eq!(encode(path), name, "encoding {path:?}");
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
            let name = encode(path);
            for component in name.split('/') {
                assert!(component.bytes().all(is_safe), "{component:?}");
                assert!(!component.ends_with('.') && !component.ends_with(' '));
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
                let name = encode(&path);
                assert_eq!(decode_path(&name).unwrap(), path, "{path:?} -> {name:?}");
            }
        }
    }

    #[test]
    fn distinct_paths_get_distinct_names() {
        let paths = [
            "-2d", "--2d", "-", "--", "a-b", "-a-2db", "ä", "-c3-a4", "--c3-a4", "x.", "-x-2e",
        ];
        let names: std::collections::HashSet<_> = paths.iter().map(|p| encode(p)).collect();
        assert_eq!(names.len(), paths.len());
    }

    #[test]
    fn rejects_malformed_names() {
        assert!(decode_path("-a-zz").is_none());
        assert!(decode_path("-a-2").is_none());
        assert!(decode_path("-a-").is_none());
        assert!(decode_path("--ff").is_none());
    }

    /// Paths built mostly from the characters the encoding treats specially,
    /// so edge cases come up far more often than in arbitrary strings.
    fn tricky_path() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9 _.,+=()!@&'?:*%~#\\\\\\[\\]\\-/\\t\u{e4}\u{df}\u{301}\u{65e5}\u{1f600}]{0,24}"
    }

    fn check_encoding(path: &str) -> Result<(), TestCaseError> {
        let name = encode(path);
        prop_assert_eq!(decode_path(&name), Some(path.to_owned()), "name {:?}", name);
        for component in name.split('/') {
            prop_assert!(
                component.bytes().all(is_safe),
                "{:?} in {:?}",
                component,
                name
            );
            prop_assert!(!component.ends_with(' '), "{:?}", name);
            prop_assert!(!component.ends_with('.'), "{:?}", name);
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        #[test]
        fn prop_round_trips_arbitrary_paths(path in any::<String>()) {
            check_encoding(&path)?;
        }

        #[test]
        fn prop_round_trips_tricky_paths(path in tricky_path()) {
            check_encoding(&path)?;
        }

        #[test]
        fn prop_distinct_paths_get_distinct_names(a in tricky_path(), b in tricky_path()) {
            prop_assume!(a != b);
            prop_assert_ne!(encode(&a), encode(&b));
        }

        #[test]
        fn prop_plain_components_are_kept(
            component in "[a-zA-Z0-9 _.,+=()!@&'\\[\\]][a-zA-Z0-9 _.,+=()!@&'\\[\\]\\-]{0,20}[a-zA-Z0-9_,+=()!@&'\\[\\]\\-]"
        ) {
            prop_assert_eq!(encode(&component), component);
        }
    }
}
