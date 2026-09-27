mod dsl_definition;

// File and directory names outside ASCII: German umlauts and ß, accents,
// CJK, Cyrillic, emoji, and a name with spaces. Every test below moves these
// across one boundary (local, rclone, ssh) and checks they arrive unchanged.

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_add_status_missing() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "Übersicht/größe.txt" "Umlaute"
        @a write_file "日本語/ファイル.txt" "CJK"
        @a write_file "😀 emoji.txt" "emoji"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b

        # action
        @a amber sync b

        # then
        @b amber missing
        assert_output_contains "missing Übersicht/größe.txt (exists in: a)"
        assert_output_contains "missing 日本語/ファイル.txt (exists in: a)"
        assert_output_contains "missing 😀 emoji.txt (exists in: a)"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_local_push_pull() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "Übersicht/größe.txt" "Umlaute aus a"
        @a write_file "Café/crème brûlée.txt" "Akzente aus a"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b

        @b write_file "Москва/привет.txt" "Kyrillisch aus b"
        @b write_file "日本語/ファイル.txt" "CJK aus b"
        @b write_file "😀/🎉 party.txt" "Emoji aus b"
        @b amber add

        # action
        @a amber pull b
        @a amber push b
        @b amber sync

        # then
        @a assert_exists "Москва/привет.txt" "Kyrillisch aus b"
        @a assert_exists "日本語/ファイル.txt" "CJK aus b"
        @a assert_exists "😀/🎉 party.txt" "Emoji aus b"
        @b assert_exists "Übersicht/größe.txt" "Umlaute aus a"
        @b assert_exists "Café/crème brûlée.txt" "Akzente aus a"
        assert_equal a b

        @a amber missing
        assert_output_contains "no files missing"
        @b amber missing
        assert_output_contains "no files missing"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_local_path_selector() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "Übersicht/größe.txt" "selected"
        @a write_file "Übersicht/übrig.txt" "also selected"
        @a write_file "Ärger.txt" "not selected"
        @a amber add

        @b amber init b
        @a amber remote add b local $ROOT/b

        # action
        @a amber push b Übersicht
        @b amber sync

        # then
        @b assert_exists "Übersicht/größe.txt" "selected"
        @b assert_exists "Übersicht/übrig.txt" "also selected"
        @b assert_does_not_exist "Ärger.txt"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_mv_then_push() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "plain.txt" "renamed content"
        @a amber add
        @a amber mv plain.txt "Grüße/Straße.txt"

        @b amber init b
        @a amber remote add b local $ROOT/b

        # action
        @a amber push b
        @b amber sync

        # then
        @b assert_exists "Grüße/Straße.txt" "renamed content"
        @b assert_does_not_exist plain.txt
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_rclone_push_pull() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "Übersicht/größe.txt" "Umlaute im store"
        @a write_file "日本語/ファイル.txt" "CJK im store"
        @a write_file "😀/🎉 party.txt" "Emoji im store"
        @a amber add

        @b amber init b

        @a amber remote add store rclone :local:/$ROOT/rclone
        @b amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber push store
        @b amber pull store

        # then: the store holds the files under ASCII-only names ...
        @rclone assert_exists "--c3-9cbersicht/-gr-c3-b6-c3-9fe.txt" "Umlaute im store"
        @rclone assert_exists "--f0-9f-98-80/--f0-9f-8e-89 party.txt" "Emoji im store"
        @rclone assert_does_not_exist "Übersicht/größe.txt"

        # ... and they come back out unchanged
        @b assert_exists "Übersicht/größe.txt" "Umlaute im store"
        @b assert_exists "日本語/ファイル.txt" "CJK im store"
        @b assert_exists "😀/🎉 party.txt" "Emoji im store"
        assert_equal a b

        @b amber missing
        assert_output_contains "no files missing"
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_rclone_missing_after_sync() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "Übersicht/größe.txt" "Umlaute"
        @a amber add

        @b amber init b

        @a amber remote add store rclone :local:/$ROOT/rclone
        @b amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber push store
        @b amber sync store

        # then
        @b amber missing
        assert_output_contains "missing Übersicht/größe.txt (exists in: a, store)"
    "#;
    dsl_definition::run_dsl_script(script).await
}

/// "é" can be spelled as one code point (NFC, U+00E9) or as "e" followed by a
/// combining acute accent (NFD, U+0065 U+0301). Linux treats these as two
/// different names, so both files must survive every hop as distinct files.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_nfc_and_nfd_names_stay_distinct() -> Result<(), anyhow::Error> {
    let nfc = "caf\u{e9}.txt";
    let nfd = "cafe\u{301}.txt";
    assert_ne!(nfc, nfd);

    let script = format!(
        r#"
        # when
        @a amber init a
        @a write_file "{nfc}" "composed"
        @a write_file "{nfd}" "decomposed"
        @a amber add

        @b amber init b
        @c amber init c

        @a amber remote add b local $ROOT/b
        @a amber remote add store rclone :local:/$ROOT/rclone
        @c amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber push b
        @b amber sync
        @a amber push store
        @c amber pull store

        # then
        @b assert_exists "{nfc}" "composed"
        @b assert_exists "{nfd}" "decomposed"
        @rclone assert_exists "-caf-c3-a9.txt" "composed"
        @rclone assert_exists "-cafe-cc-81.txt" "decomposed"
        @c assert_exists "{nfc}" "composed"
        @c assert_exists "{nfd}" "decomposed"
        assert_equal a b
        assert_equal a c
    "#
    );
    dsl_definition::run_dsl_script(&script).await
}

/// Names that some rclone targets cannot hold are escaped in the store, while
/// ordinary names - underscores, dashes, spaces and all - are kept as they are.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rclone_store_escapes_only_names_that_need_it() -> Result<(), anyhow::Error>
{
    let script = r#"
        # when
        @a amber init a
        @a write_file "_DSC1234 (1).JPG" "plain"
        @a write_file "2024-05-01_Urlaub/IMG_0001.jpg" "plain in folder"
        @a write_file "what?.txt" "question mark"
        @a write_file "a:b.txt" "colon"
        @a write_file "ends with dot." "trailing dot"
        @a write_file "-starts-with-dash.txt" "leading dash"
        @a amber add

        @b amber init b

        @a amber remote add store rclone :local:/$ROOT/rclone
        @b amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber push store
        @b amber pull store

        # then
        @rclone assert_exists "_DSC1234 (1).JPG" "plain"
        @rclone assert_exists "2024-05-01_Urlaub/IMG_0001.jpg" "plain in folder"
        @rclone assert_exists "-what-3f.txt" "question mark"
        @rclone assert_exists "-a-3ab.txt" "colon"
        @rclone assert_exists "-ends with dot-2e" "trailing dot"
        @rclone assert_exists "--2dstarts-2dwith-2ddash.txt" "leading dash"
        assert_equal a b
    "#;
    dsl_definition::run_dsl_script(script).await
}

/// Plain `--files-from` would skip lines starting with `#` or `;` as comments
/// and trim the whitespace around each line, so these files would never be
/// copied. amber hands rclone its list with `--files-from-raw` instead.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rclone_store_names_files_from_would_mangle() -> Result<(), anyhow::Error>
{
    let script = r##"
        # when
        @a amber init a
        @a write_file "#notes.txt" "hash"
        @a write_file ";semi.txt" "semicolon"
        @a write_file " leading space.txt" "leading space"
        @a write_file "trailing space.txt " "trailing space"
        @a write_file "#archive/;old.txt" "in folder"
        @a amber add

        @b amber init b

        @a amber remote add store rclone :local:/$ROOT/rclone
        @b amber remote add store rclone :local:/$ROOT/rclone

        # action
        @a amber push store
        @b amber pull store

        # then
        @rclone assert_exists "--23notes.txt" "hash"
        @rclone assert_exists "--3bsemi.txt" "semicolon"
        @rclone assert_exists " leading space.txt" "leading space"
        @rclone assert_exists "-trailing space.txt-20" "trailing space"
        @rclone assert_exists "--23archive/--3bold.txt" "in folder"
        assert_equal a b
    "##;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_rclone_fsck() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "Übersicht/größe.txt" "Umlaute"
        @a write_file "Übersicht/übrig.txt" "bleibt"
        @a amber add

        @a amber remote add store rclone :local:/$ROOT/rclone
        @a amber push store

        # action: a clean store checks out
        @a amber fsck store
        @a amber missing store
        assert_output_contains "no files missing"

        # action: a file lost from the store is found missing
        @rclone remove_file "--c3-9cbersicht/-gr-c3-b6-c3-9fe.txt"
        @a amber fsck store

        # then
        @a amber missing store
        assert_output_contains "missing Übersicht/größe.txt"

        # and pushing again restores it
        @a amber push store
        @rclone assert_exists "--c3-9cbersicht/-gr-c3-b6-c3-9fe.txt" "Umlaute"
    "#;
    dsl_definition::run_dsl_script(script).await
}

/// A store keeps each file where it was uploaded. Renaming the file later
/// must not stop others from pulling it: they fetch it from where the store
/// recorded it, not from where its current name would put it.
#[tokio::test(flavor = "multi_thread")]
async fn integration_test_rclone_pull_after_rename() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "Grüße/x.txt" "renamed"
        @a write_file "plain.txt" "also renamed"
        @a amber add
        @a amber remote add store rclone :local:/$ROOT/rclone
        @a amber push store

        @b amber init b
        @b amber remote add a local $ROOT/a
        @b amber remote add store rclone :local:/$ROOT/rclone

        @a amber mv "Grüße/x.txt" "Straße/y.txt"
        @a amber mv plain.txt other.txt
        @b amber sync a

        # action
        @b amber pull store

        # then
        @b assert_exists "Straße/y.txt" "renamed"
        @b assert_exists other.txt "also renamed"
        @rclone assert_exists "-Gr-c3-bc-c3-9fe/x.txt" "renamed"
        @rclone assert_exists plain.txt "also renamed"
        @rclone assert_does_not_exist other.txt
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_ssh_pull() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "Übersicht/größe.txt" "Umlaute über ssh"
        @a write_file "日本語/ファイル.txt" "CJK über ssh"
        @a amber add
        @a start_ssh 45675 hunter2

        @b amber init b
        @b amber remote add a-ssh ssh "user:hunter2@localhost:45675/"

        # action
        @b amber pull a-ssh

        # then
        @b assert_exists "Übersicht/größe.txt" "Umlaute über ssh"
        @b assert_exists "日本語/ファイル.txt" "CJK über ssh"

        @a end_ssh
    "#;
    dsl_definition::run_dsl_script(script).await
}

#[tokio::test(flavor = "multi_thread")]
async fn integration_test_utf8_ssh_push() -> Result<(), anyhow::Error> {
    let script = r#"
        # when
        @a amber init a
        @a write_file "Übersicht/größe.txt" "Umlaute über ssh"
        @a write_file "😀/🎉 party.txt" "Emoji über ssh"
        @a amber add

        @b amber init b
        @b start_ssh 45676 hunter2

        @a amber remote add b-ssh ssh "user:hunter2@localhost:45676/"

        # action
        @a amber push b-ssh

        @b end_ssh
        @b amber sync

        # then
        @b assert_exists "Übersicht/größe.txt" "Umlaute über ssh"
        @b assert_exists "😀/🎉 party.txt" "Emoji über ssh"
    "#;
    dsl_definition::run_dsl_script(script).await
}
