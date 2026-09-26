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

        # then: the store holds the files under their real names ...
        @rclone assert_exists "Übersicht/größe.txt" "Umlaute im store"
        @rclone assert_exists "日本語/ファイル.txt" "CJK im store"
        @rclone assert_exists "😀/🎉 party.txt" "Emoji im store"

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
        @rclone assert_exists "{nfc}" "composed"
        @rclone assert_exists "{nfd}" "decomposed"
        @c assert_exists "{nfc}" "composed"
        @c assert_exists "{nfd}" "decomposed"
        assert_equal a b
        assert_equal a c
    "#
    );
    dsl_definition::run_dsl_script(&script).await
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
