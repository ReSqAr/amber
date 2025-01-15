CREATE TABLE current_repository
(
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id TEXT NOT NULL
);

CREATE TABLE repositories
(
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id         TEXT UNIQUE NOT NULL,
    last_file_index INTEGER     NOT NULL DEFAULT -1,
    last_blob_index INTEGER     NOT NULL DEFAULT -1
);

CREATE TABLE files
(
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid       TEXT UNIQUE NOT NULL,
    path       TEXT        NOT NULL,
    object_id  TEXT,
    valid_from DATETIME    NOT NULL
);
CREATE TABLE blobs
(
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid        TEXT UNIQUE NOT NULL,
    repo_id     TEXT        NOT NULL,
    object_id   TEXT        NOT NULL,
    valid_from  DATETIME    NOT NULL,
    file_exists INTEGER     NOT NULL
);
