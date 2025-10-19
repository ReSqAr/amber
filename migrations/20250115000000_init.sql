-- table: current_repository
CREATE TABLE current_repository
(
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id TEXT NOT NULL
);

-- table: repositories
CREATE TABLE repositories
(
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id         TEXT UNIQUE NOT NULL,
    last_file_index INTEGER     NOT NULL DEFAULT -1,
    last_blob_index INTEGER     NOT NULL DEFAULT -1,
    last_name_index INTEGER     NOT NULL DEFAULT -1
);

-- table: repository_names
CREATE TABLE repository_names
(
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid        TEXT UNIQUE NOT NULL,
    repo_id     TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    valid_from  DATETIME    NOT NULL
);

-- table: connections
CREATE TABLE connections
(
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT UNIQUE NOT NULL,
    connection_type  TEXT        NOT NULL,
    parameter        TEXT        NOT NULL
);

-- table: transfers
CREATE TABLE transfers
(
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    transfer_id      INTEGER NOT NULL,
    blob_id          TEXT    NOT NULL,
    blob_size        INTEGER NOT NULL,
    path             TEXT    NOT NULL
);