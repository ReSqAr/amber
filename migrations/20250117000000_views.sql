-- view: latest_filesystem_files
CREATE VIEW latest_filesystem_files AS
WITH versioned_files AS (
    SELECT
        path,
        blob_id,
        valid_from,
        ROW_NUMBER() OVER (
            PARTITION BY path
            ORDER BY valid_from DESC, blob_id DESC
        ) AS rn
    FROM files
),
latest_file_version AS (
    SELECT
        path,
        blob_id
    FROM versioned_files
    WHERE rn = 1
)
SELECT
    path,
    blob_id
FROM latest_file_version
WHERE blob_id IS NOT NULL;

-- view: latest_available_blobs
CREATE VIEW latest_available_blobs AS
WITH versioned_blobs AS (
    SELECT
        repo_id,
        blob_id,
        blob_size,
        valid_from,
        has_blob,
        ROW_NUMBER() OVER (
            PARTITION BY repo_id, blob_id
            ORDER BY valid_from DESC, has_blob DESC
        ) AS rn
    FROM blobs
),
latest_blob_version AS (
    SELECT
        repo_id,
        blob_id,
        blob_size,
        has_blob
    FROM versioned_blobs
    WHERE rn = 1
)
SELECT
    repo_id,
    blob_id,
    blob_size
FROM latest_blob_version
WHERE has_blob = 1;

-- view: latest_filesystem_files
CREATE VIEW latest_repository_names AS
WITH versioned_repository_names AS (
    SELECT
        repo_id,
        name,
        valid_from,
        ROW_NUMBER() OVER (
            PARTITION BY repo_id
            ORDER BY valid_from DESC, name DESC
            ) AS rn
    FROM repository_names
),
     latest_repository_names_version AS (
         SELECT
             repo_id,
             name
         FROM versioned_repository_names
         WHERE rn = 1
     )
SELECT
    repo_id,
    name
FROM latest_repository_names_version;

-- view: latest_materialisations
CREATE VIEW latest_materialisations AS
WITH versioned_materialisations AS (
    SELECT
        path,
        blob_id,
        valid_from,
        ROW_NUMBER() OVER (
            PARTITION BY path
            ORDER BY valid_from DESC, blob_id DESC
            ) AS rn
    FROM materialisations
),
     latest_materialisations_version AS (
         SELECT
             path,
             blob_id
         FROM versioned_materialisations
         WHERE rn = 1
     )
SELECT
    path,
    blob_id
FROM latest_materialisations_version;