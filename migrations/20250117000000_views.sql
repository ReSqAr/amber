-- view: latest_repository_names
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