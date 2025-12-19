import duckdb
#path to db
con = duckdb.connect(r"musicbrainz.duckdb")

# Drops the views if needed
def drop_all_views():
    con.execute("""
    DROP VIEW IF EXISTS recordings_with_rating;
    DROP VIEW IF EXISTS recording_meta_view;
    DROP VIEW IF EXISTS release_unknown_country_view;
    DROP VIEW IF EXISTS release_country_view;
    DROP VIEW IF EXISTS medium_view;
    DROP VIEW IF EXISTS track_view;
    DROP VIEW IF EXISTS artist_credit_name_view;
    DROP VIEW IF EXISTS recording_tag_view;
    DROP VIEW IF EXISTS tag_view;
    DROP VIEW IF EXISTS recording_view;
    DROP VIEW IF EXISTS area_view;
    DROP VIEW IF EXISTS artist_view;
    """)

# Names the columns to the corresponding values
def create_all_views_fix():
    # Some Columns are created as column00 and others as column0 so that's cool
    con.execute("""
    CREATE VIEW IF NOT EXISTS artist_view AS
    SELECT
        CAST(column00 AS BIGINT) AS id,
        column01                AS gid,
        column02                AS name,
        column03                AS sort_name,
        CAST(NULLIF(column04,'\\N') AS INTEGER) AS begin_date_year,
        CAST(NULLIF(column05,'\\N') AS INTEGER) AS begin_date_month,
        CAST(NULLIF(column06,'\\N') AS INTEGER) AS begin_date_day,
        CAST(NULLIF(column07,'\\N') AS INTEGER) AS end_date_year,
        CAST(NULLIF(column08,'\\N') AS INTEGER) AS end_date_month,
        CAST(NULLIF(column09,'\\N') AS INTEGER) AS end_date_day,
        CAST(NULLIF(column10,'\\N') AS BIGINT)  AS type,
        CAST(NULLIF(column11,'\\N') AS BIGINT)  AS area
    FROM artist;

    CREATE VIEW IF NOT EXISTS area_view AS
    SELECT
        CAST(column00 AS BIGINT) AS id,
        column01                 AS gid,
        column02                 AS name,
        column03                 AS sort_name,
        CAST(NULLIF(column04,'\\N') AS BIGINT) AS type
    FROM area;

    CREATE VIEW IF NOT EXISTS tag_view AS
    SELECT
        CAST(column0 AS BIGINT) AS id,
        column1                 AS name
    FROM tag;

    CREATE VIEW IF NOT EXISTS recording_view AS
    SELECT
        CAST(column0 AS BIGINT) AS id,
        column1                 AS gid,
        column2                 AS name,
        CAST(column3 AS BIGINT) AS artist_credit,
        CAST(NULLIF(column4,'\\N') AS BIGINT) AS length
    FROM recording;

    CREATE VIEW IF NOT EXISTS recording_tag_view AS
    SELECT
        CAST(column0 AS BIGINT) AS recording,
        CAST(column1 AS BIGINT) AS tag,
        CAST(NULLIF(column2,'\\N') AS BIGINT) AS count
    FROM recording_tag;

    CREATE VIEW IF NOT EXISTS artist_credit_name_view AS
    SELECT
        CAST(column0 AS BIGINT) AS artist_credit,
        CAST(column1 AS INTEGER) AS position,
        CAST(column2 AS BIGINT) AS artist,
        column3                 AS name,
        column4                 AS join_phrase
    FROM artist_credit_name;

    -- =========================
    -- Track
    -- =========================
    CREATE VIEW IF NOT EXISTS track_view AS
    SELECT
        CAST(column00 AS BIGINT) AS id,
        column01                 AS gid,
        CAST(column02 AS BIGINT) AS recording,
        CAST(column03 AS BIGINT) AS medium,
        CAST(NULLIF(column04,'\\N') AS INTEGER) AS position,
        column05                 AS name
    FROM track;

    -- =========================
    -- Medium
    -- =========================
    CREATE VIEW IF NOT EXISTS medium_view AS
    SELECT
        CAST(column0 AS BIGINT) AS id,
        CAST(column1 AS BIGINT) AS release,
        CAST(NULLIF(column2,'\\N') AS INTEGER) AS position
    FROM medium;

    -- =========================
    -- Release Country
    -- =========================
    CREATE VIEW IF NOT EXISTS release_country_view AS
    SELECT
        CAST(column0 AS BIGINT) AS release,
        CAST(column1 AS BIGINT) AS country,
        CAST(NULLIF(column2,'\\N') AS INTEGER) AS date_year,
        CAST(NULLIF(column3,'\\N') AS INTEGER) AS date_month,
        CAST(NULLIF(column4,'\\N') AS INTEGER) AS date_day
    FROM release_country;

    -- =========================
    -- Release Unknown Country
    -- =========================
    CREATE VIEW IF NOT EXISTS release_unknown_country_view AS
    SELECT
        CAST(column0 AS BIGINT) AS release,
        CAST(NULLIF(column1,'\\N') AS INTEGER) AS date_year,
        CAST(NULLIF(column2,'\\N') AS INTEGER) AS date_month,
        CAST(NULLIF(column3,'\\N') AS INTEGER) AS date_day
    FROM release_unknown_country;

    -- =========================
    -- Recording Meta
    -- =========================
    CREATE VIEW IF NOT EXISTS recording_meta_view AS
    SELECT
        CAST(column0 AS BIGINT) AS id,
        CAST(NULLIF(column1,'\\N') AS DOUBLE) AS rating,
        CAST(NULLIF(column2,'\\N') AS BIGINT) AS rating_count
    FROM recording_meta;
    """)

# Function that creates a view that reflects the earlier approach where I requested the data from the API
def create_view_record_ratings():
    con.execute("""
    Create or replace view recordings_with_rating as
    Select * from (
    WITH punk_recordings AS (
      SELECT r.id, r.gid, r.name, r.artist_credit
      FROM recording_view r
      JOIN recording_tag_view rt ON rt.recording = r.id
      JOIN tag_view t ON t.id = rt.tag
      WHERE t.name = 'punk'
    ),
    release_years AS (
      SELECT tr.recording, rc.date_year AS year
      FROM track_view tr
      JOIN medium_view m ON m.id = tr.medium
      JOIN release_country_view rc ON rc.release = m.release
      WHERE rc.date_year IS NOT NULL

      UNION ALL

      SELECT tr.recording, ruc.date_year
      FROM track_view tr
      JOIN medium_view m ON m.id = tr.medium
      JOIN release_unknown_country_view ruc ON ruc.release = m.release
      WHERE ruc.date_year IS NOT NULL
    ),
    first_release AS (
      SELECT recording, MIN(year) AS first_year
      FROM release_years
      GROUP BY recording
    )
    SELECT
      pr.gid AS recording_mbid,
      pr.name AS title,
      acn.name AS artist,
      ar.name AS area,
      fr.first_year AS release_year,
      rm.rating,
      rm.rating_count
    FROM punk_recordings pr
    JOIN first_release fr ON fr.recording = pr.id AND fr.first_year BETWEEN 1970 AND 2021
    LEFT JOIN artist_credit_name_view acn
           ON acn.artist_credit = pr.artist_credit AND acn.position = 0
    LEFT JOIN artist_view a ON a.id = acn.artist
    LEFT JOIN area_view ar ON ar.id = a.area
    LEFT JOIN recording_meta_view rm ON rm.id = pr.id
    ORDER BY rm.rating_count DESC NULLS LAST ); """)



drop_all_views()
create_view_record_ratings()