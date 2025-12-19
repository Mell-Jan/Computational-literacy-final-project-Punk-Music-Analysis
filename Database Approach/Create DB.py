import duckdb
from pathlib import Path

#path to db
con = duckdb.connect(r"musicbrainz.duckdb")

# Import the lyrics data
def import_lyrics_data(con, path_lyrics):

    con.execute(f"""
    CREATE OR REPLACE TABLE lyrics_with_lyrics AS
    SELECT title, artist, year, lyrics, id FROM read_csv_auto(
      '{path_lyrics}',
      header=True,
      quote='"',
      escape='"',
      ignore_errors=true
    );
    """)

# Import Json file with profanity words
def import_profanity_words_with_score(con, path):
    con.execute(rf"""
    CREATE OR REPLACE TABLE profanity_words_with_info AS
    SELECT
      id,
      pattern,
      severity,
      tags,
      allow_partial_default AS allow_partial,
      exceptions
    FROM (
      SELECT
        id,
        "match" AS pattern,
        CAST(severity AS INTEGER) AS severity,
        tags,
        COALESCE(allow_partial, TRUE) AS allow_partial_default,
        exceptions
      FROM read_json(
        '{path}',
        columns = {{
          'id': 'VARCHAR',
          'match': 'VARCHAR',
          'severity': 'INTEGER',
          'tags': 'VARCHAR[]',
          'allow_partial': 'BOOLEAN',
          'exceptions': 'VARCHAR[]'
        }}
      )
    );
    """)

# If something goes wrong this will drop all the tables
def drop_tables(con):
    required = [
        "tag",
        "artist_tag",
        "recording_tag",
        "artist_credit_name",
        "track",
        "medium",
        "release_country",
        "release_unknown_country",
        "recording_meta",
        "genre",
    ]
    for table in required:
        con.execute(f"""
        DROP TABLE IF EXISTS {table};
        """)


# Adds all the table from the data dumps provided by music Brainz
def add_missing_musicbrainz_tables(con, mbdump_dir):

    mbdump_dir = Path(mbdump_dir)

    required = [
        "tag",
        "artist_tag",
        "recording_tag",
        "artist_credit_name",
        "track",
        "medium",
        "release_country",
        "release_unknown_country",
        "recording_meta",
        "genre",
    ]

    # What tables do we already have?
    existing = {row[0] for row in con.execute("SHOW TABLES").fetchall()}

    created = []
    skipped_missing_file = []
    skipped_already_exists = []

    for table in required:
        if table in existing:
            skipped_already_exists.append(table)
            continue

        dump_file = mbdump_dir / table
        if not dump_file.exists():
            skipped_missing_file.append(table)
            continue

        # DuckDB likes forward slashes in paths (works on Windows too)
        file_path = dump_file.as_posix()

        con.execute(f"""
            CREATE TABLE {table} AS
            SELECT *
            FROM read_csv(
                '{file_path}',
                delim='\\t',
                header=False,
                nullstr='\\N', -- Fix issue where NULL strings become BIGINT for some reason
                all_varchar=1
            );
        """)
        created.append(table)

    return {
        "created": created,
        "already_existed": skipped_already_exists,
        "missing_dump_file": skipped_missing_file,
    }

drop_tables(con)

result = add_missing_musicbrainz_tables(
     con,
    r"mbdump"
)

print(result)

import_lyrics_data(con, r"song_lyrics.csv")
import_profanity_words_with_score(con,r"profanity_with_info.json" )