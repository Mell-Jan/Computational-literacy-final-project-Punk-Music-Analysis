import duckdb
import matplotlib.pyplot as plt

#path to db
con = duckdb.connect(r"musicbrainz.duckdb")

def visualize_avg_profanity(con):
    df = con.execute("""
    SELECT release_year, AVG(total_profanity_count) AS avg_prof,
    FROM top_100_with_info_eng p
    GROUP BY release_year
    ORDER BY release_year;
    """).df()

    df.plot(x="release_year", y=["avg_prof"])
    plt.show()

def visualize_avg_severity(con):
    df = con.execute("""
        SELECT release_year,
                     AVG(severity_avg) AS avg_severity
        FROM top_100_with_info_eng p
        GROUP BY release_year
        ORDER BY release_year;
        """).df()

    df.plot(x="release_year", y=["avg_severity"])
    plt.show()

def visualize_tags_by_year(con):
    df = con.execute("""
    SELECT release_year, tag, songs_with_tag
    FROM profanity_tag_counts_by_year
    WHERE release_year BETWEEN 1976 AND 2020
    ORDER BY release_year, tag;
    """).df()

    # ensure tag is string (extra safety)
    df["tag"] = df["tag"].astype(str)

    pivot = df.pivot(index="release_year", columns="tag", values="songs_with_tag").fillna(0)
    pivot.plot(figsize=(12, 6))
    plt.show()

def visualize_avg_severity_by_wordcount(con):
    df = con.execute("""
        SELECT release_year,
               avg_density
        FROM profanity_density_by_year p
        ORDER BY release_year;
        """).df()

    df.plot(x="release_year", y=["avg_density"])
    plt.show()

visualize_avg_severity(con)
visualize_avg_profanity(con)
visualize_tags_by_year(con)
visualize_avg_severity_by_wordcount(con)