"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python quakes_flow.py
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\")) # "raw" or "sample"
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"bluesky{TAG}.duckdb"                     # bluesky_sample.duckdb for the sample
CHART = f"charts/reply_share_by_lang{TAG}.png"  # the sample never overwrites your chart


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of quakes.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]


@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_posts
            WHERE action <> 'malformed'
            AND kind = 'commit'      -- not identity or account events
            AND did IS NOT NULL AND rkey IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_posts WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}

@task
def entities():
    """Kind 2: one row per post (did, rkey), with its latest operation."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT did, rkey, operation, lang, text,
                   make_timestamp(time_us) AS event_time, -- microseconds -> UTC
                   (raw::JSON ->> '$.data.commit.record.reply.parent.uri')
                    IS NOT NULL AS is_reply -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY did, rkey
                                    ORDER BY time_us DESC, kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM posts").fetchone()[0]

@task 
def marts():
    """Share of new posts that are replies, for the 6 biggest languages."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_reply_share AS
            SELECT lang,
                   count(*) AS posts,
                   round(100.0 * avg(CASE WHEN is_reply THEN 1 ELSE 0 END), 1) AS pct_reply
            FROM posts
            WHERE operation = 'create' AND lang IS NOT NULL
            GROUP BY lang
            ORDER BY posts DESC
            LIMIT 6
        """)

@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("""
            SELECT lang, posts, pct_reply FROM mart_reply_share ORDER BY pct_reply DESC
        """).fetchall()
    os.makedirs("charts", exist_ok=True)
    langs = [r[0] for r in rows]
    pct = [r[2] for r in rows]
    plt.figure(figsize=(8, 4))
    plt.bar(langs, pct, color="#0C6CAC")
    plt.title("German posts are mostly replies; Japanese posts are mostly originals")
    plt.xlabel("post language (Bluesky langs code)")
    plt.ylabel("replies (% of new posts)")
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    plt.close()
    return CHART


@flow(name="bluesky-pipeline")
def pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    things = entities()
    marts()
    path = chart()
    log.info("Raw records landed: %d", raw)
    log.info("Malformed (kept in raw): %d", n["malformed"])
    log.info("Unique Kafka records: %d", n["records"])
    log.info("Unique posts: %d", things)
    log.info("Chart: %s", path)

if __name__ == "__main__":
    pipeline()
