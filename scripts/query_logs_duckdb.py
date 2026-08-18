"""DuckDB analytical query script for querying batched S3 audit logs."""

from __future__ import annotations

import os

SQL_QUERY = """
INSTALL httpfs;
LOAD httpfs;

-- Configure S3 endpoint for RustFS
SET s3_endpoint='127.0.0.1:9000';
SET s3_access_key_id='rustfsadmin';
SET s3_secret_access_key='rustfsadmin123';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Query all compressed jsonl.gz batches across all dates
SELECT 
    event,
    job_type,
    count(*) AS total_events,
    avg(duration_ms) AS avg_duration_ms,
    max(duration_ms) AS max_duration_ms,
    min(duration_ms) AS min_duration_ms
FROM read_ndjson('s3://processing-logs/logs/*/*/*.jsonl.gz')
GROUP BY event, job_type
ORDER BY total_events DESC;
"""


def main():
    print("DuckDB S3 Audit Log Analytics Script")
    print("=" * 45)
    print("To execute this query directly with duckdb CLI:")
    print("  duckdb -c \"" + SQL_QUERY.replace("\n", " ") + "\"")
    print("\nOr in Python:")
    print("""
import duckdb
con = duckdb.connect()
con.execute(\"\"\"%s\"\"\")
df = con.df()
print(df)
""" % SQL_QUERY)


if __name__ == "__main__":
    main()
