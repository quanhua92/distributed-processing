"""DuckDB analytical query script for querying batched S3 audit logs."""

from __future__ import annotations

import argparse


def get_init_sql(endpoint: str = "127.0.0.1:19000") -> str:
    return f"""
INSTALL httpfs;
LOAD httpfs;

-- Configure S3 endpoint for RustFS
SET s3_endpoint='{endpoint}';
SET s3_access_key_id='rustfsadmin';
SET s3_secret_access_key='rustfsadmin123';
SET s3_use_ssl=false;
SET s3_url_style='path';
"""


QUERY_SQL = """
SELECT 
    event,
    job_type,
    count(*) AS total_events,
    ROUND(avg(duration_ms)::numeric, 1) AS avg_duration_ms,
    max(duration_ms) AS max_duration_ms,
    min(duration_ms) AS min_duration_ms
FROM read_ndjson('s3://processing-logs/logs/*/*/*/*.jsonl.gz')
GROUP BY event, job_type
ORDER BY total_events DESC;
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Query S3 audit logs using DuckDB")
    parser.add_argument("--endpoint", default="127.0.0.1:19000", help="S3 endpoint (default: 127.0.0.1:19000)")
    parser.add_argument("--run", action="store_true", help="Execute query directly via python duckdb module")
    args = parser.parse_args()

    if args.run:
        try:
            import duckdb
            con = duckdb.connect()
            print("Connecting to RustFS S3 log archive and executing analytical query...")
            print("=" * 70)
            con.sql(get_init_sql(args.endpoint))
            con.sql(QUERY_SQL).show()
            return
        except ImportError:
            print("duckdb not installed in current environment. Install via: pip install duckdb or uv run --with duckdb python scripts/query_logs_duckdb.py --run")
            return
        except Exception as e:
            print(f"Query error: {e}")
            return

    print("DuckDB S3 Audit Log Analytics Query:")
    print("=" * 70)
    print(get_init_sql(args.endpoint) + QUERY_SQL)


if __name__ == "__main__":
    main()
