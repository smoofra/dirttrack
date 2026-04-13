#!/usr/bin/env python

import argparse
import duckdb

# Uses DuckDB to read parquet files and export to SQLite.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet_dir", metavar="PQ_DIR")
    parser.add_argument(
        "sqlite_db", metavar="DATABASE", help="output SQLite file, e.g. output.db"
    )
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{args.sqlite_db}' AS db (TYPE sqlite);")
    con.execute(
        "CREATE TABLE db.events AS" " SELECT * FROM read_parquet(?)",
        [f"{args.parquet_dir}/**/*.parquet"],
    )
    print(f"Loaded into {args.sqlite_db} as table 'events'")


if __name__ == "__main__":
    main()
