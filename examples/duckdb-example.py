#!/usr/bin/env python

import argparse
import duckdb


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet_dir", metavar="DIR")
    args = parser.parse_args()

    con = duckdb.connect()
    result = con.execute(
        "SELECT eventSource, eventName, tlsDetails->>'$.tlsVersion' AS tlsVersion"
        " FROM read_parquet(?)"
        " LIMIT 10",
        [f"{args.parquet_dir}/**/*.parquet"],
    )
    print(result.df())


if __name__ == "__main__":
    main()
