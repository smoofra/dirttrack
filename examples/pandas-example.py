#!/usr/bin/env python

import argparse
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet_dir", metavar="DIR")
    args = parser.parse_args()

    files = list(Path(args.parquet_dir).rglob("*.parquet"))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    filtered = df[df["eventSource"] == "ec2.amazonaws.com"]
    print(filtered[["eventTime", "eventName"]].to_string(index=False))


if __name__ == "__main__":
    main()
