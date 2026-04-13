#!/usr/bin/env python

import argparse
from pathlib import Path
import json

import pandas as pd

# this will load everything into memory!


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet_dir", metavar="DIR")
    args = parser.parse_args()

    files = list(Path(args.parquet_dir).rglob("*.parquet"))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    # parse json
    df["tls"] = df["tlsDetails"].map(
        lambda x: json.loads(x)["tlsVersion"] if x else None
    )

    filtered = df[df["eventSource"] == "ec2.amazonaws.com"]

    print(filtered.sample(10)[["eventTime", "eventName", "tls"]])


if __name__ == "__main__":
    main()
