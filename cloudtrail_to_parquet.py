#!/usr/bin/env python3

import argparse
import gzip
import json
import os
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq


def flatten(rec: dict) -> dict:
    out: dict = {}
    for key, val in rec.items():
        if isinstance(val, (dict, list)):
            out[key] = json.dumps(val)
        else:
            out[key] = val
    return out


def read_file(path: Path) -> list[dict]:
    """Return flattened records from a CloudTrail .json or .json.gz file."""
    if path.suffix == ".gz":
        fh = gzip.open(path, "rt", encoding="utf-8")
    else:
        fh = open(path, "rt")
    with fh:
        data = json.load(fh)
    return [flatten(r) for r in data["Records"]]


def iter_files(root: Path | str) -> Iterator[Path]:
    """Yield every .json and .json.gz path under root, sorted for determinism."""
    if not os.path.exists(root):
        raise FileNotFoundError(root)
    for dirpath, _, names in sorted(os.walk(root), key=lambda x: x[0]):
        for name in sorted(names):
            if name.endswith(".json") or name.endswith(".json.gz"):
                yield Path(dirpath) / name


class Main:

    def __init__(self) -> None:
        self.index: int = 0
        self.records = pa.Table.from_pylist([])

    def flush(self):
        if len(self.records) == 0:
            return
        path = self.out_dir / f"{self.index}.parquet"
        self.index += 1
        with pq.ParquetWriter(path, self.records.schema, compression="zstd") as w:
            w.write_table(self.records)
            self.records = pa.Table.from_pylist([])

    def main(self) -> None:
        parser = argparse.ArgumentParser(
            description="Pack CloudTrail json logs into Parquet files."
        )
        parser.add_argument("in_dir", help="Root directory of CloudTrail log files")
        parser.add_argument("out_dir", help="Output directory for Parquet files")
        parser.add_argument(
            "--overwrite", action="store_true", help="rm OUT_DIR/*.parquet"
        )
        parser.add_argument(
            "--records-per-file", "-n", type=int, metavar="N", default=1_000_000
        )
        args = parser.parse_args()

        self.out_dir = Path(args.out_dir)
        self.records_per_file = args.records_per_file

        if existing := list(self.out_dir.glob("*.parquet")):
            if args.overwrite:
                for file in existing:
                    file.unlink()
            else:
                raise Exception(f"{self.out_dir} is not empty")

        for path in iter_files(args.in_dir):
            self.records = pa.concat_tables(
                [self.records, pa.Table.from_pylist(read_file(path))],
                promote_options="default",
            )
            if len(self.records) > self.records_per_file:
                self.flush()

        self.flush()


def main():
    Main().main()


if __name__ == "__main__":
    main()
