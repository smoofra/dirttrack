#!/usr/bin/env python3

import argparse
import gzip
import json
import multiprocessing
import multiprocessing.pool
import os
import threading
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from rich.progress import Progress, SpinnerColumn, TextColumn, TaskID


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


def iter_subdirs(root: Path | str) -> Iterator[Path]:
    if not os.path.exists(root):
        raise FileNotFoundError(root)
    for dirpath, _, _ in os.walk(root):
        yield Path(dirpath)


class Job:

    def __init__(
        self,
        in_dir: Path,
        out_dir: Path,
        records_per_file: int,
        queue,
    ):
        self.in_dir = in_dir
        self.out_dir = out_dir
        self.records_per_file = records_per_file
        self.queue = queue
        self.index: int = 0
        self.records = pa.Table.from_pylist([])

    def flush(self):
        if len(self.records) == 0:
            return
        os.makedirs(self.out_dir, exist_ok=True)
        path = self.out_dir / f"{self.index}.parquet"
        self.index += 1
        with pq.ParquetWriter(path, self.records.schema, compression="zstd") as w:
            w.write_table(self.records)
        self.records = pa.Table.from_pylist([])

    def run(self):
        for file in sorted(self.in_dir.iterdir()):
            if not file.is_file() or not (
                file.name.endswith(".json") or file.name.endswith(".json.gz")
            ):
                continue
            records = read_file(file)
            self.queue.put((1, len(records), str(file)))
            self.records = pa.concat_tables(
                [self.records, pa.Table.from_pylist(records)],
                promote_options="default",
            )
            if len(self.records) > self.records_per_file:
                self.flush()
        self.flush()


def update_progress(queue: multiprocessing.Queue, progress: Progress, task_id: TaskID):
    files_total = 0
    records_total = 0
    while True:
        item = queue.get()
        if item is None:
            break
        files_delta, records_delta, current_file = item
        files_total += files_delta
        records_total += records_delta
        progress.update(
            task_id,
            files=files_total,
            records=records_total,
            description=current_file,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pack CloudTrail json logs into Parquet files."
    )
    parser.add_argument("in_dir", help="Root directory of CloudTrail log files")
    parser.add_argument("out_dir", help="Output directory for Parquet files")
    parser.add_argument(
        "--overwrite", action="store_true", help="rm OUT_DIR/**/*.parquet"
    )
    parser.add_argument(
        "--records-per-file", "-n", type=int, metavar="N", default=1_000_000
    )
    parser.add_argument(
        "--workers",
        "-j",
        type=int,
        default=os.cpu_count(),
        help="worker processes (default: cpu count)",
    )
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)

    for existing in out_dir.glob("**/*.parquet"):
        if args.overwrite:
            existing.unlink()
        else:
            raise Exception(f"{out_dir} has .parquet files already")

    with (
        multiprocessing.Manager() as manager,
        multiprocessing.pool.Pool(processes=args.workers) as pool,
        Progress(
            SpinnerColumn(),
            TextColumn(
                "[green]{task.fields[files]}[/] files  [cyan]{task.fields[records]:,}[/] records"
            ),
            TextColumn("[dim]{task.description}[/]"),
        ) as progress,
    ):

        queue = manager.Queue()
        task_id = progress.add_task("", files=0, records=0, total=None)

        progress_thread = threading.Thread(
            target=update_progress, daemon=True, args=(queue, progress, task_id)
        )
        progress_thread.start()

        jobs = (
            Job(path, out_dir / path.relative_to(in_dir), args.records_per_file, queue)
            for path in iter_subdirs(in_dir)
        )
        for _ in pool.imap_unordered(Job.run, jobs):
            pass

        queue.put(None)
        progress_thread.join()


if __name__ == "__main__":
    main()
