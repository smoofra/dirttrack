#!/bin/sh

if ! [ $# = 1 ]; then
  echo "usage ./$0 DIR"
  exit 1
fi

parquet_dir=`realpath $1`

duckdb -c "SELECT eventSource,eventName FROM read_parquet('$parquet_dir/**/*.parquet');"