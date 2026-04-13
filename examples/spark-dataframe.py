#!/usr/bin/env python

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("parquet_dir", metavar="DIR")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("example")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )

    df = spark.read.option("recursiveFileLookup", "true").parquet(args.parquet_dir)
    filtered = df.filter((col("eventSource") == "ec2.amazonaws.com"))
    filtered.select("eventTime", "eventName").show()


if __name__ == "__main__":
    main()
