#!/usr/bin/env python3

from . import common


class Job(common.Job):

    @classmethod
    def get_records(cls, data: list | dict) -> list[dict]:
        assert isinstance(data, dict)
        return data["Records"]


class Main(common.Main):
    job_class = Job
    description = "Pack CloudTrail json logs into Parquet files."


main = Main.main

if __name__ == "__main__":
    main()
