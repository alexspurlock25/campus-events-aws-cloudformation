import sys
import logging
from dataclasses import dataclass
from awsglue.utils import getResolvedOptions


@dataclass
class Args:
    """
    Data type for args coming from the glue job
    """

    job_name: str
    source_bucket_name: str
    target_bucket_name: str


_args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_BUCKET_NAME",
        "TARGET_BUCKET_NAME",
    ],
)

args = Args(
    job_name=_args["JOB_NAME"],
    source_bucket_name=_args["SOURCE_BUCKET_NAME"],
    target_bucket_name=_args["TARGET_BUCKET_NAME"],
)

print("print: Job job started!")
print(f"print: job args: {args}")
