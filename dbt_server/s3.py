import os

import s3fs

from dbt_server.services import filesystem_service

BUCKET = "dbt-server"


def create_filesystem() -> s3fs.S3FileSystem:
    endpoint_url = os.getenv("AWS_S3_ENDPOINT_URL")
    if endpoint_url is None:
        return s3fs.S3FileSystem()
    else:
        return s3fs.S3FileSystem(endpoint_url=endpoint_url, client_kwargs={'endpoint_url': endpoint_url})


def put_cache() -> None:
    fs = create_filesystem()
    fs.put(filesystem_service.DEFAULT_WORKING_DIR, f"/{BUCKET}/", recursive=True)


def get_cache() -> None:
    fs = create_filesystem()
    fs.get(f"s3://{BUCKET}", "./", recursive=True)
