import os

import s3fs

from dbt_server.logging import DBT_SERVER_LOGGER as logger

BUCKET = f'dp-all-trp-{os.environ["TRP_AWS_ENVIRONMENT"].lower()}-dbt-server'


def create_filesystem() -> s3fs.S3FileSystem:
    logger.info("Creating S3FileSystem")
    endpoint_url = os.getenv("AWS_S3_ENDPOINT_URL")
    if endpoint_url is None:
        return s3fs.S3FileSystem()
    else:
        return s3fs.S3FileSystem(endpoint_url=endpoint_url, client_kwargs={'endpoint_url': endpoint_url})


def put_cache() -> None:
    fs = create_filesystem()
    logger.info("Uploading project state to s3 cache")
    if fs.exists(f"/{BUCKET}/working-dir/"):
        fs.rm(f"/{BUCKET}/working-dir/", recursive=True)
    fs.put("./working-dir", f"/{BUCKET}/", recursive=True)


def get_cache() -> None:
    fs = create_filesystem()
    logger.info("Retrieving project state from s3 cache")
    fs.get(f"s3://{BUCKET}", "./", recursive=True)
