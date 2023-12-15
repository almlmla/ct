# ctetl.py

import io
import json
import os
import sys

from datetime import datetime, timedelta, timezone

from minio import Minio
from minio.commonconfig import Tags
from minio.error import S3Error

import pandas as pd

import psycopg2

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


from sqlalchemy import create_engine, text
from time import sleep


# Used by ct_posts_to_minio and ct_post_details_to_minio
def create_minio_client():
    # Take values of MinIO variables from environment
    minio_host = os.environ.get("MINIO_HOST")
    minio_access = os.environ.get("MINIO_ACCESS")
    minio_secret = os.environ.get("MINIO_SECRET")

    # Create MinIO client with access and secret key.
    # Currently using local network without SSL access so secure=False
    return Minio(minio_host, minio_access, minio_secret, secure=False)


# Used by ct_posts_to_minio and ct_post_details_to_minio
def request_with_backoff(url, headers=None):
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))

    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Requests error after retrying.  Error: {e}")
        return None


# Used by ct_posts_to_minio and ct_post_details_to_minio
def check_buckets(client, buckets):
    """
    Check if buckets (in a list) can be found.

    """
    for bucket in buckets:
        # Proceed only if the bucket exists
        found = client.bucket_exists(bucket)
        if not found:
            print(f"Cannot find bucket '{bucket}', exiting.")
            # Exit if any buckets not found
            sys.exit(1)


def get_db_credentials():
    return (
        os.environ.get("PGUSER"),
        os.environ.get("PGPASSWD"),
        os.environ.get("PGHOST"),
        os.environ.get("PGPORT"),
    )


def create_sqlalchemy_engine():
    """
    Hard-coded database name of 'ct'.

    """
    pguser, pgpasswd, pghost, pgport = get_db_credentials()
    engine_string = f"postgresql://{pguser}:{pgpasswd}@{pghost}:{pgport}/ct"
    return create_engine(engine_string)

