# ct_helpers.py

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

# Used by ct_bundled_posts_to_minio and ct_post_details_to_minio
def get_request_params():
	# define request_headers and ct_key from environment
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
    }

    ct_key = os.environ.get("CT_KEY")
    
    return request_headers, ct_key


# Used by ct_bundled_posts_to_minio and ct_post_details_to_minio
def create_minio_client():
    # Take values of MinIO variables from environment
    minio_host = os.environ.get("MINIO_HOST")
    minio_access = os.environ.get("MINIO_ACCESS")
    minio_secret = os.environ.get("MINIO_SECRET")

    # Create MinIO client with access and secret key.
    # Currently using local network without SSL access so secure=False
    return Minio(minio_host, minio_access, minio_secret, secure=False)


def create_minio_tags():
	# Prepare to tag posts to keep from processing objects more than once
    tags = Tags.new_object_tags()
    tags["processed"] = "true"
    
    return tags


def get_minio_object_names(minio_client, bucket):
    """
    Get all object names in the MinIO bucket.
    """
    return [obj.object_name for obj in minio_client.list_objects(bucket)]
   
    
def get_minio_response_js(object_name, bucket, client):
    """
    Process a MinIO object.
    """
    tagged = client.get_object_tags(bucket, object_name)
    if not tagged:
        try:
            minio_response = client.get_object(bucket, object_name)
            minio_response_js = json.loads(minio_response.data.decode())
        except S3Error as e:
            print(f"S3 Error getting object:{e}")
            sys.exit(1)
        finally:
            minio_response.close()
            minio_response.release_conn()

        return minio_response_js
    return None


# Used by ct_posts_to_minio and ct_post_details_to_minio
def check_minio_buckets(client, buckets):
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

# combine please


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

# Used by ct_bundled_posts_to_minio and ct_post_details_to_minio
def isoformat_to_seconds(*datetimes):
    if len(datetimes) == 1 and isinstance(datetimes[0], datetime):
        # If only one argument is passed and it is a datetime object, convert it to isoformat
        formatted_datetime = datetimes[0].isoformat(timespec="seconds")
        return formatted_datetime
    else:
        # If multiple arguments or a non-datetime object is passed, convert all to isoformat
        formatted_datetimes = [dt.isoformat(timespec="seconds") for dt in datetimes]
        return formatted_datetimes


# Used by ct_bundled_posts_to_minio and ct_post_details_to_minio
def minio_put_text(minio_client, bucket, obj_name, data_in_bytes, data_length):
    try:
        minio_client.put_object(
            bucket_name=bucket,
            object_name=obj_name,
            data=data_in_bytes,
            length=data_length,
            content_type="text/plain",
        )
    except S3Error as e:
        print(f"S3 Error putting object: {e}")
        sys.exit(1)


# Used by ct_posts_to_minio and ct_post_details_to_minio
def request_with_backoff(url, headers=None):
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))

    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Requests error after retrying.  Error: {e}")
        return None


def get_db_credentials():
    return (
        os.environ.get("PGUSER"),
        os.environ.get("PGPASSWD"),
        os.environ.get("PGHOST"),
        os.environ.get("PGPORT"),
    )


def load_db_credentials():
    return (
        os.environ.get("PGUSER"),
        os.environ.get("PGPASSWD"),
        os.environ.get("PGHOST"),
        os.environ.get("PGPORT"),
        'ct',
    )


def create_sqlalchemy_engine():
    """
    

    """
    pguser, pgpasswd, pghost, pgport, pgdb = load_db_credentials()
    engine_string = f"postgresql://{pguser}:{pgpasswd}@{pghost}:{pgport}/{pgdb}"
    return create_engine(engine_string)














































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

