# ct_helpers.py

import io
import json
import os
import sys
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine

from minio import Minio
from minio.commonconfig import Tags
from minio.error import S3Error


### Functions to get parameters from environment


def get_request_parameters():
    """ "
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    API key for accessing CrowdTangle must be saved as CT_KEY
    and defined in the environment.

    Load CT_KEY from environment and define REQUEST_HEADERS.
    """
    REQUEST_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
    }

    CT_KEY = os.environ.get("CT_KEY")
    if CT_KEY is None:
        print("CT_KEY is empty.  Is it defined in the environment?")
        sys.exit(1)
    else:
        return REQUEST_HEADERS, CT_KEY


def load_db_credentials():
    """
    Used by functions in ct_transform_and_load and ct_score_reports.

    Returns PostgreSQL credentials from the environment.

    """

    PGUSER = os.environ.get("PGUSER")
    PGPASSWD = os.environ.get("PGPASSWD")
    PGHOST = os.environ.get("PGHOST")
    PGPORT = os.environ.get("PGPORT")
    PGDB = "ct"

    if any(P is None for P in (PGUSER, PGPASSWD, PGHOST, PGPORT)):
        print(
            "At least one of the PostgreSQL parameters is empty.  Are they defined in the environment?"
        )
        sys.exit(1)
    else:
        return PGUSER, PGPASSWD, PGHOST, PGPORT, PGDB


### MinIO functions


def create_minio_client():
    """
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    Create MinIO client from access and secret key.

    """
    # Load values of MinIO parameters from environment
    MINIO_HOST = os.environ.get("MINIO_HOST")
    MINIO_ACCESS = os.environ.get("MINIO_ACCESS")
    MINIO_SECRET = os.environ.get("MINIO_SECRET")

    if any(P is None for P in (MINIO_HOST, MINIO_ACCESS, MINIO_SECRET)):
        print(
            "At least one of the MinIO parameters is empty.  Are they defined in the environment?"
        )
        sys.exit(1)
    else:
        # Modify 'secure' to your configuration
        return Minio(MINIO_HOST, MINIO_ACCESS, MINIO_SECRET, secure=False)


def check_minio_buckets(minio_client, *buckets):
    """
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    Find buckets otherwise exit. Buckets entered as individual arguments.

    """
    for bucket in buckets:
        # Proceed only if the bucket exists
        found = minio_client.bucket_exists(bucket)
        if not found:
            print(f"Cannot find bucket '{bucket}', exiting.")
            # Exit if any bucket not found
            sys.exit(1)


def create_minio_tags():
    """
    Used by ct_post_details_to_minio and ct_transform_and_load.

    Prepare tags to tag objects to keep from processing more than once.
    Using "processed" as a tag but modify as desired:
    ct_post_details_to_minio and ct_transform_and_load only check for presence of a tags,
    not the content of the actual tag.

    """

    tags = Tags.new_object_tags()
    tags["processed"] = "true"

    return tags


def get_minio_object_names(minio_client, bucket):
    """
    Used by ct_bundled_posts_to_minio, ct_post_details_to_minio,
    and ct_transform_and_load.

    Get all object names in the MinIO bucket.
    """
    return [obj.object_name for obj in minio_client.list_objects(bucket)]




def get_minio_response_js(object_name, bucket, client):
    """
    Process a MinIO object.
    """
    try:
        with client.get_object(bucket, object_name) as minio_response:
            minio_response_js = json.loads(minio_response.data.decode())
    except S3Error as e:
        print(f"S3 Error getting object: {e}")
        sys.exit(1)

    return minio_response_js


def deprecate_get_minio_response_js(object_name, bucket, client):
    """
    Process a MinIO object.
    """
    try:
        minio_response = client.get_object(bucket, object_name)
        minio_response_js = json.loads(minio_response.data.decode())
    except S3Error as e:
        print(f"S3 Error getting object: {e}")
        sys.exit(1)
    finally:
        minio_response.close()
        minio_response.release_conn()

    return minio_response_js


def minio_put_text_object(minio_client, bucket, object_name, request_response):
    """
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    Save data_in_bytes as object_name in bucket using minio_client.
    """

    request_response_bytes = io.BytesIO(request_response.text.encode("utf-8"))
    data_length = len(request_response_bytes.getvalue())

    try:
        minio_client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=request_response_bytes,
            length=data_length,
            content_type="text/plain",
        )
    except S3Error as e:
        print(f"S3 Error putting object: {e}")
        sys.exit(1)


#### Web functions


def request_with_backoff(url, headers=None):
    """
    Used by ct_posts_to_minio and ct_post_details_to_minio.

    Retries requests a total=total times, sleeping between retries
    for backoff_factor**retry number.  If status not in status_forcelist
    is received, request will not be retried.

    Returns response if successful.

    Prints error message and returns None if response is not successful.

    """

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


### Functions for formatting


def isoformat_to_seconds(*datetimes):
    """
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    Convert datetime objects to string format compatible with API call requirements and object naming

    """

    # Check for empty arguments
    if not datetimes:
        raise ValueError("No datetime objects provided.")
    try:
        if any(not isinstance(dt, datetime) for dt in datetimes):
            raise ValueError("Can only isoformat datetime objects.")

        if len(datetimes) == 1 and isinstance(datetimes[0], datetime):
            # If only one argument is passed and it is a datetime object, convert it to isoformat
            formatted_datetime = datetimes[0].isoformat(timespec="seconds")
            return formatted_datetime
        else:
            # If multiple arguments convert all to isoformat
            formatted_datetimes = [dt.isoformat(timespec="seconds") for dt in datetimes]
            return formatted_datetimes
    except ValueError as e:
        print(f"Error: {e}")
        raise


#### Database functions


def create_sqlalchemy_engine():
    """ """
    PGUSER, PGPASSWD, PGHOST, PGPORT, PGDB = load_db_credentials()
    engine_string = f"postgresql://{PGUSER}:{PGPASSWD}@{PGHOST}:{PGPORT}/{PGDB}"
    return create_engine(engine_string)
