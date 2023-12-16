# ct_extract.py

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

from .ct_helpers import request_with_backoff

# Used by ct_posts_to_minio
def get_initial_start_and_end(client, bucket):
    # Very first start if bucket is empty
    very_first_start_str = "2023-12-10 05:00:00"

    # Window of posts to get from CrowdTangle
    time_window = timedelta(hours=1)

    # The latest post date from now() to get from CrowdTangle
    # Post performance changes with its lifespan
    latest_hours = 72

    # Assign latest_start
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    latest_start = now - timedelta(hours=latest_hours)

    # Get the last run start
    start = get_latest_post_start(client, bucket, very_first_start_str)

    # Exit this run if start > latest_start
    if start > latest_start:
        print("All caught up and too early to start!")
        sys.exit(0)

    end = start + time_window

    return start, end, now


# Used by ct_posts_to_minio
def get_latest_post_start(client, bucket, very_first_start_str):
    # Find last run from end time and determine start for this run
    post_objects = client.list_objects(bucket)

    post_obj_names = [post_obj.object_name for post_obj in post_objects]
    if post_obj_names:
        post_obj_names.sort()
        start = datetime.strptime(post_obj_names[-1].split("_")[1], "%Y-%m-%dT%H:%M:%S")
    else:
        start = datetime.strptime(very_first_start_str, "%Y-%m-%d %H:%M:%S")

    return start


# Used by ct_posts_to_minio
def get_and_save_ct_post_aggregates(url, request_headers, client, bucket, as_of, end_str, start_str, page):

    response = request_with_backoff(url, request_headers)

    if response is None:
        sys.exit(1)
    else:
        response_bytes = io.BytesIO(response.text.encode("utf-8"))
        obj_name = as_of + "_" + end_str + "_" + start_str + "_" + str(page) + ".txt"

    try:
        client.put_object(
            bucket_name=bucket,
            object_name=obj_name,
            data=response_bytes,
            length=len(response_bytes.getvalue()),
            content_type="text/plain",
        )
        return response
    except S3Error as e:
        print(f"S3 Error:{e}")
        sys.exit(1)


# Used by ct_posts_to_minio
def get_posts_next_page_url(response):
    try:
        response_js = response.json()
        return response_js["result"]["pagination"]["nextPage"]
    except KeyError:
        return None


# Used by ct_post_details_to_minio
def process_post_object(tags, num_calls, request_headers, ct_key, client, posts_bucket, details_bucket, post_obj):
    """
    Find details on posts from aggregate post details stored in posts_bucket in MinIO.
    Call downstream process.
    """
    post_obj_name = post_obj.object_name
    tagged = client.get_object_tags(posts_bucket, post_obj_name)

    # Process only if not tagged
    if not tagged:
        try:
            minio_response = client.get_object(posts_bucket, post_obj_name)
            minio_response_js = json.loads(minio_response.data.decode())
        except S3Error as e:
            print(f"S3 Error getting object:{e}")
            sys.exit(1)
        finally:
            minio_response.close()
            minio_response.release_conn()

        get_and_save_post_details(tags, num_calls, request_headers, ct_key, client, posts_bucket, details_bucket, post_obj_name, minio_response_js)

# Used by ct_post_details_to_minio
def get_and_save_post_details(tags, num_calls, request_headers, ct_key, client, posts_bucket, details_bucket, post_obj_name, minio_response_js):
    """
    Request post details from CrowdTangle.  Ensure that request_headers and ct_key are defined globally.
    Call downstream process.  Tag the aggregate post object as processed once all post details of the aggregate
    are uploaded. Ensure that tags are defined globally.
    """

    # Number of posts in minio_response_js is half the count of platformId
    for i in range(str(minio_response_js).count("platformId") // 2):
        platform_id = minio_response_js["result"]["posts"][i]["platformId"]

        # URL for specific posts
        url = f"https://api.crowdtangle.com/post/{platform_id}?token={ct_key}&includeHistory=True"

        # Have to use global variable to ensure CrowdTangle API request limits are respected
        # Limit is 6 calls/minute
        if num_calls > 5:
            sleep(60)
            num_calls = 0

        response = request_with_backoff(url, request_headers)
        num_calls += 1

        if response is None:
            sys.exit(1)
        else:
            upload_post_details(client, posts_bucket, details_bucket, post_obj_name, platform_id, response)

    client.set_object_tags(posts_bucket, post_obj_name, tags)

# Used by ct_post_details_to_minio
def upload_post_details(client, posts_bucket, details_bucket, post_obj_name, platform_id, response):
    """
    Upload post details to MinIO bucket.
    """
    # as_of will form part of the name of the post_details object
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    as_of = now.isoformat(timespec="seconds")

    response_bytes = io.BytesIO(response.text.encode("utf-8"))
    detail_name = f"{platform_id}_{as_of}_.txt"

    try:
        client.put_object(
            bucket_name=details_bucket,
            object_name=detail_name,
            data=response_bytes,
            length=len(response_bytes.getvalue()),
            content_type="text/plain",
        )

    except S3Error as e:
        print(f"S3 Error putting object:{e}")
        sys.exit(1)
