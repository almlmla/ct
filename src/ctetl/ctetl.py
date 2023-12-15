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


def query_report_data_from_db(engine, start=96, end=72):
    """
    Changing timestamp data to local/UTC+8 for the reports.
    CrowdTangle timestamp data are always at UTC.

    """

    if start < end:
        print("Start must be greater than end.")
        sys.exit(1)

    query = text(
        f"""
    SELECT  account_name,
            account_id,
            platform_id,
            post_message,
            post_url,
            posting_date AT TIME ZONE 'UTC+8' AS local_posting_date,
            as_of AT TIME ZONE 'UTC+8',
            score,
            metric_timestep
    FROM accounts
    JOIN posts USING (account_id)
    JOIN post_metrics USING (platform_id)
    WHERE posting_date BETWEEN CURRENT_TIMESTAMP - interval '{start} hours' AND CURRENT_TIMESTAMP - interval '{end} hours';
    """
    )
    with engine.connect() as con:
        result = con.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


def fill_missing_timesteps(df, max_timesteps=51):
    """
    Timestep of 50 corresponds to an interval of between 2 days 18 hours and 3 days since the posting date.
    Using 51 because, python.
    """

    # Initialize list to accumulate data
    result_data = []

    for metric_timestep in range(max_timesteps):
        if metric_timestep in df["metric_timestep"].values:
            result_data.append(
                df[df["metric_timestep"] == metric_timestep].iloc[0].to_dict()
            )
        else:
            if result_data:
                last_valid_values = result_data[-1].copy()
                last_valid_values["metric_timestep"] = metric_timestep
                result_data.append(last_valid_values)

    # Create a DataFrame from the list of dictionaries
    result_df = pd.DataFrame(result_data)

    return result_df


def generate_report(df):
    max_timesteps = 51
    # Initialize list to accumulate data
    report_data = []

    unique_platform_ids = df["platform_id"].unique()

    for platform_id in unique_platform_ids:
        post_df = df[df["platform_id"] == platform_id]
        filled_df = fill_missing_timesteps(post_df, max_timesteps)
        report_data.extend(filled_df.to_dict(orient="records"))

    # Create a DataFrame from the list of dictionaries
    report_df = pd.DataFrame(report_data)
    report_df.reset_index(drop=True, inplace=True)

    return report_df


def save_report_to_csv(report_df):
    now = datetime.now().replace(tzinfo=None)
    as_of = now.isoformat(timespec="seconds")
    filename = as_of.replace(":", "-") + "-ct_posts_report.csv"
    report_df.to_csv(filename, index=False, encoding="utf-8-sig")
