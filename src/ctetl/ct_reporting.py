# ct_reporting.py

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
