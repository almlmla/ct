# ct_tl

import pandas as pd

import psycopg2

import sys

from .ct_helpers import load_db_credentials


def load_columns_to_extract():
    """ """

    ACCOUNTS_COLS = [
        "account.id",
        "account.name",
        "account.handle",
        "account.url",
        "account.platform",
        "account.platformId",
        "account.accountType",
        "account.pageAdminTopCountry",
        "account.pageDescription",
        "account.pageCreatedDate",
        "account.pageCategory",
        "account.verified",
    ]

    POSTS_COLS = [
        "platformId",
        "platform",
        "date",
        "type",
        "message",
        "postUrl",
        "subscriberCount",
        "account.id",
    ]

    POST_METRICS_COLS = [
        "platform_id",
        "as_of",
        "score",
        "metric_name",
        "metric_value",
        "date",
        "timestep",
    ]

    return ACCOUNTS_COLS, POSTS_COLS, POST_METRICS_COLS


def load_column_remaps():
    """
    Remap/rename columns to ensure compatibility with SQL
    """

    ACCOUNTS_COLS_REMAP = {
        "account.id": "account_id",
        "account.name": "account_name",
        "account.handle": "account_handle",
        "account.url": "account_url",
        "account.platform": "account_platform",
        "account.platformId": "account_platform_id",
        "account.accountType": "account_type",
        "account.pageAdminTopCountry": "account_page_admin_top_country",
        "account.pageDescription": "account_page_description",
        "account.pageCreatedDate": "account_page_created_date",
        "account.pageCategory": "account_page_category",
        "account.verified": "account_verified",
    }

    POSTS_COLS_REMAP = {
        "platformId": "platform_id",
        "date": "posting_date",
        "type": "post_type",
        "message": "post_message",
        "postUrl": "post_url",
        "subscriberCount": "subscriber_count",
        "account.id": "account_id",
    }

    POST_METRICS_COLS_REMAP = {
        "date": "metric_timestamp",
        "timestep": "metric_timestep",
    }

    return ACCOUNTS_COLS_REMAP, POSTS_COLS_REMAP, POST_METRICS_COLS_REMAP


def queries_for_insert():
    # Define INSERT statements with placeholders.  Query inserts
    # only if record doesn't exist
    ACCOUNTS_INSERT_QUERY = "INSERT INTO accounts VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (account_id) DO NOTHING"
    POSTS_INSERT_QUERY = "INSERT INTO posts VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (platform_id) DO NOTHING"
    POST_METRICS_INSERT_QUERY = "INSERT INTO post_metrics VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (platform_id, as_of, score, metric_name, metric_value, metric_timestamp, metric_timestep) DO NOTHING"

    return ACCOUNTS_INSERT_QUERY, POSTS_INSERT_QUERY, POST_METRICS_INSERT_QUERY


def recast_accounts(accounts_df):
    """
    Recast dtypes prior to uploading to PostgreSQL.
    """

    recast_accounts_df = accounts_df.astype(
        {
            "account_id": "int64",
            "account_name": "string",
            "account_handle": "string",
            "account_url": "string",
            "account_platform": "string",
            "account_platform_id": "int64",
            "account_type": "string",
            "account_page_admin_top_country": "string",
            "account_page_description": "string",
            "account_page_created_date": "datetime64[ns]",
            "account_page_category": "string",
        }
    )

    recast_accounts_df["account_verified"] = (
        accounts_df["account_verified"]
        .map({"False": False, "True": True})
        .astype("bool")
    )

    return recast_accounts_df


def recast_posts(posts_df):
    """
    Recast dtypes prior to uploading to PostgreSQL.
    """

    recast_posts_df = posts_df.astype(
        {
            "platform_id": "string",
            "platform": "string",
            "posting_date": "datetime64[ns]",
            "post_type": "string",
            "post_message": "string",
            "post_url": "string",
            "subscriber_count": "int64",
            "account_id": "int64",
        }
    )

    return recast_posts_df


def recast_post_metrics(post_metrics_df):
    """
    Recast dtypes prior to uploading to PostgreSQL.
    """

    recast_post_metrics_df = post_metrics_df.astype(
        {
            "platform_id": "string",
            "as_of": "datetime64[ns]",
            "score": "float64",
            "metric_name": "string",
            "metric_value": "int64",
            "metric_timestamp": "datetime64[ns]",
            "metric_timestep": "int64",
        }
    )

    return recast_post_metrics_df


def transform_post_details(minio_response_js, detail_object_name):
    accounts_cols, posts_cols, post_metrics_cols = load_columns_to_extract()
    (
        accounts_cols_remap,
        posts_cols_remap,
        post_metrics_cols_remap,
    ) = load_column_remaps()

    # Create accounts_df and posts_df from posts object
    pa_df = pd.json_normalize(minio_response_js, record_path=["result", "posts"])

    accounts_df = pa_df[accounts_cols].rename(columns=accounts_cols_remap)
    posts_df = pa_df[posts_cols].rename(columns=posts_cols_remap)

    # Create post_metrics_df from history object
    history_df = pd.json_normalize(
        minio_response_js, record_path=["result", "posts", "history"]
    )

    # Melt/unpivot the metrics and metric values to two columns
    melted_df = pd.melt(
        history_df,
        id_vars=["timestep", "date", "score"],
        var_name="metric_name",
        value_name="metric_value",
    )

    # Include other data for referencing
    melted_df["platform_id"] = pa_df.platformId[0]
    melted_df["as_of"] = detail_object_name.split("_")[2]

    # Rearrange columns and amend column names, filter out rows on platform_id that was expressed as a metric
    post_metrics_df = melted_df[post_metrics_cols].rename(
        columns=post_metrics_cols_remap
    )
    condition = post_metrics_df["metric_name"] == "platform_id"
    post_metrics_filtered_df = post_metrics_df[~condition]

    # Recast to correct dtypes
    recast_accounts_df = recast_accounts(accounts_df)
    recast_posts_df = recast_posts(posts_df)
    recast_post_metrics_df = recast_post_metrics(post_metrics_filtered_df)

    # Convert DataFrames to a list of dictionaries
    accounts_to_insert = [
        tuple(row) for row in recast_accounts_df.itertuples(index=False, name=None)
    ]
    posts_to_insert = [
        tuple(row) for row in recast_posts_df.itertuples(index=False, name=None)
    ]
    post_metrics_to_insert = [
        tuple(row) for row in recast_post_metrics_df.itertuples(index=False, name=None)
    ]

    return accounts_to_insert, posts_to_insert, post_metrics_to_insert


def insert_to_postgres(
    accounts_insert_query,
    posts_insert_query,
    post_metrics_insert_query,
    accounts_to_insert,
    posts_to_insert,
    post_metrics_to_insert,
):
    PGUSER, PGPASSWD, PGHOST, PGPORT, PGDB = load_db_credentials()

    try:
        with psycopg2.connect(
            database=PGDB,
            user=PGUSER,
            password=PGPASSWD,
            host=PGHOST,
            port=PGPORT,
        ) as connection:
            with connection.cursor() as cursor:
                # Insert records
                cursor.executemany(accounts_insert_query, accounts_to_insert)
                cursor.executemany(posts_insert_query, posts_to_insert)
                cursor.executemany(post_metrics_insert_query, post_metrics_to_insert)

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        sys.exit(1)
