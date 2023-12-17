# ct_reporting.py

from datetime import datetime

import pandas as pd

from sqlalchemy import text

from ctetl.ct_helpers import isoformat_to_seconds


def query_report_data_from_db(engine, start=96, end=72):
    """
    Used by ct_score_report

    Changing timestamp data to local/UTC+8 for the reports.
    CrowdTangle timestamp data are always at UTC.

    """

    if start <= end:
        print("Start must be greater than end.")
        sys.exit(1)

    query = text(
        f"""
    SELECT  account_name,
            account_id,
            platform_id,
            post_message,
            post_url,
            posting_date AT TIME ZONE 'UTC+8' AS sgt_posting_date,
            as_of AT TIME ZONE 'UTC+8' as sgt_as_of,
            score,
            metric_timestep
    FROM accounts
    JOIN posts USING (account_id)
    JOIN post_metrics USING (platform_id)
    WHERE posting_date BETWEEN CURRENT_TIMESTAMP - interval '{start} hours' 
    AND CURRENT_TIMESTAMP - interval '{end} hours';
    """
    )
    with engine.connect() as con:
        result = con.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


def fill_missing_timesteps(df, max_timesteps=51):
    """
    Used by ct_score_report and generate_report.

    Used to forward fill missing timestep values from last for every post.
    Logically this works as any changes in values will have a timestep entry.

    """

    # Accumulate date in a list as it is cheaper than a DataFrame.
    result_data = []

    # Forward fill missing timestep values from last.
    # Logically this works as any changes in values
    # will have a timestep entry

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

    # finally create and return a DataFrame from the list of dictionaries
    return pd.DataFrame(result_data)


def generate_report(df):
    """
    Used by ct_score_report.

    Generate a daily report of CrowdTangle scores from the supplied df.

    Timestep of 50 corresponds to an interval of between 2 days 18 hours
    and 3 days since the posting date.  Using 51 because of python counting.

    """
    max_timesteps = 51

    # Accumulating data in a list as it is cheaper than a DataFrame
    report_data = []

    unique_platform_ids = df["platform_id"].unique()

    for platform_id in unique_platform_ids:
        post_df = df[df["platform_id"] == platform_id]
        report_data.extend(
            fill_missing_timesteps(post_df, max_timesteps).to_dict(orient="records")
        )

    # Create a DataFrame from the list of dictionaries
    report_df = pd.DataFrame(report_data)
    report_df.reset_index(drop=True, inplace=True)

    return report_df


def save_report_to_csv(report_df):
    """
    Used by ct_score_report.

    Save the report to a csv.  Include timestamp of report generation
    in filename.

    """

    now = datetime.now().replace(tzinfo=None)
    as_of = isoformat_to_seconds(now)
    filename = as_of.replace(":", "-") + "-ct_posts_score_report.csv"
    report_df.to_csv(filename, index=False, encoding="utf-8-sig")
