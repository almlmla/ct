#!/home/pscripts/venv/bin/python
# coding: utf-8

from ctetl.ct_helpers import get_db_credentials, create_sqlalchemy_engine
from ctetl.ctetl import query_report_data_from_db, fill_missing_timesteps, generate_report, save_report_to_csv

def main():
    pguser, pgpasswd, pghost, pgport = get_db_credentials()
    engine = create_sqlalchemy_engine()

    start_hours = 96
    end_hours = 72

    df = query_report_data_from_db(engine, start=start_hours, end=end_hours)

    # Need to drop duplicates since only querying for scores
    df.drop_duplicates(inplace=True)

    report_df = generate_report(df)
    save_report_to_csv(report_df)


if __name__ == "__main__":
    main()
