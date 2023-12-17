#!/home/pscripts/venv/bin/python

from ctetl.ct_helpers import create_sqlalchemy_engine
from ctetl.ct_reporting import query_report_data_from_db, generate_report
from ctetl.ct_reporting import save_report_to_csv


def main():
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
