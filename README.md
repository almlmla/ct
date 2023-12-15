# ETL of CrowdTangle data

Python scripts to perform ETL on CrowdTangle data.

MinIO S3 buckets are used as intermediary storage of CrowdTangle data
to allow flexibility in subsequent transformation and loading into a 
PostgreSQL database.

The pipeline is comprised of 4 scripts:

1. **ct_posts_to_minio.py**  This script extracts posts data from 
CrowdTangle and saves the data as objects in a MinIO bucket.
2. **ct_post_details_to_minio.py** extracts post details data
from CrowdTangle using post data from earlier stored post objects
in MinIO.
3. **ct_post_details_transform_and_load.py** loads post details data
from MinIO storage, transforms the data and loads the data into a PostgreSQL
database.
4. **ct_analysis.py** extracts data from a PostgreSQL database and formats
the data for analysis.

Scripts 1-3 should be scheduled to run in regular time intervals and in sequence.
However, script 1 will attempt to backfill any missing data, while scripts 2
and 3 rely on S3 object tagging to help ensure completeness of data.  Each run of
script 4 will generate a report for analysis and should be scheduled as needed.
