# CrowdTangle ETL Pipeline

This repository contains Python scripts for Extract, Transform, and Load (ETL) operations on CrowdTangle data. The ETL process involves extracting data from CrowdTangle, storing it in MinIO S3 buckets for flexibility, and subsequently transforming and loading it into a PostgreSQL database.

## Scripts Overview

### 1. ct_bundled_posts_to_minio.py
This script extracts posts data from CrowdTangle and saves the data as objects in a MinIO bucket.

### 2. ct_post_details_to_minio.py
Extracts post details data from CrowdTangle using post data from previously stored post objects in MinIO.

### 3. ct_transform_and_load.py
Loads post details data from MinIO storage, transforms the data, and loads it into a PostgreSQL database.

### 4. ct_score_report.py
Extracts data from a PostgreSQL database and formats it for analysis, generating a report.

## Usage Instructions

1. **Run Scripts in Sequence:**
   Schedule scripts 1-3 to run at regular intervals. Script 1 attempts to backfill missing data, while scripts 2 and 3 rely on S3 object tagging for data completeness.

2. **Generate Analysis Report:**
   Schedule script 4 as needed to generate analysis reports based on the data in the PostgreSQL database.

## Important Considerations

- **MinIO S3 Buckets:**
  Ensure that MinIO S3 buckets are properly configured and accessible for storing and retrieving CrowdTangle data.

- **Script Dependencies:**
  Verify that all required Python packages and dependencies are installed before running the scripts.

- **Scheduling:**
  Consider scheduling scripts using a task scheduler or cron jobs to automate regular execution.

- **Data Completeness:**
  Be aware that the success of scripts 2 and 3 relies on proper S3 object tagging for data completeness.
