
#!/home/pscripts/venv/bin/python
# coding: utf-8

from datetime import datetime, timedelta, timezone
import json
import os

import requests

from minio.commonconfig import Tags

from ctetl.ct_helpers import create_minio_client, check_buckets
from ctetl.ct_extract import process_post_object, get_and_save_post_details, upload_post_details

def main():
    # request_headers and ct_key must be defined
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
    }

    ct_key = os.environ.get("CT_KEY")

    # Input from posts_bucket, output to details_bucket
    posts_bucket = "ct-posts"
    details_bucket = "ct-post-details"
    
    minio_client = create_minio_client()
    
    # Proceed only if both bucket are found
    check_buckets(minio_client, [posts_bucket, details_bucket])
    
    # Prepare to tag posts to keep from processing objects more than once
    tags = Tags.new_object_tags()
    tags["processed"] = "true"

    # Set number of API calls to track and limit calls to CrowdTangle as a global variable
    num_calls = 0

    # Get post objects saved in MinIO and loop through each to process
    post_objects = minio_client.list_objects(posts_bucket)
    for post_obj in post_objects:
        process_post_object(tags, num_calls, request_headers, ct_key, minio_client, posts_bucket, details_bucket, post_obj)


if __name__ == "__main__":
    main()
