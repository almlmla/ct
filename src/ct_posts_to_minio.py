#!/home/pscripts/venv/bin/python
# coding: utf-8

import os

from ctetl.ct_helpers import create_minio_client, check_buckets, request_with_backoff
from ctetl.ctetl import get_initial_start_and_end, get_and_save_ct_post_aggregates, get_posts_next_page_url

def main():
    # request_headers and ct_key must be defined
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
    }

    ct_key = os.environ.get("CT_KEY")

    # Aggregate post objects are saved in 'ct-posts' that must already exist
    posts_bucket = "ct-posts"

    # Create MinIO client
    minio_client = create_minio_client()

    # Check that the bucket exists
    check_buckets(minio_client, [posts_bucket])

    # Determine start and end times of aggregate posts to get from CrowdTangle
    # Also get now to save with object name
    start, end, now = get_initial_start_and_end(minio_client, posts_bucket)

    # Convert times to string format compatible with API call requirements and object naming

    start_str = start.isoformat(timespec="seconds")
    end_str = end.isoformat(timespec="seconds")
    as_of = now.isoformat(timespec="seconds")

    # Set number of API calls to track and limit calls to CrowdTangle
    num_calls = 0

    # Set page counter in case request returns multiple pages
    page = 1

    # Set initial URL to the first page. Next pages determined from the first response
    url = f"https://api.crowdtangle.com/posts?token={ct_key}&sortBy=date&endDate={end_str}&startDate={start_str}&count=100"

    while url:

        # Sleep for 60 seconds to accommodate and respect CrowdTangles API rate limit of 6 calls/minute
        if num_calls > 5:
            sleep(60)
            num_calls = 0

        response = get_and_save_ct_post_aggregates(
            url, request_headers, minio_client, posts_bucket, as_of, end_str, start_str, page
        )

        num_calls +=1
        page += 1

        # Find subsequent page if it exists, else url = '' and exits the loop
        url = get_posts_next_page_url(response)


if __name__ == "__main__":
    main()
