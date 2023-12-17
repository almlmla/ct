#!/home/pscripts/venv/bin/python

from time import sleep

from ctetl.ct_helpers import get_request_parameters, create_minio_client
from ctetl.ct_helpers import check_minio_buckets
from ctetl.ct_helpers import isoformat_to_seconds
from ctetl.ct_extract import get_initial_start_and_end
from ctetl.ct_extract import get_and_save_ct_post_aggregates, get_posts_next_page_url


def main():
    # load request parameters to use
    REQUEST_HEADERS, CT_KEY = get_request_parameters()

    # Bundled post objects are saved in 'ct-posts' that must already exist
    posts_bucket = "ct-posts"

    # Create MinIO client
    minio_client = create_minio_client()

    # Output will be to posts_bucket
    check_minio_buckets(minio_client, posts_bucket)

    # Determine start and end times of bundled posts to get from CrowdTangle.
    # Also 'get' now to save as part of with object name
    start, end, now = get_initial_start_and_end(minio_client, posts_bucket)

    # Convert times to string format compatible with API call
    # requirements and object naming

    start_str, end_str, as_of = isoformat_to_seconds(start, end, now)

    # Set number of API calls to track and limit calls to CrowdTangle
    num_calls = 0

    # Set page counter in case request returns multiple pages
    page = 1

    # Set initial URL to the first page.
    # URL of any subsequent pages are determined from the response(s)
    url = f"https://api.crowdtangle.com/posts?token={CT_KEY}&sortBy=date&endDate={end_str}&startDate={start_str}&count=100"

    while url:
        # If num_calls >5, sleep for 60 seconds to accommodate and respect
        # CrowdTangle's API rate limit of 6 calls/minute
        if num_calls > 5:
            sleep(60)
            num_calls = 0

        request_response = get_and_save_ct_post_aggregates(
            url,
            REQUEST_HEADERS,
            minio_client,
            posts_bucket,
            as_of,
            end_str,
            start_str,
            page,
        )

        num_calls += 1
        page += 1

        # Find subsequent page if it exists, else empty url exits the loop
        url = get_posts_next_page_url(request_response)


if __name__ == "__main__":
    main()
