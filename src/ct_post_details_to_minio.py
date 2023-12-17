#!/home/pscripts/venv/bin/python

from ctetl.ct_helpers import get_request_parameters
from ctetl.ct_helpers import create_minio_client, check_minio_buckets
from ctetl.ct_helpers import create_minio_tags, get_minio_object_names
from ctetl.ct_extract import process_post_object


def main():
    # load request parameters to use
    REQUEST_HEADERS, CT_KEY = get_request_parameters()

    # Input from posts_bucket, output to details_bucket
    posts_bucket = "ct-posts"
    details_bucket = "ct-post-details"

    minio_client = create_minio_client()

    # Proceed only if both bucket are found
    check_minio_buckets(minio_client, posts_bucket, details_bucket)

    # Prepare to tag posts to keep from processing them more than once
    tags = create_minio_tags()

    # Set number of API calls to track and limit calls to CrowdTangle
    num_calls = 0

    # Get post object names saved in posts_bucket and loop through each to process
    post_object_names = get_minio_object_names(minio_client, posts_bucket)
    for post_object_name in post_object_names:
        process_post_object(
            tags,
            num_calls,
            REQUEST_HEADERS,
            CT_KEY,
            minio_client,
            posts_bucket,
            details_bucket,
            post_object_name,
        )


if __name__ == "__main__":
    main()
