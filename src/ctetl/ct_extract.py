# ct_extract.py

import sys

from datetime import datetime, timedelta, timezone
import time
from time import sleep

from minio.error import S3Error

from .ct_helpers import create_redis_client, allow_request
from .ct_helpers import get_minio_object_names, request_with_backoff
from .ct_helpers import minio_put_text_object
from .ct_helpers import get_minio_response_js, isoformat_to_seconds


### Functions of ct_bundled_posts_to_minio
def get_initial_start_and_end(minio_client, posts_bucket):
    """
    Used by ct_bundled_posts_to_minio.

    Start from maiden start if posts_bucket is empty.  Otherwise, start from
    end of latest post in posts_bucket.

    """

    # Define chosen maiden start in case posts_bucket is empty
    # Need to format as '%Y-%m-%d %H:%M:%S'
    MAIDEN_START_STR = "2023-12-10 05:00:00"

    # Get posts dated from start+TIME_WINDOW from CrowdTangle.
    # The bigger this window, the more posts will be returned
    # but the risk of getting HTTP response failures is higher.
    TIME_WINDOW = timedelta(hours=1)

    # datetime.now - START_LIMIT hours is the latest posting date
    # of posts to get from CrowdTangle. Post performance changes
    # with its lifespan and so posts should be allowed to age before
    # getting post data from CrowdTangle for analysis.  However, this
    # should also be balanced for relevance of information.
    # 72 hours was chose as a compromise between both considerations.
    START_LIMIT_HOURS = 72

    # Assign start_limit from START_LIMIT_HOURS
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start_limit = now - timedelta(hours=START_LIMIT_HOURS)

    # Determine start by comparing MAIDEN_START_STR with the timestamp of
    # the latest post in posts_bucket
    start = set_start(minio_client, posts_bucket, MAIDEN_START_STR)

    # Exit this run normally if start > start_limit
    if start > start_limit:
        print("Too early to get new post data!")
        sys.exit(0)

    end = start + TIME_WINDOW

    # Also return 'now' as it is used in object naming
    return start, end, now


def set_start(minio_client, posts_bucket, MAIDEN_START_STR):
    """
    Used by ct_bundled_posts_to_minio.

    Find end time of latest post in bucket
    If post_object_names is empty (no data in posts_bucket)
    then return start as datetime object of MAIDEN_START_STR.

    """

    post_object_names = get_minio_object_names(minio_client, posts_bucket)

    if post_object_names:
        post_object_names.sort()
        # Get end timestamp from last element of the sorted post_obj_names.
        # End timestamp index position is dependent on object naming convention
        # of ct_bundled_posts_to_minio!
        start = datetime.strptime(
            post_object_names[-1].split("_")[1], "%Y-%m-%dT%H:%M:%S"
        )
    else:
        start = datetime.strptime(MAIDEN_START_STR, "%Y-%m-%d %H:%M:%S")

    return start


def get_and_save_ct_post_aggregates(
    url, REQUEST_HEADERS, minio_client, posts_bucket, as_of, end_str, start_str, page
):
    """
    Used by ct_bundled_posts_to_minio.

    Get bundled posts data from CrowdTangle and save to posts_bucket.

    """

    request_response = request_with_backoff(url, REQUEST_HEADERS)

    if request_response is None:
        # request_with_backoff prints the error message.
        sys.exit(1)
    else:
        # Naming convention important in determining start times with set_start
        # Use caution if modifying!
        post_object_name = (
            as_of + "_" + end_str + "_" + start_str + "_" + str(page) + ".txt"
        )
        try:
            minio_put_text_object(
                minio_client, posts_bucket, post_object_name, request_response
            )
            return request_response
        except S3Error as e:
            print(f"S3 Error:{e}")
            sys.exit(1)


def get_posts_next_page_url(request_response):
    """
    Used by ct_bundled_posts_to_minio.

    Determine next url from current response.  Return None if nextPage doesn't exist.

    """

    try:
        request_response_js = request_response.json()
        return request_response_js["result"]["pagination"]["nextPage"]
    except KeyError:
        return None


### Functions of ct_post_details_to_minio.


def process_post_object(
    tags,
    num_calls,
    request_headers,
    ct_key,
    minio_client,
    posts_bucket,
    details_bucket,
    post_object_name,
):
    """
    Used by ct_post_details_to_minio.

    Find details on posts from aggregate post details stored in posts_bucket in MinIO.
    Cascade to downstream process.
    """
    tagged = minio_client.get_object_tags(posts_bucket, post_object_name)

    # Process only if not tagged
    if not tagged:
        minio_response_js = get_minio_response_js(
            post_object_name, posts_bucket, minio_client
        )
        get_and_save_post_details(
            tags,
            num_calls,
            request_headers,
            ct_key,
            minio_client,
            posts_bucket,
            details_bucket,
            post_object_name,
            minio_response_js,
        )


def get_and_save_post_details(
    tags,
    num_calls,
    request_headers,
    ct_key,
    minio_client,
    posts_bucket,
    details_bucket,
    post_object_name,
    minio_response_js,
):
    """
    Used by ct_post_details_to_minio.

    Request post details from CrowdTangle.  Ensure that tags are defined globally. Ensure
    that request_headers and ct_key are defined globally. Call downstream process.  Tag
    the aggregate post object as processed once all post details of the aggregate
    are uploaded.
    """

    redis_client = create_redis_client()
    redis_key = ct_key

    # Post details are uniquely identified by platformId
    # Number of posts in minio_response_js is half the count of platformId
    for i in range(str(minio_response_js).count("platformId") // 2):
        platform_id = minio_response_js["result"]["posts"][i]["platformId"]

        # URL for specific posts
        url = f"https://api.crowdtangle.com/post/{platform_id}?token={ct_key}&includeHistory=True"
        allowed = False
        while not allowed:
            # Keep looping until allowed by the rate limiter
            # CrowdTangle's limit is 6 requests in 60 seconds
            allowed = allow_request(redis_client, redis_key, 6, 60)
        request_response = request_with_backoff(url, request_headers)

        if request_response is None:
            sys.exit(1)
        else:
            upload_post_details(
                minio_client, details_bucket, platform_id, request_response
            )

    # Tag post_object after processing to prevent reprocessing
    minio_client.set_object_tags(posts_bucket, post_object_name, tags)


def upload_post_details(minio_client, details_bucket, platform_id, request_response):
    """
    Used by ct_post_details_to_minio.

    Upload post details to MinIO bucket.
    """
    # as_of will form part of the name of the post_details object
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    as_of = isoformat_to_seconds(now)
    details_object_name = f"{platform_id}_{as_of}_.txt"

    try:
        minio_put_text_object(
            minio_client, details_bucket, details_object_name, request_response
        )
    except S3Error as e:
        print(f"S3 Error putting object:{e}")
        sys.exit(1)
