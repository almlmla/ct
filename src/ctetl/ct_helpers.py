# ct_helpers.py

import io
import json
import os
import sys
from datetime import datetime
import time

import psycopg2
import redis
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine

from minio import Minio
from minio.commonconfig import Tags
from minio.error import S3Error


### Functions to get parameters from environment


def get_request_parameters():
    """ "
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    API key for accessing CrowdTangle must be saved as CT_KEY
    and defined in the environment.

    Load CT_KEY from environment and define REQUEST_HEADERS.
    """
    REQUEST_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
    }

    CT_KEY = os.environ.get("CT_KEY")
    if CT_KEY is None:
        print("CT_KEY is empty.  Is it defined in the environment?")
        sys.exit(1)
    else:
        return REQUEST_HEADERS, CT_KEY


def load_db_credentials():
    """
    Used by functions in ct_transform_and_load and ct_score_reports.

    Returns PostgreSQL credentials from the environment.

    """

    PGUSER = os.environ.get("PGUSER")
    PGPASSWD = os.environ.get("PGPASSWD")
    PGHOST = os.environ.get("PGHOST")
    PGPORT = os.environ.get("PGPORT")
    PGDB = "ct"

    if any(P is None for P in (PGUSER, PGPASSWD, PGHOST, PGPORT)):
        print(
            "At least one of the PostgreSQL parameters is empty.  Are they defined in the environment?"
        )
        sys.exit(1)
    else:
        return PGUSER, PGPASSWD, PGHOST, PGPORT, PGDB


### Redis functions

def create_redis_client():
    """
    Create a Redis client using standard parameters.
    Redis server must already be running.
    """
    host = 'localhost'
    port = 6379
    db = 0
    try:
        # Attempt to create a Redis client
        redis_client = redis.StrictRedis(host=host, port=port, db=db)
        redis_client.ping()
        return redis_client
    except Exception as e:
        # Handle exceptions, such as connection errors
        print(f"Error creating Redis client: {e}")
        return None


### MinIO functions


def create_minio_client():
    """
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    Create MinIO client from access and secret key.

    """
    # Load values of MinIO parameters from environment
    MINIO_HOST = os.environ.get("MINIO_HOST")
    MINIO_ACCESS = os.environ.get("MINIO_ACCESS")
    MINIO_SECRET = os.environ.get("MINIO_SECRET")

    if any(P is None for P in (MINIO_HOST, MINIO_ACCESS, MINIO_SECRET)):
        print(
            "At least one of the MinIO parameters is empty.  Are they defined in the environment?"
        )
        sys.exit(1)
    else:
        # Modify 'secure' to your configuration
        return Minio(MINIO_HOST, MINIO_ACCESS, MINIO_SECRET, secure=False)


def check_minio_buckets(minio_client, *buckets):
    """
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    Find buckets otherwise exit. Buckets entered as individual arguments.

    """
    for bucket in buckets:
        # Proceed only if the bucket exists
        found = minio_client.bucket_exists(bucket)
        if not found:
            print(f"Cannot find bucket '{bucket}', exiting.")
            # Exit if any bucket not found
            sys.exit(1)


def create_minio_tags():
    """
    Used by ct_post_details_to_minio and ct_transform_and_load.

    Prepare tags to tag objects to keep from processing more than once.
    Using "processed" as a tag but modify as desired:
    ct_post_details_to_minio and ct_transform_and_load only check for presence of a tags,
    not the content of the actual tag.

    """

    tags = Tags.new_object_tags()
    tags["processed"] = "true"

    return tags


def get_minio_object_names(minio_client, bucket):
    """
    Used by ct_bundled_posts_to_minio, ct_post_details_to_minio,
    and ct_transform_and_load.

    Get all object names in the MinIO bucket.
    """
    return [obj.object_name for obj in minio_client.list_objects(bucket)]


def get_minio_response_js(object_name, bucket, client):
    """
    Process a MinIO object.
    """
    try:
        with client.get_object(bucket, object_name) as minio_response:
            minio_response_js = json.loads(minio_response.data.decode())
    except S3Error as e:
        print(f"S3 Error getting object: {e}")
        sys.exit(1)

    return minio_response_js


def minio_put_text_object(minio_client, bucket, object_name, request_response):
    """
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    Save data_in_bytes as object_name in bucket using minio_client.
    """

    request_response_bytes = io.BytesIO(request_response.text.encode("utf-8"))
    data_length = len(request_response_bytes.getvalue())

    try:
        minio_client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=request_response_bytes,
            length=data_length,
            content_type="text/plain",
        )
    except S3Error as e:
        print(f"S3 Error putting object: {e}")
        sys.exit(1)


#### Web functions


def request_with_backoff(url, headers=None):
    """
    Used by ct_posts_to_minio and ct_post_details_to_minio.

    Retries requests a total=total times, sleeping between retries
    for backoff_factor**retry number.  If status not in status_forcelist
    is received, request will not be retried.

    Returns response if successful.

    Prints error message and returns None if response is not successful.

    """

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))

    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Requests error after retrying.  Error: {e}")
        return None


def allow_request(redis_client, redis_key, rate_limit, time_limit):
    """
    Check data at redis_key if request is allowed within rate_limit and time_limit.
    Function will sleep if rate if above rate limit and will return False.  Use within
    a while loop until function return True.
    
    Assumes redis_key is only used for this function and the data are timestamps.  
    No checks are made to validate this assumption.  If using this function to limit
    API calls, using the API key as the redis_key is recommended.
    
    """
    request_time = int(time.time())
    
    # Retrieve the current list of timestamps from Redis
    timestamps_bytes = redis_client.lrange(redis_key, 0, -1)
    
    # Convert timestamps from bytes to floats
    timestamps = [int(round(float(ts))) for ts in timestamps_bytes]
  
    # If more timestamps than rate_limit, trim to rate_limit elements
    if len(timestamps) > rate_limit:
        for _ in range(len(timestamps) - rate_limit):
            # Popping from right to keep earliest request time
            redis_client.rpop(redis_key)    

    # Assign current_window.  If timestamps is empty make current_window bigger than time_limit
    current_window = request_time - timestamps[0] if timestamps else time_limit + 1

    if len(timestamps) < rate_limit:  
        redis_client.rpush(redis_key, request_time)
        return True
    else:
        # May want to buffering with an additional time_buffer
        time_buffer = 1
        if current_window - time_buffer  < time_limit: # Not enough time has passed.  Deny and sleep.
            # sleep_time = time_limit - current_window + time_buffer
            sleep_time = time_limit - current_window + time_buffer
            # Deny and sleep for sleep_time
            time.sleep(sleep_time)
            return False
        else: # Enough time has passed.  Allow request.  
            # Pop the previous oldest timestamp
            previous_oldest = redis_client.lpop(redis_key)
            # Push current time into timestamps
            redis_client.rpush(redis_key, request_time)
            return True
            
            
### Formatting functions


def isoformat_to_seconds(*datetimes):
    """
    Used by ct_bundled_posts_to_minio and ct_post_details_to_minio.

    Convert datetime objects to string format compatible with API call requirements and object naming

    """

    # Check for empty arguments
    if not datetimes:
        raise ValueError("No datetime objects provided.")
    try:
        if any(not isinstance(dt, datetime) for dt in datetimes):
            raise ValueError("Can only isoformat datetime objects.")

        if len(datetimes) == 1 and isinstance(datetimes[0], datetime):
            # If only one argument is passed and it is a datetime object, convert it to isoformat
            formatted_datetime = datetimes[0].isoformat(timespec="seconds")
            return formatted_datetime
        else:
            # If multiple arguments convert all to isoformat
            formatted_datetimes = [dt.isoformat(timespec="seconds") for dt in datetimes]
            return formatted_datetimes
    except ValueError as e:
        print(f"Error: {e}")
        raise


#### Database functions


def create_sqlalchemy_engine():
    """ """
    PGUSER, PGPASSWD, PGHOST, PGPORT, PGDB = load_db_credentials()
    engine_string = f"postgresql://{PGUSER}:{PGPASSWD}@{PGHOST}:{PGPORT}/{PGDB}"
    return create_engine(engine_string)
    

#### Sliding window

class SQLSlidingWindowRateLimiter:
    def __init__(self, max_requests, interval_seconds, db_params):
        self.max_requests = max_requests
        self.interval_seconds = interval_seconds
        self.timestamps = deque()
        self.db_params = db_params

        # Create or connect to the PostgreSQL database
        self.connection = psycopg2.connect(**self.db_params)
        self.create_table()

    def create_table(self):
        # Create a table to store the timestamp counter
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS rate_limit (
            id SERIAL PRIMARY KEY,
            timestamp DOUBLE PRECISION
        );
        '''
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)

        # Query PostgreSQL to fetch the latest timestamp
        select_latest_timestamp_query = 'SELECT MAX(timestamp) FROM rate_limit;'
        with self.connection.cursor() as cursor:
            cursor.execute(select_latest_timestamp_query)
            result = cursor.fetchone()
            if result and result[0] is not None:
                # Set the timestamps deque with the latest timestamp
                self.timestamps = deque([result[0]])

        self.connection.commit()

    def is_allowed(self):
        current_time = time.time()

        # Remove timestamps that are older than the specified interval
        while self.timestamps and self.timestamps[0] < current_time - self.interval_seconds:
            self.timestamps.popleft()

        # Check if the number of requests is within the limit
        if len(self.timestamps) < self.max_requests:
            # Add the current timestamp to the deque
            self.timestamps.append(current_time)

            # Store the timestamp in the database
            self.store_timestamp(current_time)

            return True
        else:
            return False

    def store_timestamp(self, timestamp):
        # Store the timestamp in the database
        insert_query = 'INSERT INTO rate_limit (timestamp) VALUES (%s);'
        with self.connection.cursor() as cursor:
            cursor.execute(insert_query, (timestamp,))
        self.connection.commit()
