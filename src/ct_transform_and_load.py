#!/home/pscripts/venv/bin/python

from ctetl.ct_helpers import create_minio_client, check_minio_buckets, create_minio_tags
from ctetl.ct_helpers import get_minio_object_names, get_minio_response_js
from ctetl.ct_tl import transform_post_details, queries_for_insert, insert_to_postgres


def main():
    # Create MinIO client
    minio_client = create_minio_client()

    # Input from details_bucket
    details_bucket = "ct-post-details"

    # Proceed only if both bucket are found
    check_minio_buckets(minio_client, details_bucket)

    # Prepare to tag post details to keep from processing them more than once
    tags = create_minio_tags()

    # Get post detail object names saved in MinIO and loop through each to process
    detail_object_names = get_minio_object_names(minio_client, details_bucket)

    for detail_object_name in detail_object_names:
        tagged = minio_client.get_object_tags(details_bucket, detail_object_name)
        if not tagged:
            minio_response_js = get_minio_response_js(
                detail_object_name, details_bucket, minio_client
            )
            insert_to_postgres(
                *queries_for_insert(),
                *transform_post_details(minio_response_js, detail_object_name)
            )

            # Tag the object to prevent reprocessing
            minio_client.set_object_tags(details_bucket, detail_object_name, tags)


if __name__ == "__main__":
    main()
