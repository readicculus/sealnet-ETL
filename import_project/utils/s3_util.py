# === helper functions section ===
# check if a bucket has the given key and if is in database
import os

import cv2
from botocore.exceptions import ClientError


def check(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        return int(e.response['Error']['Code']) != 404
    return True

# upload a cv2 image to bucket without saving to disk
def put_image(s3_client, bucket_name, im, image_key):
    is_success, im_buf_arr = cv2.imencode(".png", im)
    byte_im = im_buf_arr.tobytes()
    response = s3_client.put_object(Bucket=bucket_name, Key=image_key, Body=byte_im, ContentType='image/png')
    print("Response: {}".format(response))  # See result of request.

# def key_exists(s3_client, bucket, key):
#     try:
#         s3_client.head_object(Bucket=bucket, Key=key)
#     except ClientError:
#         return False
#     return True
#
# def upoload_s3(s3, s3_client, s3_bucket, src, dst):
#     if not key_exists(s3_client, s3_bucket, dst):
#         print("Uploading %s -> %s" % (src, dst))
#         s3.meta.client.upload_file(src, 'noaa-data', dst)