from io import BytesIO

import pandas as pd
from boto3 import boto3

from config import (API, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                    S3_BUCKET_NAME, S3_ENDPOINT_URL)


def _get_client_s3():
    return boto3.resource(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

def _create_bucket_s3(s3, bucket_name: str) -> None:
    if not s3.Bucket(bucket_name) in s3.buckets.all():
        s3.create_bucket(Bucket=bucket_name)

def _base_url(endpoint: str) -> str:
    return f"{API}{endpoint}"

def _exist_file(filename: str) -> bool:
    s3 = _get_client_s3()
    bucket = s3.Bucket(S3_BUCKET_NAME)
    return any(obj.key == filename for obj in bucket.objects.all())
def _save_data(df: pd.DataFrame, filename: str) -> None:
    s3 = _get_client_s3()

    bucket = s3.Bucket(S3_BUCKET_NAME)
    bucket.put_object(Key=filename, Body=df.to_parquet(index=False), ContentType="application/octet-stream")

def _read_data(filename: str) -> pd.DataFrame:
    s3 = _get_client_s3()
    obj = s3.Object(S3_BUCKET_NAME, filename)
    return pd.read_parquet(BytesIO(obj.get()["Body"].read()))


