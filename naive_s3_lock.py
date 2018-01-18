import boto3
from botocore.exceptions import ClientError


class NaiveS3Lock(object):
    """Manages a lock object on S3.
    It can be used to prevent subsequent executions of a script after an error occurred.
    DO NOT USE THIS TO MANAGE CONCURRENCY."""

    def __init__(self, bucket_name, key):
        self.bucket_name = bucket_name
        self.bucket = boto3.resource('s3').Bucket(bucket_name)
        self.key = key

    def lock(self):
        try:
            boto3.client('s3').head_object(Bucket=self.bucket_name, Key=self.key)
        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                self.bucket.put_object(Key=self.key)
                return  # successfully locked
            else:
                print('Could not check for lock "{}" in bucket "{}".'.format(self.key, self.bucket))
                raise
        # s3.head_object() did not raise an error, so the lock file still exists
        raise FileExistsError('S3 Bucket is locked. Fix previous error and remove lock "{}".'.format(self.key))

    def unlock(self):
        self.bucket.delete_objects(Delete={'Objects': [{'Key': self.key}]})
