import urllib
import re
import boto3
from botocore.exceptions import ClientError


def log_print(msg):
    """Print with flush for better real-time logging in containerized environments."""
    print(str(msg), flush=True)

def parse_uri(uri):
    """Parse GCS or S3 URI into bucket and key/path components."""
    parsed = urllib.parse.urlparse(uri)
    bucket = parsed.netloc
    path = re.sub(r'/{2,}', '/', parsed.path.lstrip('/'))
    return bucket, path

def s3_file_exists(bucket, key):
    """Check if a file exists in S3."""
    try:
        session = boto3.session.Session()
        s3 = session.client("s3")
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        raise