from boto3 import client
from time import sleep

conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3
for key in conn.list_objects(Bucket='gwre-rpa-aiver')['Contents']:
    print(key['Key'])

lambda_call = client('lambda', region_name='us-west-2')
