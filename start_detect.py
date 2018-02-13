import boto3
import os
from datetime import datetime as dt

# Specifies the minimum confidence that Rekognition Video must have in order to return
# a detected label. Confidence represents how certain Amazon Rekognition is that a label
# is correctly identified. 0 is the lowest confidence. 100 is the highest confidence.
# Rekognition Video doesn't return any labels with a confidence level lower than this specified value.
MIN_CONFIDENCE = 90.0
MAX_LABELS = os.environ.get('MAX_LABELS', 123)

# User environment params
REGION = os.environ.get('AWS_REGION', 'us-east-1')
SNS_TOPIC_ARN = os.environ.get('AWS_SNS_TOPIC_ARN')
ROLE_ARN = os.environ.get('AWS_ROLE_ARN')


def start_face_detect(bucket, key, client=None, client_token=None, region=REGION):
    print('START_FACE_DETECT with bucket={} key={} client_token={}'.format(bucket, key, client_token))

    # create a rekognition client if it was not provided
    if client is None:
        print('Creating Rekognition Client ...')
        client = boto3.client('rekognition', region)

    # create a dummy client token if not provided
    if client_token is None:
        t = int(dt.utcnow().timestamp())
        client_token = str(t)
        print('Using dummy client_token=', t)

    # Create the async face detection job
    response = client.start_face_detection(
        Video={
            'S3Object': {
                'Bucket': bucket,
                'Name': key,
                 # 'Version': string
            }
        },
        ClientRequestToken=client_token,
        NotificationChannel={
            'SNSTopicArn': SNS_TOPIC_ARN,
            'RoleArn': ROLE_ARN
        },
        FaceAttributes='DEFAULT',  # ALL or DEFAULT
        JobTag='jobtag-' + client_token
    )

    job_id = response['JobId']
    print('START_FACE_DETECTION client_token={} now has job_id={}\n'.format(client_token, job_id))


def start_person_tracking(bucket, key, client=None, client_token=None, region=REGION):
    print('START_PERSON_TRACKING with bucket={} key={} client_token={}'.format(bucket, key, client_token))

    # create a rekognition client if it was not provided
    if client is None:
        print('Creating Rekognition Client ...')
        client = boto3.client('rekognition', region)

    # create a dummy client token if not provided
    if client_token is None:
        t = int(dt.utcnow().timestamp())
        client_token = str(t)
        print('Using dummy client_token=', t)

    # Create the async label detection job
    response = client.start_person_tracking(
        Video={
            'S3Object': {
                'Bucket': bucket,
                'Name': key,
                 # 'Version': string
            }
        },
        ClientRequestToken=client_token,
        NotificationChannel={
            'SNSTopicArn': SNS_TOPIC_ARN,
            'RoleArn': ROLE_ARN
        },
        JobTag='jobtag-' + client_token
    )

    job_id = response['JobId']
    print('START_PERSON_TRACKING client_token={} now has job_id={}\n'.format(client_token, job_id))


def start_label_detect(bucket, key, client=None, client_token=None, region=REGION):
    print('START_LABEL_DETECT with bucket={} key={} client_token={}'.format(bucket, key, client_token))

    # create a rekognition client if it was not provided
    if client is None:
        print('Creating Rekognition Client ...')
        client = boto3.client('rekognition', region)

    # create a dummy client token if not provided
    if client_token is None:
        t = int(dt.utcnow().timestamp())
        client_token = str(t)
        print('Using dummy client_token=', t)

    # Create the async label detection job
    response = client.start_label_detection(
        Video={
            'S3Object': {
                'Bucket': bucket,
                'Name': key,
                 # 'Version': string
            }
        },
        MinConfidence=MIN_CONFIDENCE,
        ClientRequestToken=client_token,
        NotificationChannel={
            'SNSTopicArn': SNS_TOPIC_ARN,
            'RoleArn': ROLE_ARN
        },
        JobTag='jobtag-' + client_token
    )

    job_id = response['JobId']
    print('START_LABEL_DETECT client_token={} now has job_id={}\n'.format(client_token, job_id))


def detect_labels(bucket, key):
    rekognition = boto3.client('rekognition', REGION)
    response = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key,
            }
        },
        MaxLabels=MAX_LABELS,
        MinConfidence=MIN_CONFIDENCE,
    )
    return response['Labels']
