"""

    We don't recommend getting the request completion status by repeatedly calling the Rekognition
    Video Get operation. This is because Rekognition Video throttles the Get operation if too many
    requests are made. If you're processing multiple videos concurrently, it's simpler and more
    efficient to monitor one SQS queue for the completion notification than to poll Rekognition
    Video for the status of each video individually.

    # Sample message.body
    #
    # "Type": "Notification",
    # "MessageId": "87805724-7a33-5725-8d12-05f63e6f0f53",
    # "TopicArn": "arn:aws:sns:us-east-1:275072183127:AmazonRekognition-Result",
    # "Message": "{\"JobId\":\"f40f6c76f87f5c9aeea9a50b3d53688a4174711f9b1f20392c044e21f71c5e4d\",\"Status\":\"SUCCEEDED\",\"API\":\"StartFaceDetection\",\"JobTag\":\"jobtag-1515751503\",\"Timestamp\":1515733532180,\"Video\":{\"S3ObjectName\":\"DT inside back door_20180109_080816.mp4\",\"S3Bucket\":\"vsaas-rekog-test\"}}",
    # "Timestamp": "2018-01-12T05:05:32.281Z",
    # "SignatureVersion": "1",
    # "Signature": "PqHbNU5Z3d7AxX2Vub82oO7gWjRLb9i1heyk4jECsbw0UX/RERLetE2d7PKFPhGkrImalyRps6PXfelVIvmoh2nm1o2Eji/a2IrOlG1GijLTbmCs7OAjYFUxiaakC5JUrocw6fQRhKtuw7LbuKRLcVjUqSEePq7/HvqQLBZ14gpv2icTsv47tluxelTcTJQ//ME+Jkpzj6k8UDvExJ9zltHF8+lLC482eVhZ4LvdXdSMmSfd4QSmr8tlZn9BqvboVpt7VR8pbBo6W7t9P9LYsDmw4s9nMepXIbKI/85XVTi+Pq0/XUGQXbmdIqa0s3dqKuHi3pNpm42/ReQ1+5h1wA==",
    # "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-433026a4050d206028891664da859041.pem",
    # "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:275072183127:AmazonRekognition-Result:a0f69abb-a2e0-4144-b592-e0c182b4026c"

"""
import boto3
import os
import json
from datetime import datetime as dt
from threading import Thread
import pprint

REGION = os.environ.get('AWS_REGION', 'us-east-1')
MAX_LABELS = os.environ.get('MAX_LABELS', 123)
pp = pprint.PrettyPrinter(indent=4)


def on_label_detect(rekog_client, job_id, job_tag):
    unique_labels = set()

    # get paginated results
    next_token = ''
    result = None
    while next_token is not None:
        print('Getting paged label_detect result')
        result = rekog_client.get_label_detection(
            JobId=job_id,
            MaxResults=MAX_LABELS,
            NextToken=next_token,
            SortBy='TIMESTAMP'  # 'TIMESTAMP' or 'NAME'
        )
        next_token = result.get('NextToken')

        last_ts = -1
        for label in result['Labels']:
            # inspect the timestamps - should span entire clip duration
            ts = int(label['Timestamp'])
            if ts > last_ts:
                print('Timestamp = ', ts)
                last_ts = ts

            unique_labels.add(label['Label']['Name'])

    print('-------------------- Results from Rekognition.StartFaceDetection ------------------')
    print('{Codec} {DurationMillis} {Format} {FrameHeight} {FrameRate} {FrameWidth}'.format(**result['VideoMetadata']))
    print('DetectLabel Unique Label results for job_id={}'.format(job_id))
    pp.pprint(unique_labels)
    return unique_labels


def on_face_detect(rekog_client, job_id, job_tag):
    # get paginated results
    next_token = ''
    while next_token is not None:
        print('Getting paged face_detect result')
        result = rekog_client.get_face_detection(
            JobId=job_id,
            MaxResults=MAX_LABELS,
            NextToken=next_token
        )
        next_token = result.get('NextToken')

        # Perform clip/face tagging somewhere here
        print('FaceDetect results for job_id={}'.format(job_id))
        pp.pprint(result)


def on_person_track(rekog_client, job_id, job_tag):
    # get paginated results
    next_token = ''

    print('-------------------- Results from Rekognition.StartPersonTracking ------------------')
    print('PersonTrack results for job_id={}'.format(job_id))

    while next_token is not None:
        print('Getting paged person_track result')
        result = rekog_client.get_person_tracking(
            JobId=job_id,
            MaxResults=MAX_LABELS,
            NextToken=next_token,
            SortBy='TIMESTAMP'  # 'INDEX' or 'TIMESTAMP' default is TIMESTAMP
        )
        next_token = result.get('NextToken')

        # Perform clip/face tagging somewhere here
        pp.pprint(result)


def lambda_handler(event, context):
    """
    AWS Lambda function to handle Rekognition Analysis Results
    :param event:
    :param context:
    :return:
    """
    outer_msg = json.loads(event.body)
    rekog_msg = json.loads(outer_msg['Message'])
#    msg_timestamp = dt.fromtimestamp(outer_msg['Timestamp'])
    api_name = rekog_msg['API']
    job_id = rekog_msg['JobId']
    job_tag = rekog_msg['JobTag']

    if 'SUCCEEDED' not in rekog_msg['Status']:
        print('FAILED Rekognition.{} job_id={} job_tag={}'.format(api_name, job_id, job_tag))
        return

    rekog_client = boto3.client('rekognition', REGION)

    # Only handle selected rekog api results.  Discard anything else.
    if 'StartLabelDetect' in api_name:
        on_label_detect(rekog_client, job_id, job_tag)
    elif 'StartFaceDetect' in api_name:
        on_face_detect(rekog_client, job_id, job_tag)
    elif 'StartPersonTracking' in api_name:
        on_person_track(rekog_client, job_id, job_tag)
    else:
        print('No result handler for api_name={}'.format(api_name))


class RekogResultListener(Thread):
    """
    This will wait for a Rekognition completion message to arrive on SQS.
    We assume that the SQS Queue has been previously subscribed to a SNS Topic that receives these
    completion messages from Rekognition video operations.
    Refer to https://docs.aws.amazon.com/rekognition/latest/dg/video-analyzing-with-sqs.html
    """
    SQS_QUEUE_NAME = 'RekognitionQueue'

    def __init__(self):
        super(self.__class__, self).__init__(name='rekog_listener')

        # Create SQS resource
        self.sqs = boto3.resource('sqs')
        # Get URL for SQS queue
        self.q = self.sqs.get_queue_by_name(QueueName=self.SQS_QUEUE_NAME)
        self.abort = False
        self.start()

    def stop(self):
        self.abort = True
        self.join()

    def run(self):
        """
        :param timeout:
        :return:
        """
        print('Thread start ', self.name)

        while not self.abort:
            messages = self.q.receive_messages()  # Could be 0 to 10 max of messages
            for msg in messages:
                lambda_handler(msg, None)
                msg.delete()

        print('Thread stop ', self.name)

