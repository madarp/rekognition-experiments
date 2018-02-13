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

"""
import boto3
import os
import json
from datetime import datetime as dt
from threading import Thread

import subprocess
import pprint

REGION = os.environ.get('AWS_REGION', 'us-east-1')
MAX_LABELS = os.environ.get('MAX_LABELS', 123)
LOCAL_DIR = '/Users/piero/Documents/exacqVision Files/'
pp = pprint.PrettyPrinter(indent=4)


def on_label_detect(rekog_client, rekog_msg):
    unique_labels = set()
    job_id = rekog_msg['JobId']
    next_token = ''
    result = None

    while next_token is not None:
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

    print('-------------------- Results from Rekognition.StartLabelDetection ------------------')
    print('job_id={}'.format(job_id))
#    print('{Codec} {DurationMillis} {Format} {FrameHeight} {FrameRate} {FrameWidth}'.format(**result['VideoMetadata']))
    pp.pprint(unique_labels)
    return unique_labels


def on_face_detect(rekog_client, rekog_msg):
    # get paginated results
    next_token = ''
    job_id = rekog_msg['JobId']

    videofile = LOCAL_DIR + rekog_msg['Video']['S3ObjectName']
    ffmpeg_boxes = []

    print('-------------------- Results from Rekognition.StartFaceDetection ------------------')
    print('job_id={}'.format(job_id))

    while next_token is not None:
        result = rekog_client.get_face_detection(
            JobId=job_id,
            MaxResults=MAX_LABELS,
            NextToken=next_token
        )
        next_token = result.get('NextToken')

        if os.path.exists(videofile):
            ffmpeg_boxes.extend(convert_to_ffmpeg_boxes(result['VideoMetadata'], result['Persons']))

    render_ffmpeg_bounding_boxes(videofile, ffmpeg_boxes)


def on_person_track(rekog_client, rekog_msg):
    # get paginated results
    next_token = ''
    job_id = rekog_msg['JobId']
    videofile = LOCAL_DIR + rekog_msg['Video']['S3ObjectName']
    ffmpeg_boxes = []

    print('-------------------- Results from Rekognition.StartPersonTracking ------------------')
    print('job_id={}'.format(job_id))

    while next_token is not None:
        result = rekog_client.get_person_tracking(
            JobId=job_id,
            MaxResults=MAX_LABELS,
            NextToken=next_token,
            SortBy='TIMESTAMP'  # 'INDEX' or 'TIMESTAMP' default is TIMESTAMP
        )
        next_token = result.get('NextToken')

        if os.path.exists(videofile):
            ffmpeg_boxes.extend(convert_to_ffmpeg_boxes(result['VideoMetadata'], result['Persons']))

    render_ffmpeg_bounding_boxes(videofile, ffmpeg_boxes)


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
        on_label_detect(rekog_client, rekog_msg)
    elif 'StartFaceDetect' in api_name:
        on_face_detect(rekog_client, rekog_msg)
    elif 'StartPersonTracking' in api_name:
        on_person_track(rekog_client, rekog_msg)
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


def convert_to_ffmpeg_boxes(videometa, persons):
    """
    Takes a list of Persons from Rekognition PersonTracking output (sorted by TIMESTAMP)
    and extracts the bounding boxes for each indexed person, then formats the list of boxes
    so that they can be ingested by ffmpeg as a linear video filter.
    :param videometa: a dict of video metadata parameters to convert from normalized to pixel
    :param persons: a list of person bounding box data sorted by TIMESTAMP, then INDEX
    :return: A list of strings that contain ffmpeg
    """
    frame_height = videometa['FrameHeight']
    frame_width = videometa['FrameWidth']
    fps = int(round(videometa['FrameRate'], 0))
    box_colors = [
        'yellow', 'red', 'white', 'green', 'lightcyan',
        'limegreen', 'magenta', 'lightpink', 'lightsalmon', 'yellowgreen'
    ]  # TODO only handles 10 persons per image

    # Assumes that persons is a list of persons and their bounding box timelines.
    prev_frame = None
    ffmpeg_boxes = []
    for person in persons:
        bb = person['Person'].get('BoundingBox')

        # It seems that sometimes, Rekognition can mix Person and Face detection results together.
        # In this case there will exist an extra 'Face' dict in each person element.  Just discard for now.
        if bb is None:
            print('WARNING found a face box inside person data')
            continue

        # convert box coordinates from normalized floats, to pixel ints
        box_height = int(round(bb['Height'] * frame_height, 0))
        box_width = int(round(bb['Width'] * frame_width, 0))
        box_left = int(round(bb['Left'] * frame_width, 0))
        box_top = int(round(bb['Top'] * frame_height, 0))

        i = person['Person']['Index']
        if i >= len(box_colors):
            print('ERROR person index is out of range:', i)
            continue

        frame_num = int(person['Timestamp'] * fps / 1000)
        if prev_frame is None:
            prev_frame = frame_num

        # Create an ffmpeg style drawbox filter notation for each person-box.
        # Since the drawbox filter supports timeline edit notation, I keep the box visible from
        # for the span of frames since the previous box was rendered.
        # TODO Timeline notation does not account for separate person indexes
        ffmpeg_boxes.append("drawbox=enable='between(n,{},{})':x={}:y={}:w={}:h={}:color={}".format(
            prev_frame, frame_num, box_left, box_top, box_width, box_height, box_colors[i]
        ))
        prev_frame = frame_num + 1

    return ffmpeg_boxes


def render_ffmpeg_bounding_boxes(infile, ffmpeg_boxes):
    # see https://stackoverflow.com/questions/17339841/ffmpeg-drawbox-on-a-given-frame
    outfile = infile.replace('.mp4', '.bb.mp4')
    print('Render {} bounding boxes to file {}'.format(len(ffmpeg_boxes), outfile))

    if not os.path.exists(infile):
        print('Bounding Box ERROR: Local file not found:', infile)
        return

    if len(ffmpeg_boxes) == 0:
        print('No bounding boxes to render.', infile)
        return

    # write drawbox to a filter script file
    boxstr = ',\n'.join([box for box in ffmpeg_boxes])
    with open('filter_file.txt', 'w') as f:
        f.write('[in]' + boxstr + '[out]')

    ffmpeg_cmdline = [
        'ffmpeg',
        '-y',
        '-loglevel', 'quiet',
        '-i', '\"' + infile + '\"',
        '-filter_script', 'filter_file.txt',
        '-codec:a', 'copy',
        '\"' + outfile + '\"'
        ]

    try:
        subprocess.check_call(ffmpeg_cmdline)
    except subprocess.CalledProcessError as e:
        if e.returncode:
            print('Failed to render bounding boxes.')
            return

    print('Bounding box render is complete.')
