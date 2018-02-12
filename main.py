#!/usr/bin/env python3
"""
Trying out some the AWS Rekognition APIs.

Rekognition Video enables you to automatically identify thousands of objects - such as vehicles
or pets - and activities - such as celebrating or dancing - and provides you with timestamps and a
confidence score for each label. It also relies on motion and time context in the video to accurately
identify complex activities, such as “blowing a candle” or “extinguishing fire”.

As a rule of thumb, please ensure that the smallest object or face present in the image is at
least 5% of the size (in pixels) of the shorter image dimension. For example, if you are working
with a 1600x900 image, the smallest face or object should be at least 45 pixels in either dimension.

References:
https://gist.github.com/alexcasalboni/0f21a1889f09760f8981b643326730ff

"""
import os
import time

import boto3
import start_detect
import result_handler

BUCKET = os.environ.get('AWS_S3_BUCKET')


def main():

    # Some rekognition client functions for face search require boto3 >= 1.5.10
    print('Using boto3 version: ', boto3.__version__)

    # Start the SQS result handler thread
    t = result_handler.RekogResultListener()

    # Do a label-detect on a single jpg in S3
#    for label in detect_labels(BUCKET, 'human_present.jpg'):
#        print("{Name} - {Confidence:.1f}%".format(**label))

    # Analyze some mp4 video
    # file = 'DT inside back door_20180109_080816.mp4'

    file = 'monon_person_dog_20180130_122001.mp4'
#    file = 'monon_1_bike_20180130_121732.mp4'
    file = 'monon_1_person_20180130_120128.mp4'
    file = 'steph_exit_dt_20180130_124658.mp4'
    #file = 'piero_enters_bike_dt_20180130_133618.mp4'

    # start_detect.start_face_detect(BUCKET, file)
    # start_detect.start_label_detect(BUCKET, file)
    start_detect.start_person_tracking(BUCKET, file)

    try:
        print('Ctrl-C to exit ...')
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\n\n--- test aborted by user ---\n\n')

    # SQS handler thread cleanup
    t.stop()


if __name__ == '__main__':
    main()
