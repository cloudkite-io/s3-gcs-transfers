#!/usr/bin/env python

"""
This script creates Google Storage Transfer jobs for source buckets in S3.

Inputs as environment vars:
    * GOOGLE_PROJECT_ID
    * AWS_ACCESS_ID
    * AWS_SECRET_KEY
    * S3_BUCKETS : a comma separated list of buckets

This script is intended to be run once per source environment (dev, stage, & prod).

Requirements:
    Google Storage Transfer API enabled for the google project:
        https://console.cloud.google.com/apis/api/storagetransfer/overview?project=<Google Project>
    pip install --upgrade google-api-python-client
    Assumes the runner of the script has already created a local Google OAuth token:
        Install gcloud CLI: https://cloud.google.com/sdk/
        gcloud beta auth application-default login
"""
import os
import datetime
import json
import sys

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


def get_s3_bucket(google_project_id, s3_bucket):
    try:
        bucket = storage_client.buckets().get(bucket=s3_bucket).execute()
    except Exception:
        print('Creating a new bucket in GCS: {}...'.format(s3_bucket))
        bucket = storage_client.buckets().insert(
            project=google_project_id,
            predefinedAcl='projectPrivate',
            predefinedDefaultObjectAcl='projectPrivate',
            body={
                'name': s3_bucket,
                'location': 'US',
                'storageClass': 'NEARLINE'
            }
        ).execute()

    # We need to identify Google's service account for transfers
    service_account_email = storagetransfer_client.googleServiceAccounts().\
        get(projectId=google_project_id).execute().get('accountEmail')
    try:
        service_account_policy = storage_client.bucketAccessControls().get(
            bucket=s3_bucket,
            entity='user-{}'.format(service_account_email)
        ).execute()
    except Exception:
        # Missing bucket policy.
        print('Setting ACL on bucket {} for service account {}..'.format(s3_bucket, service_account_email))
        try:
            storage_client.bucketAccessControls().insert(
                bucket=s3_bucket,
                body={
                    'entity': 'user-{}'.format(service_account_email),
                    'role': 'WRITER'
                }
            ).execute()
        except:
            print('{}: failed to set bucket ACL for service account. Error: {}'.format(s3_bucket, _error))


def patch_gcs_transfer(google_project_id, job_name, transfer_job):
    transfer_job_patch = {}
    for key in ['description', 'transferSpec', 'status']:
        transfer_job_patch[key] = transfer_job[key]
    try:
        _body = {
            'projectId': google_project_id,
            'transferJob': transfer_job_patch,
            'updateTransferJobFieldMask': 'transferSpec,status'
        }
        storagetransfer_client.transferJobs().patch(jobName=job_name, body=_body).execute()
    except Exception as _error:
        print('Error patching jobName {}. transfer_job: \n{}\nError: {}'.format(job_name,
                                                                                transfer_job,
                                                                                _error))

def create_gcs_transfer(google_project_id, aws_access_id, aws_secret_key, s3_bucket):
    yesterday = datetime.datetime.utcnow() + datetime.timedelta(hours=-24)
    description = 'AWS S3: {} to GCS Daily Transfer'.format(s3_bucket)
    transfer_job = {
        'description': description,  # we use this as a key to check for an existing transfer
        'projectId': google_project_id,
        'transferSpec': {
            'awsS3DataSource': {
                'bucketName': s3_bucket,
                'awsAccessKey': {
                    'accessKeyId': aws_access_id,
                    'secretAccessKey': aws_secret_key,
                }
            },
            'gcsDataSink': {
                'bucketName': s3_bucket
            },
            'transferOptions': {
                'overwriteObjectsAlreadyExistingInSink': False,
                'deleteObjectsUniqueInSink': False,
                'deleteObjectsFromSourceAfterTransfer': False
            }
        },
        'schedule': {
            'scheduleStartDate': {
                'year': yesterday.year,
                'month': yesterday.month,
                'day': yesterday.day
            },
            'startTimeOfDay': {
                'hours': 10  # 5AM US Central Time
            }
        },
        'status': 'ENABLED'
    }

    # check for existing
    _filter = {'project_id': google_project_id}
    all_jobs = storagetransfer_client.transferJobs().list(filter=json.dumps(_filter)).execute()
    existing_transfer_job = False
    for job in all_jobs.get('transferJobs', []):
        if job['description'] == description:
            # Looks like job exists. Let's patch the existing one
            print('Existing job already exists with description: {}. Patching...'.format(description))
            patch_gcs_transfer(google_project_id, job['name'], transfer_job)
            existing_transfer_job = True
            continue
    if not existing_transfer_job:
        try:
            request = storagetransfer_client.transferJobs().create(body=transfer_job).execute()
            return request['description']
        except Exception as _error:
            print('Bucket: {} transfer job creation failed: {}'.format(s3_bucket, _error))


if __name__ == "__main__":
    credentials = GoogleCredentials.get_application_default()
    storage_client = discovery.build('storage', 'v1', credentials=credentials)
    storagetransfer_client = discovery.build('storagetransfer', 'v1', credentials=credentials)

    try:
        GOOGLE_PROJECT_ID = os.environ['GOOGLE_PROJECT_ID']
        AWS_ACCESS_ID = os.environ['AWS_ACCESS_ID']
        AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
        S3_BUCKETS = os.environ['S3_BUCKETS'].split(',')
    except KeyError:
        print('\nFail. Must set the following env vars: \n'
              'GOOGLE_PROJECT_ID\n'
              'AWS_ACCESS_ID\n'
              'AWS_SECRET_KEY\n'
              'S3_BUCKETS (comma separated)\n')
        sys.exit(1)

    for s3_bucket in S3_BUCKETS:
        print('Processing bucket {}...'.format(s3_bucket))
        get_s3_bucket(GOOGLE_PROJECT_ID, s3_bucket)
        transfer_name = create_gcs_transfer(GOOGLE_PROJECT_ID, AWS_ACCESS_ID, AWS_SECRET_KEY, s3_bucket)
        if transfer_name:
            print('Finished creating transfer: {}'.format(transfer_name))

