# s3_gcs_transfers.py 

This script creates Google Storage Transfer jobs for source buckets in S3.

Inputs as environment vars:

* `GOOGLE_PROJECT_ID`
* `AWS_ACCESS_ID`
* `AWS_SECRET_KEY`
* `S3_BUCKETS` : a comma separated list of buckets


### Install 
Git clone, pip install the requirements, and run. You may want to do this in a virtualenv.

     git clone https://github.com/cloudkite-io/s3-gcs-transfers
     cd s3-gcs-transfers
     pip install -r requirements.txt

This script also assumes the runner already has a local Google Cloud OAuth token:

* Install gcloud CLI: https://cloud.google.com/sdk/
* `gcloud beta auth application-default login`


### Usage 
Google needs a set of AWS credentials that has read access to the buckets you want to backup. You'll need to supply them
as environment vars to s3_gcs_transfers.py:

    GOOGLE_PROJECT_ID=<My-Google-Project_Id> AWS_ACCESS_ID=<aws_access_id> AWS_SECRET_KEY=<aws_secret_key> S3_BUCKETS=<bucket1, bucket2, bucket3> python s3_gcs_transfers.py


### Help
CloudKite is happy to help. Reach out at any time: https://cloudkite.io
