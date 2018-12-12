#!/bin/bash

NUM_WORKERS=$1
NUM_CORES=$2

gcloud dataproc clusters create babby --region us-central1 --zone us-central1-f  --master-boot-disk-size=10GB --master-boot-disk-type=pd-standard --master-machine-type=n1-standard-1 --worker-boot-disk-type=pd-standard --worker-boot-disk-size=10GB --worker-machine-type=n1-standard-$NUM_CORES --num-workers=$NUM_WORKERS --initialization-actions gs://babby-bucket/initialization.sh
