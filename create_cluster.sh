#!/bin/bash

gcloud dataproc clusters create babby --region us-central1 --zone us-central1-f  --master-boot-disk-size=10GB --master-boot-disk-type=pd-standard --master-machine-type=n1-standard-1 --worker-boot-disk-type=pd-standard --worker-boot-disk-size=10GB --worker-machine-type=n1-standard-1 --num-workers=2 --initialization-actions gs://babby-bucket/initialization.sh
