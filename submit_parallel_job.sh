#!/bin/bash

INPUT="$1"

gcloud dataproc jobs submit pyspark \
gs://babby-bucket/scripts/spark.py \
    --cluster=babby \
       --region us-central1 \
       --py-files=gs://babby-bucket/scripts/kw.py,gs://babby-bucket/scripts/create_graphs.py,gs://babby-bucket/scripts/spark.py,gs://babby-bucket/scripts/sequential.py \
       --files=gs://babby-bucket/Inputs/8.txt,gs://babby-bucket/Inputs/128.txt,gs://babby-bucket/Inputs/32.txt,gs://babby-bucket/Inputs/512.txt,gs://babby-bucket/Inputs/2048.txt \
       -- $INPUT 6

