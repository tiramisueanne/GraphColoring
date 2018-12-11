#!/bin/bash

./create_cluster.sh
./submit_test.sh gs://babby-bucket/Inputs/128.txt 
./delete_cluster.sh
