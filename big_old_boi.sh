#!/bin/bash


for num_workers in 2 4 8 16; do
    for num_cores in 2 4; do
        echo "Cluster with $num_workers workers, $num_cores cores"
        echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

        ./create_cluster.sh $num_workers $num_cores

        for input_file in 2048.txt 8192.txt 32768.txt 131072.txt; do
            ./submit_test.sh gs://babby-bucket/Inputs/$input_file
        done

        ./delete_cluster.sh
    done
done
