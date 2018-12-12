# GraphColoring
Implementation of Kuhn-Wattenhofer's Algorithm for distributed graph coloring using Spark and Cloud Dataproc. Used as a final project for Concurrency (cs378h).

### Python Files
* create_graphs.py - code to generate random graph inputs, save them to a file, and read them back from a file
* sequential.py - Implementation of naive graph coloring algorithm and sequential version of Kuhn-Wattenhofer
* spark.py - spark setup code, including reading inputs and converting them to an RDD. Also contains the main code for our Spark app.
* kw.py - Implementation of Kuhn-Wattenhofer in Spark
* test.py - correctness tests for our implementations

### Scripts
* create_cluster.sh - initialize a cluster in Cloud Dataproc
* submit_parallel_job.sh - submit our spark job
* delete_cluster.sh - delete cluster
* initialization.sh - initialization script to run on each node in the cluster
* run_spark_experiments.sh - run all spark experiments, including setup and teardown of clusters
