#!/bin/bash

WORKLOAD=$1
THREADS=$2

./bin/ycsb run alluxio -P workloads/workload_alluxio_${WORKLOAD} -threads ${THREADS} -s
