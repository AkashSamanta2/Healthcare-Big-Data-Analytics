#!/bin/bash
# Runs INSIDE the namenode container. Copies raw MIMIC files into HDFS.
set -e
hdfs dfs -mkdir -p /data/mimic/raw/hosp /data/mimic/raw/icu
hdfs dfs -put -f ./data/raw/hosp/*.csv.gz /data/mimic/raw/hosp/
hdfs dfs -put -f ./data/raw/icu/*.csv.gz  /data/mimic/raw/icu/
hdfs dfs -ls -R /data/mimic/raw
