#!/bin/bash

echo "Uploading MIMIC-IV files to HDFS..."

docker exec namenode bash -c "
set -e

hdfs dfs -mkdir -p /data/mimic/raw

hdfs dfs -put -f /data/raw/hosp/*.csv.gz /data/mimic/raw/

hdfs dfs -put -f /data/raw/icu/*.csv.gz /data/mimic/raw/
"

if [ $? -eq 0 ]; then
    echo "Upload completed successfully"
else
    echo "Upload failed"
fi


docker exec namenode bash -c "
hdfs dfs -ls /data/mimic/raw
"