#!/bin/bash
# Runs INSIDE the hive-server container. Executes every .hql in sql/analytics
# and saves the result as data/local_cache/<name>.tsv
mkdir -p /workspace/data/local_cache
for f in /workspace/sql/analytics/*.hql; do
  name=$(basename "$f" .hql)
  echo ">> $name"
  /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --outputformat=tsv2 --silent=true \
     -f "$f" > "/workspace/data/local_cache/${name}.tsv"
done
echo "Done. Files:"; ls -l /workspace/data/local_cache/*.tsv
