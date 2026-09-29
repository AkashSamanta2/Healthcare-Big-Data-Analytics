#!/bin/bash
# Runs INSIDE the hive-server container.
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /workspace/sql/ddl/create_tables.hql
