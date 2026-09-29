# Healthcare Big Data Analytics — run book (matches your actual docker-compose.yml)

Run everything from the VS Code terminal, from the project root `healthcare-bigdata-project/`
(the folder containing `docker/`, `src/`, `sql/`, `data/`). On Windows use PowerShell.

Your compose file's ports/images are unchanged from what already works for you — the only
additions are `container_name:` on every service (so `docker exec <name> ...` works, the way it
already did for `namenode`) and a `..:/workspace` volume so the Spark/Hive containers can see this
project folder. Apply the compose file, then:

```
cd docker
docker compose up -d
docker compose ps          # everything should say "running"/"Up"
```
UIs: NameNode http://localhost:9870 · YARN http://localhost:8088 · Spark http://localhost:8080

## One-time step after every fresh cluster: install numpy/pandas in spark-master
The `bde2020/spark-master` image doesn't ship these, and the RF feature-importance /
`toPandas()` steps need them. This installs into the running container — it's lost if you
`docker compose down` and recreate the container, so repeat it whenever you do that.
```
docker exec -u root spark-master bash -c "pip3 install --no-cache-dir numpy pandas || pip install --no-cache-dir numpy pandas"
```

## Step 1 — Data into HDFS — already done ✅
(You already ran this successfully. For reference, now that `/workspace` is mounted, next
time you can just run `docker exec namenode bash /workspace/scripts/load_hdfs.sh`.)

## Step 2 — Ingest + clean (Spark)
```
docker exec spark-master spark-submit /workspace/src/01_ingest_clean.py
```
Watch it run at http://localhost:8080. Output: Parquet under HDFS `/data/mimic/cleaned/`,
and `reports/cleaning_report.md` on your PC (keep it — you'll need these numbers in the report).

## Step 3 — Readmission label
```
docker exec spark-master spark-submit /workspace/src/02_label.py
```
Prints the 0/1 class balance for survivors — write down the exact counts, you'll cite them later.

## Step 4 — Hive: disease + utilization analytics
Give HiveServer2 ~60s after `up -d` before this works.
```
docker exec hiveserver2 bash /workspace/scripts/hive_run_ddl.sh
docker exec hiveserver2 bash /workspace/scripts/hive_run_analytics.sh
```
Results land in `data/local_cache/*.tsv`. GROUP BY queries run as MapReduce jobs — watch
them appear at http://localhost:8088.

## Step 5 — ICU analysis (Spark SQL)
```
docker exec spark-master spark-submit /workspace/src/04_icu_analysis.py
```

## Step 6 — Feature engineering
```
docker exec spark-master spark-submit /workspace/src/05_features.py
```
Prints train/test sizes and positive counts per set (patient-level split — no patient in both).

## Step 7 — Train + evaluate (LR, RandomForest, GBT)
```
docker exec spark-master spark-submit /workspace/src/06_train_eval.py
```
Produces `data/local_cache/model_results.csv`, `rf_predictions.csv`, `feature_importance.csv`,
`reports/experiment_log.md`; saves the model to HDFS `/data/mimic/models/readmission_rf`.
Report raw counts (tp/fp/fn/tn) alongside percentages — the test set has very few positives.

## Step 8 — Figures (on your PC, not in Docker)
```
cd ..
python -m venv .venv
.venv\Scripts\Activate.ps1          # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python src/07_visualize.py
```
PNGs land in `reports/figures/`.

## Step 9 — Reproducibility test (do this near the end, W9–W10)
```
cd docker
docker compose down -v      # wipes HDFS too, on purpose
docker compose up -d
docker exec -u root spark-master bash -c "pip3 install --no-cache-dir numpy pandas"
```
Then redo Steps 1–7 from scratch, from the committed scripts only, timing each stage.

## Everyday commands
| Task | Command |
|---|---|
| Stop, keep data | `docker compose stop` |
| Start again | `docker compose start` |
| Logs | `docker compose logs --tail 50 spark-master` |
| Shell inside a container | `docker exec -it spark-master bash` |
| Browse HDFS | `docker exec namenode hdfs dfs -ls -R /data/mimic` |
| Interactive PySpark | `docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077` |
| Interactive Hive | `docker exec -it hiveserver2 /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000` |

## Troubleshooting
| Symptom | Fix |
|---|---|
| `No such container: spark-master` | You're on the old compose file — apply the updated one (adds `container_name:`). |
| `No such file or directory: /workspace/...` | Old compose file, no volume mount yet — apply the updated one. |
| `ModuleNotFoundError: pandas` / `numpy` inside a Spark job | Redo the one-time `pip3 install` step above (lost on container recreation). |
| `bash: ...\r: command not found` | Script has CRLF line endings — in VS Code, bottom-right `CRLF` → `LF`, save. |
| `spark-submit` hangs / no resources | `docker compose ps`; `docker compose restart spark-worker`; check Docker Desktop has ≥8GB RAM. |
| Hive `Connection refused` on 10000 | HiveServer2 still starting — wait, then `docker compose logs hiveserver2`. |
| `Too few positives for CV` message | Expected on the tiny demo dataset; script falls back to fixed RF params. |
| Model AUC ≈ 1.0 | Suspect leakage — re-check features in `05_features.py`. |
