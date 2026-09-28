# Healthcare Analysis using Big Data Analytics — Implementation Guide

**Project:** ICU Patient Analysis · Disease Statistics · Hospital Utilization · Readmission Prediction
**Dataset:** MIMIC-III/IV Clinical Database **Demo** (PhysioNet, open access, ~100 patients)
**Stack:** Hadoop HDFS · Apache Hive · Apache Spark & Spark SQL · Spark MLlib · YARN · Python (Matplotlib/Seaborn)
**Team:** 2 members · **Duration:** 10 weeks

This guide turns the proposal's timeline into concrete, executable steps. Each part maps to one row of your timeline table, with setup commands, PySpark/HiveQL snippets, checkpoints, and a "definition of done" so you always know whether you're on track.

> **Read this before you start — sizing reality check.** The MIMIC Demo is intentionally tiny (~100 patients, roughly 130–275 hospital admissions depending on whether you use the MIMIC-III or MIMIC-IV demo). That's perfect for proving out a Hadoop/Spark/Hive pipeline end-to-end, but it means your readmission-prediction dataset will have on the order of dozens of positive cases. Don't expect production-grade model accuracy — the goal is a **correct, well-engineered pipeline** with honestly-reported, small-sample results. Say this explicitly in your report (Section 10.2) rather than letting a reviewer discover it. This single framing decision will save you from over-claiming in the presentation.

---

## Table of Contents

0. [Prerequisites &amp; Repository Structure](#0-prerequisites--repository-structure)
1. [W1–W2 — PhysioNet Access + Environment Setup](#1-w1w2--physionet-access--environment-setup)
2. [W2–W3 — Ingestion &amp; Data Cleaning (Spark)](#2-w2w3--ingestion--data-cleaning-spark)
3. [W3 — Readmission Label Definition](#3-w3--readmission-label-definition)
4. [W4–W5 — Hive Analytics: Disease &amp; Utilization Stats](#4-w4w5--hive-analytics-disease--utilization-stats)
5. [W5–W6 — ICU Patient Analysis (Spark SQL)](#5-w5w6--icu-patient-analysis-spark-sql)
6. [W6–W7 — Feature Engineering (MLlib)](#6-w6w7--feature-engineering-mllib)
7. [W7–W8 — Readmission Prediction Model Training](#7-w7w8--readmission-prediction-model-training)
8. [W8–W9 — Model Evaluation &amp; Tuning](#8-w8w9--model-evaluation--tuning)
9. [W9 — Visualization (Matplotlib/Seaborn)](#9-w9--visualization-matplotlibseaborn)
10. [W9–W10 — Buffer, Testing &amp; Report Writing](#10-w9w10--buffer-testing--report-writing)
11. [W10 — Final Presentation](#11-w10--final-presentation)
12. [Appendix A — Suggested 2-Person Role Split](#appendix-a--suggested-2-person-role-split)
13. [Appendix B — Troubleshooting Cheat Sheet](#appendix-b--troubleshooting-cheat-sheet)
14. [Appendix C — Reference Links](#appendix-c--reference-links)

---

## 0. Prerequisites & Repository Structure

**Before Week 1 starts:**

- [ ] Both team members have a PhysioNet account (free, email verification only — no CITI training needed for the *demo* datasets).
- [ ] Docker Desktop (or Docker Engine + Compose) installed, with at least **8 GB RAM allocated to Docker** (16 GB recommended if your laptop has it) if you're going the containerized route. If using GCP, both members added as collaborators on the project with Compute/Dataproc IAM roles.
- [ ] A shared GitHub/GitLab repo created — this is your single source of truth for code, SQL, and the report.
- [ ] Python 3.9+ locally, for driving notebooks/scripts against the cluster.

**Recommended repo layout** (create this in Week 1, it saves confusion later):

```
healthcare-bigdata-project/
├── docker/
│   └── docker-compose.yml
├── data/
│   ├── raw/                 # original MIMIC demo CSVs (mirrored into HDFS)
│   └── local_cache/         # small aggregated CSVs pulled down for plotting
├── sql/
│   ├── ddl/                 # Hive CREATE TABLE scripts
│   └── analytics/           # .hql query files for disease/utilization stats
├── src/
│   ├── ingestion/           # raw CSV -> HDFS, schema definitions
│   ├── cleaning/            # Spark cleaning jobs
│   ├── labels/              # readmission label logic
│   ├── icu_analysis/        # Spark SQL ICU notebooks/scripts
│   ├── features/            # MLlib feature pipeline
│   ├── models/              # MLlib training + evaluation
│   └── viz/                 # matplotlib/seaborn scripts
├── notebooks/                # exploratory Jupyter notebooks (not the source of truth)
├── reports/
│   └── figures/
├── README.md
└── requirements.txt
```

Keep `notebooks/` for exploration only — every step that matters for reproducibility should end up as a `.py`/`.hql` file in `src/`/`sql/`, because your Week 9–10 "reproduce the pipeline end-to-end" test depends on this.

---

## 1. W1–W2 — PhysioNet Access + Environment Setup

**Goal:** Data in hand, cluster running, everyone able to run a Spark job against HDFS and query through Hive.

### Step 1.1 — Get the MIMIC Demo data

1. Create/verify PhysioNet accounts for both members.
2. Download one of:
   - **MIMIC-IV Clinical Database Demo (v2.2)** — recommended, more current schema, split into `hosp/` (patients, admissions, diagnoses_icd, labevents, prescriptions, transfers…) and `icu/` (icustays, chartevents, d_items, inputevents, outputevents…) modules.
   - **MIMIC-III Clinical Database Demo** — single flat schema (`PATIENTS`, `ADMISSIONS`, `ICUSTAYS`, `DIAGNOSES_ICD`, `D_ICD_DIAGNOSES`, `CHARTEVENTS`, `LABEVENTS`…), fine if your course examples use III.
   - Both are open-access (no data-use training required for the demo subset), but you still must accept the license (Open Data Commons ODbL) on the PhysioNet page.
3. Pick **one** version and standardize on it across the team — mixing III and IV table names mid-project is a common source of wasted time. Record the choice in `README.md`.
4. Place the CSV/CSV.GZ files under `data/raw/`.

### Step 1.2 — Stand up the cluster

You have two paths; pick based on team comfort and cost tolerance.

**Option A — Dockerized Hadoop + Spark + Hive (recommended for a 2-person course project — free, reproducible, easy to reset).**

Use a Compose file combining known-good images for HDFS (NameNode/DataNode), YARN (ResourceManager/NodeManager/HistoryServer), Hive (metastore + HiveServer2 on a Postgres-backed metastore), and Spark (master/worker). A minimal skeleton:

```yaml
# docker/docker-compose.yml
version: "3"
services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    environment:
      - CLUSTER_NAME=mimic-cluster
    ports: ["9870:9870", "9000:9000"]
    volumes: ["namenode:/hadoop/dfs/name"]

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    environment:
      - SERVICE_PRECONDITION=namenode:9870
    depends_on: [namenode]
    volumes: ["datanode:/hadoop/dfs/data"]

  resourcemanager:
    image: bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8
    environment:
      - SERVICE_PRECONDITION=namenode:9000 namenode:9870 datanode:9864
    depends_on: [namenode, datanode]
    ports: ["8088:8088"]

  nodemanager:
    image: bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
    environment:
      - SERVICE_PRECONDITION=namenode:9000 resourcemanager:8088
    depends_on: [resourcemanager]

  hive-metastore-postgresql:
    image: bde2020/hive-metastore-postgresql:2.3.0

  hive-metastore:
    image: bde2020/hive:2.3.2-postgresql-metastore
    command: /opt/hive/bin/hive --service metastore
    environment:
      - SERVICE_PRECONDITION=namenode:9870 datanode:9864 hive-metastore-postgresql:5432
    ports: ["9083:9083"]
    depends_on: [hive-metastore-postgresql]

  hiveserver2:
    image: bde2020/hive:2.3.2-postgresql-metastore
    environment:
      - SERVICE_PRECONDITION=hive-metastore:9083
    ports: ["10000:10000"]
    depends_on: [hive-metastore]

  spark-master:
    image: bde2020/spark-master:3.1.1-hadoop3.2
    ports: ["8080:8080", "7077:7077"]

  spark-worker:
    image: bde2020/spark-worker:3.1.1-hadoop3.2
    environment:
      - SPARK_MASTER=spark://spark-master:7077
    depends_on: [spark-master]

volumes:
  namenode:
  datanode:
```

> Check Docker Hub for the current tag numbers before pulling — pin exact versions once they work, and commit `docker-compose.yml` to the repo so both teammates get an identical cluster with `docker compose up -d`.

Verify each component:

- NameNode UI: `http://localhost:9870`
- ResourceManager (YARN) UI: `http://localhost:8088`
- Spark Master UI: `http://localhost:8080`
- `docker exec -it <namenode-container> hdfs dfsadmin -report` → confirm live DataNodes.

**Option B — Google Cloud Platform (Dataproc).** If you'd rather not fight Docker networking on two different laptops:

1. Enable Dataproc, GCS, and Compute Engine APIs on a shared GCP project.
2. Create a small Dataproc cluster with Hive and Spark components (`gcloud dataproc clusters create mimic-cluster --region=<region> --optional-components=HIVE --enable-component-gateway --master-machine-type=n2-standard-2 --worker-machine-type=n2-standard-2 --num-workers=2`).
3. Use a GCS bucket as your "HDFS" (`gs://<bucket>/mimic/raw/`) — Dataproc treats GCS paths transparently for Spark/Hive, so every HDFS path in this guide becomes a `gs://` path.
4. Use the Component Gateway links for the YARN, Spark History, and Hive/HDFS UIs instead of localhost ports.
5. **Set a budget alert immediately** — leaving a cluster running overnight is the #1 way student GCP credits disappear. Stop/delete the cluster after each work session (`gcloud dataproc clusters delete`).

Either way, tune resources down for demo-sized data: in `yarn-site.xml` (or Dataproc cluster properties), set `yarn.nodemanager.resource.memory-mb` and `yarn.scheduler.maximum-allocation-mb` to something sane for a laptop (e.g., 4096 MB total is enough for this dataset), and cap `spark.executor.memory`/`spark.driver.memory` at 1–2g so the cluster doesn't hang waiting for resources it doesn't have.

### Step 1.3 — Load raw data into HDFS

```bash
# from inside the namenode container, or via `hdfs` CLI configured to talk to it
hdfs dfs -mkdir -p /data/mimic/raw/hosp /data/mimic/raw/icu
hdfs dfs -put data/raw/hosp/*.csv.gz /data/mimic/raw/hosp/
hdfs dfs -put data/raw/icu/*.csv.gz  /data/mimic/raw/icu/
hdfs dfs -ls -R /data/mimic/raw
```

### Step 1.4 — Confirm Spark ↔ HDFS ↔ Hive ↔ YARN are wired together

```bash
pyspark --master yarn --deploy-mode client
```

```python
df = spark.read.csv("hdfs://namenode:9000/data/mimic/raw/hosp/admissions.csv.gz", header=True)
df.printSchema()
df.count()
spark.sql("SHOW DATABASES").show()   # confirms Hive metastore connectivity
```

### Step 1.5 — Set up the shared dev environment

- `requirements.txt`: `pyspark`, `pandas`, `matplotlib`, `seaborn`, `jupyter`, `scikit-learn` (only for sanity-checking metrics, not for the MLlib model itself).
- Agree on one working directory convention (HDFS paths, database name `mimic`) in the README so both members' code paths match.

### ✅ End-of-W2 checkpoint

- [ ] Cluster comes up with one command (`docker compose up -d` or a documented `gcloud` command) and both members have run it successfully.
- [ ] Raw MIMIC demo CSVs are visible under `/data/mimic/raw/` in HDFS.
- [ ] A PySpark shell can read a raw CSV and Spark SQL can see the Hive metastore.
- [ ] Repo pushed with the structure from Part 0.

---

## 2. W2–W3 — Ingestion & Data Cleaning (Spark)

**Goal:** Trustworthy, typed, deduplicated Parquet tables in HDFS that everything downstream reads from — never read raw CSVs again after this step.

### Step 2.1 — Define explicit schemas

Don't let Spark infer schema on the real run (it's slow and silently mis-types columns like `subject_id` as double). Define `StructType` schemas per table, e.g.:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType

admissions_schema = StructType([
    StructField("subject_id", IntegerType()),
    StructField("hadm_id", IntegerType()),
    StructField("admittime", TimestampType()),
    StructField("dischtime", TimestampType()),
    StructField("deathtime", TimestampType()),
    StructField("admission_type", StringType()),
    StructField("admission_location", StringType()),
    StructField("discharge_location", StringType()),
    StructField("insurance", StringType()),
    StructField("race", StringType()),
    StructField("hospital_expire_flag", IntegerType()),
])

admissions = spark.read.csv(
    "hdfs://namenode:9000/data/mimic/raw/hosp/admissions.csv.gz",
    header=True, schema=admissions_schema
)
```

Repeat for `patients`, `diagnoses_icd`, `d_icd_diagnoses`, `icustays`, `labevents`, `chartevents` (only load the columns/tables you'll actually use — `chartevents`/`labevents` are the largest files even in the demo, so trim columns early with `.select(...)`).

### Step 2.2 — Data quality pass

For each table, systematically check and log:

```python
from pyspark.sql import functions as F

null_report = admissions.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in admissions.columns
])
null_report.show()

dupes = admissions.groupBy("hadm_id").count().filter("count > 1")
print("Duplicate hadm_ids:", dupes.count())
```

Common MIMIC-specific cleaning tasks:

- **Ages:** MIMIC-III shifts ages >89 to 300 for de-identification; MIMIC-IV instead buckets very old patients — check which version you have and cap/flag outlier ages rather than silently trusting them.
- **Dates:** all timestamps are date-shifted per patient for de-identification — that's expected and fine for computing *durations* (LOS, days-to-readmit), just never treat the actual calendar date as meaningful.
- **ICD codes:** MIMIC-IV mixes ICD-9 and ICD-10 — always join `diagnoses_icd` to `d_icd_diagnoses` on **both** `icd_code` and `icd_version`, or you'll get wrong/duplicate diagnosis titles.
- **Missing `dischtime`/negative LOS:** drop or flag; a negative length-of-stay means a data issue, not a real record.

### Step 2.3 — Standardize & derive

```python
admissions_clean = (admissions
    .filter(F.col("dischtime") > F.col("admittime"))
    .withColumn("los_days", F.round(
        (F.col("dischtime").cast("long") - F.col("admittime").cast("long")) / 86400.0, 2))
    .dropDuplicates(["hadm_id"])
)
```

### Step 2.4 — Persist as partitioned Parquet in HDFS

```python
admissions_clean.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/data/mimic/cleaned/admissions"
)
```

Do the same for `patients_clean`, `diagnoses_clean`, `icustays_clean`, `labevents_clean` (filtered to the item IDs you actually need), `chartevents_clean` (same). Partitioning isn't very meaningful at this data size, but keep the pattern for realism/report credibility — e.g. partition `chartevents_clean` by a coarse `charttime` bucket if you want to demonstrate partition pruning.

### Step 2.5 — Write the data dictionary

Create `reports/data_dictionary.md`: one row per table/column you kept, its type, source file, and any cleaning rule applied. You'll need this almost verbatim in the report's Methodology section — write it now, not in Week 9.

### ✅ End-of-W3 checkpoint

- [ ] Every table you'll use has a cleaned Parquet version in `/data/mimic/cleaned/`.
- [ ] A short `cleaning_report.md` documents null rates, duplicates removed, and rows dropped per table (this is evidence for your report, capture it while it's fresh).
- [ ] Data dictionary drafted.

---

## 3. W3 — Readmission Label Definition

**Goal:** One unambiguous, well-justified label column per hospital admission: `readmit_30d ∈ {0,1}`.

### Step 3.1 — Choose the definition

Use the standard, defensible definition: **an admission is a positive (`1`) if the same patient has another *unplanned* hospital admission within 30 days of this admission's discharge.** Justify in the report why 30 days (it's the CMS/industry-standard readmission window — cite it, don't just assert it).

Decisions to make explicitly (write them down — a grader/reviewer will ask):

- **Exclude deaths:** if `hospital_expire_flag = 1`, the patient cannot be readmitted — exclude that admission from the label-positive pool (it's automatically `0`/not applicable, and arguably should be excluded from the cohort entirely).
- **Exclude planned readmissions:** if the *next* admission's `admission_type` is `ELECTIVE`, don't count it as a readmission event.
- **Right-censoring:** a patient's *last* admission in the dataset might get readmitted after the data collection window ends — you can't know. Standard practice: label it `0` but document this as a known limitation (small demo dataset makes this more visible than it would be at full MIMIC scale).

### Step 3.2 — Implement with a window function

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

admissions = spark.read.parquet("hdfs://namenode:9000/data/mimic/cleaned/admissions")

w = Window.partitionBy("subject_id").orderBy("admittime")

labeled = (admissions
    .withColumn("next_admittime", F.lead("admittime").over(w))
    .withColumn("next_admission_type", F.lead("admission_type").over(w))
    .withColumn("days_to_next_admit",
        F.datediff(F.col("next_admittime"), F.col("dischtime")))
    .withColumn("readmit_30d", F.when(
        (F.col("hospital_expire_flag") == 0) &
        F.col("days_to_next_admit").isNotNull() &
        (F.col("days_to_next_admit") <= 30) &
        (F.col("days_to_next_admit") >= 0) &
        (F.col("next_admission_type") != F.lit("ELECTIVE")),
        F.lit(1)
    ).otherwise(F.lit(0)))
)
```

### Step 3.3 — Validate the label

```python
labeled.groupBy("readmit_30d").count().show()
labeled.filter("days_to_next_admit < 0").count()   # should be 0 — sanity check
```

Expect strong class imbalance (positives likely well under 20% of admissions, and in absolute terms only a handful of cases given ~100 patients). **Note this number down explicitly** — you'll need it to justify your evaluation metric choice in Week 8 (accuracy is meaningless here; AUC/F1/recall matter far more).

### Step 3.4 — Persist the labeled cohort

```python
labeled.select(
    "subject_id", "hadm_id", "admittime", "dischtime", "los_days",
    "admission_type", "hospital_expire_flag", "readmit_30d"
).write.mode("overwrite").parquet("hdfs://namenode:9000/data/mimic/cleaned/admissions_labeled")
```

### ✅ End-of-W3 checkpoint

- [ ] `admissions_labeled` Parquet table exists with a validated `readmit_30d` column.
- [ ] Class balance numbers recorded (you'll cite these repeatedly later).
- [ ] Label-definition assumptions written down in `reports/` (this becomes your Methodology subsection almost verbatim).

---

## 4. W4–W5 — Hive Analytics: Disease & Utilization Stats

**Goal:** Answer "what diseases are most common, and how is the hospital being used" via HiveQL over the cleaned data — this is where Hive earns its place in your architecture (vs. just using Spark SQL for everything).

### Step 4.1 — Register external Hive tables over your Parquet

```sql
CREATE DATABASE IF NOT EXISTS mimic;

CREATE EXTERNAL TABLE IF NOT EXISTS mimic.admissions_labeled (
  subject_id INT, hadm_id INT, admittime TIMESTAMP, dischtime TIMESTAMP,
  los_days DOUBLE, admission_type STRING, hospital_expire_flag INT, readmit_30d INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/mimic/cleaned/admissions_labeled';

CREATE EXTERNAL TABLE IF NOT EXISTS mimic.diagnoses_icd (
  subject_id INT, hadm_id INT, seq_num INT, icd_code STRING, icd_version INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/mimic/cleaned/diagnoses';

CREATE EXTERNAL TABLE IF NOT EXISTS mimic.d_icd_diagnoses (
  icd_code STRING, icd_version INT, long_title STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/mimic/cleaned/d_icd_diagnoses';
```

Repeat for `patients_clean`, `icustays_clean`. Save all DDL under `sql/ddl/`.

### Step 4.2 — Disease statistics queries (save each as a `.hql` under `sql/analytics/`)

```sql
-- Top 20 diagnoses by number of distinct admissions
SELECT d.long_title AS diagnosis, COUNT(DISTINCT dx.hadm_id) AS admission_count
FROM mimic.diagnoses_icd dx
JOIN mimic.d_icd_diagnoses d
  ON dx.icd_code = d.icd_code AND dx.icd_version = d.icd_version
GROUP BY d.long_title
ORDER BY admission_count DESC
LIMIT 20;

-- Disease prevalence by gender/age band
SELECT p.gender, d.long_title AS diagnosis, COUNT(*) AS n
FROM mimic.diagnoses_icd dx
JOIN mimic.d_icd_diagnoses d ON dx.icd_code = d.icd_code AND dx.icd_version = d.icd_version
JOIN mimic.patients_clean p ON dx.subject_id = p.subject_id
GROUP BY p.gender, d.long_title
ORDER BY n DESC
LIMIT 30;
```

### Step 4.3 — Hospital utilization queries

```sql
-- Admissions & average LOS by admission type
SELECT admission_type, COUNT(*) AS num_admissions,
       ROUND(AVG(los_days), 2) AS avg_los_days,
       ROUND(SUM(readmit_30d) / COUNT(*), 3) AS readmit_rate
FROM mimic.admissions_labeled
GROUP BY admission_type
ORDER BY num_admissions DESC;

-- ICU bed utilization: stays and average ICU LOS by care unit
SELECT first_careunit, COUNT(*) AS num_stays, ROUND(AVG(los), 2) AS avg_icu_los_days
FROM mimic.icustays_clean
GROUP BY first_careunit
ORDER BY num_stays DESC;

-- Monthly-style admission volume trend (note: dates are shifted per-patient, so bucket by relative offset, not real calendar month, if you want a genuinely meaningful trend)
SELECT DATE_FORMAT(admittime, 'yyyy-MM') AS admit_month, COUNT(*) AS num_admissions
FROM mimic.admissions_labeled
GROUP BY DATE_FORMAT(admittime, 'yyyy-MM')
ORDER BY admit_month;
```

### Step 4.4 — Performance/format notes for the report

- Convert to **ORC** instead of Parquet for a subset of the largest tables and note the file-size/query-time difference — this is easy "we evaluated storage formats" content for the report even at small scale.
- Try `ANALYZE TABLE ... COMPUTE STATISTICS` and mention cost-based optimization even if the demo data is too small to show a dramatic speedup — the point at this project's scale is demonstrating you understand *why* it matters at production scale.

### Step 4.5 — Export results

Run each `.hql` via `beeline`/`hive -f`, or `spark.sql(...)` reading the same Hive tables, and save outputs as small CSVs into `data/local_cache/` for Week 9 plotting.

### ✅ End-of-W5 checkpoint

- [ ] All Hive tables created and queryable via `beeline`/HiveServer2.
- [ ] Disease-stats and utilization `.hql` files committed under `sql/analytics/`.
- [ ] Query result CSVs saved for visualization.

---

## 5. W5–W6 — ICU Patient Analysis (Spark SQL)

**Goal:** Characterize ICU stays and outcomes — this is the piece that most directly showcases Spark SQL joins across the largest table (`chartevents`).

### Step 5.1 — Identify vital-sign item IDs you care about

Look up `d_items` (MIMIC-IV) / `D_ITEMS` (MIMIC-III) for the `itemid`s of Heart Rate, Respiratory Rate, SpO2, Systolic/Diastolic BP, Temperature. Hardcode a small dict — this is standard MIMIC practice.

```python
VITAL_ITEMIDS = {
    "heart_rate": [220045],
    "resp_rate": [220210],
    "spo2": [220277],
    "sbp": [220179, 220050],
    "temp_c": [223762],
}
```

### Step 5.2 — First-24h vitals aggregation per ICU stay

```python
from pyspark.sql import functions as F

icustays = spark.table("mimic.icustays_clean")
chart = spark.table("mimic.chartevents_clean")

first_day = (chart.join(icustays, "stay_id")
    .filter(F.col("charttime") <= F.expr("intime + interval 1 day"))
)

def agg_vital(name, itemids):
    return (first_day.filter(F.col("itemid").isin(itemids))
        .groupBy("stay_id")
        .agg(F.avg("valuenum").alias(f"avg_{name}_24h"),
             F.min("valuenum").alias(f"min_{name}_24h"),
             F.max("valuenum").alias(f"max_{name}_24h")))

vitals_24h = None
for name, ids in VITAL_ITEMIDS.items():
    v = agg_vital(name, ids)
    vitals_24h = v if vitals_24h is None else vitals_24h.join(v, "stay_id", "outer")

icu_features = icustays.join(vitals_24h, "stay_id", "left")
```

### Step 5.3 — Patient-characteristic vs. outcome analysis

```python
patients = spark.table("mimic.patients_clean")
admissions = spark.table("mimic.admissions_labeled")

icu_full = (icu_features
    .join(admissions, "hadm_id")
    .join(patients, "subject_id"))

# ICU LOS and in-hospital mortality by admission type
icu_full.groupBy("admission_type").agg(
    F.count("*").alias("n_icu_stays"),
    F.round(F.avg("los"), 2).alias("avg_icu_los_days"),
    F.round(F.avg("hospital_expire_flag"), 3).alias("mortality_rate")
).orderBy(F.desc("n_icu_stays")).show()

# ICU LOS vs age bands
icu_full.withColumn("age_band", F.when(F.col("anchor_age") < 40, "<40")
    .when(F.col("anchor_age") < 65, "40-64").otherwise("65+")
).groupBy("age_band").agg(F.round(F.avg("los"), 2).alias("avg_icu_los")).show()
```

### Step 5.4 — Persist curated table

```python
icu_full.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/data/mimic/cleaned/icu_analysis"
)
```

### ✅ End-of-W6 checkpoint

- [ ] `icu_analysis` table has first-24h vitals joined with demographics and outcomes per ICU stay.
- [ ] At least 3 ICU-outcome relationships explored and saved (e.g., LOS by unit, mortality by admission type, LOS by age band).

---

## 6. W6–W7 — Feature Engineering (MLlib)

**Goal:** A single `features` vector column per admission, ready for `pyspark.ml` estimators, with no leakage.

### Step 6.1 — Assemble the feature table (join everything to `admissions_labeled`)

Build these per-`hadm_id` features **using only information available at or before discharge of the index admission** (this is the single most important correctness rule — including anything from *after* discharge, or from the readmission itself, is data leakage):

| Feature                                           | Source                                                                     |
| ------------------------------------------------- | -------------------------------------------------------------------------- |
| `age`, `gender`                               | patients                                                                   |
| `admission_type`, `insurance`, `race`       | admissions                                                                 |
| `los_days`                                      | admissions                                                                 |
| `num_prior_admissions`                          | count of this patient's admissions with`admittime` earlier than this one |
| `num_diagnoses`                                 | count of`diagnoses_icd` rows for this `hadm_id`                        |
| `had_icu_stay` (0/1), `icu_los_days`          | icustays                                                                   |
| `avg_heart_rate_24h`, `avg_resp_rate_24h`, … | icu_analysis (Step 5)                                                      |

```python
w_prior = Window.partitionBy("subject_id").orderBy("admittime").rowsBetween(Window.unboundedPreceding, -1)
admissions_feat = admissions.withColumn("num_prior_admissions", F.count("hadm_id").over(w_prior))

diag_counts = diagnoses.groupBy("hadm_id").agg(F.count("*").alias("num_diagnoses"))

icu_flags = icustays.groupBy("hadm_id").agg(
    F.lit(1).alias("had_icu_stay"), F.sum("los").alias("icu_los_days"))

feature_df = (admissions_feat
    .join(patients, "subject_id")
    .join(diag_counts, "hadm_id", "left")
    .join(icu_flags, "hadm_id", "left")
    .join(vitals_24h_by_hadm, "hadm_id", "left")   # from Step 5, re-keyed to hadm_id
    .fillna({"num_diagnoses": 0, "had_icu_stay": 0, "icu_los_days": 0}))
```

### Step 6.2 — Encode categoricals, impute, scale

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, Imputer
from pyspark.ml import Pipeline

categorical_cols = ["gender", "admission_type", "insurance", "race"]
numeric_cols = ["age", "los_days", "num_prior_admissions", "num_diagnoses",
                "icu_los_days", "avg_heart_rate_24h", "avg_resp_rate_24h", "avg_spo2_24h"]

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in categorical_cols]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec") for c in categorical_cols]
imputer = Imputer(inputCols=numeric_cols, outputCols=[f"{c}_imp" for c in numeric_cols], strategy="median")

assembler = VectorAssembler(
    inputCols=[f"{c}_vec" for c in categorical_cols] + [f"{c}_imp" for c in numeric_cols],
    outputCol="features_raw", handleInvalid="skip")

scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

pipeline = Pipeline(stages=indexers + encoders + [imputer, assembler, scaler])
pipeline_model = pipeline.fit(feature_df)
featurized = pipeline_model.transform(feature_df)
```

### Step 6.3 — Split, and think about imbalance

```python
train, test = featurized.randomSplit([0.8, 0.2], seed=42)

pos = train.filter("readmit_30d = 1").count()
neg = train.filter("readmit_30d = 0").count()
train = train.withColumn("class_weight",
    F.when(F.col("readmit_30d") == 1, neg / (pos + neg)).otherwise(pos / (pos + neg)))
```

Given the tiny positive class, also consider a **stratified** split (sample positives/negatives separately at 80/20 each, then union) rather than a pure random split, so your test set isn't left with zero or one positive case.

### Step 6.4 — Persist

```python
pipeline_model.write().overwrite().save("hdfs://namenode:9000/data/mimic/models/feature_pipeline")
featurized.write.mode("overwrite").parquet("hdfs://namenode:9000/data/mimic/cleaned/features")
```

### ✅ End-of-W7 checkpoint

- [ ] `features` Parquet table with a single dense `features` vector column and `readmit_30d` label.
- [ ] Feature pipeline (`Pipeline`/`PipelineModel`) saved to HDFS so it can be reapplied without retraining indexers/scalers.
- [ ] Train/test split done with the class-imbalance issue explicitly handled (weights and/or stratified split), and documented.

---

## 7. W7–W8 — Readmission Prediction Model Training

**Goal:** 2–3 trained candidate models with a consistent evaluation protocol, chosen for a small, imbalanced binary classification problem.

### Step 7.1 — Baseline: Logistic Regression

```python
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

lr = LogisticRegression(featuresCol="features", labelCol="readmit_30d", weightCol="class_weight", maxIter=50)
lr_model = lr.fit(train)

evaluator = BinaryClassificationEvaluator(labelCol="readmit_30d", metricName="areaUnderROC")
auc_lr = evaluator.evaluate(lr_model.transform(test))
print("LR AUC:", auc_lr)
```

Always train and report the baseline first — it's your sanity check and your "did the fancier model actually help" comparison point.

### Step 7.2 — Tree-based models

```python
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier

rf = RandomForestClassifier(featuresCol="features", labelCol="readmit_30d", seed=42)
gbt = GBTClassifier(featuresCol="features", labelCol="readmit_30d", seed=42, maxIter=50)
```

Note: `GBTClassifier` doesn't support `weightCol` in older MLlib versions — check your Spark version; if unsupported, rely on the stratified split instead for GBT.

### Step 7.3 — Cross-validation

With only ~100 patients, **3-fold** CV is more sensible than 5- or 10-fold (folds that small get noisy fast):

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

paramGrid = (ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100])
    .addGrid(rf.maxDepth, [4, 6, 8])
    .build())

cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid,
                     evaluator=evaluator, numFolds=3, seed=42)
cv_model = cv.fit(train)
print("Best RF AUC (CV):", max(cv_model.avgMetrics))
```

Repeat the same pattern for GBT with its own small param grid.

### Step 7.4 — Track experiments

Keep a simple `reports/experiment_log.md` table as you go:

| Model              | Params                   | Train AUC | Test AUC | Notes       |
| ------------------ | ------------------------ | --------- | -------- | ----------- |
| LogisticRegression | maxIter=50               | …        | …       | baseline    |
| RandomForest       | numTrees=100, maxDepth=6 | …        | …       | best via CV |
| GBT                | maxIter=50               | …        | …       |             |

This table becomes your Results section directly — fill it in live, don't reconstruct it from memory in Week 9.

### ✅ End-of-W8 checkpoint

- [ ] At least 2 model families trained (Logistic Regression + one tree ensemble).
- [ ] Cross-validation run and best hyperparameters recorded.
- [ ] Experiment log populated with train/test AUC for every run.

---

## 8. W8–W9 — Model Evaluation & Tuning

**Goal:** Pick a final model with metrics appropriate for a small, imbalanced dataset (not accuracy), understand what drives its predictions, and save it.

### Step 8.1 — Metrics beyond AUC

```python
from pyspark.mllib.evaluation import MulticlassMetrics

best_model = cv_model.bestModel
predictions = best_model.transform(test)

preds_labels = predictions.select("prediction", "readmit_30d").rdd.map(
    lambda r: (float(r[0]), float(r[1])))
metrics = MulticlassMetrics(preds_labels)

print("Confusion matrix:\n", metrics.confusionMatrix().toArray())
print("Precision (readmit=1):", metrics.precision(1.0))
print("Recall (readmit=1):", metrics.recall(1.0))
print("F1 (readmit=1):", metrics.fMeasure(1.0))
```

With so few positive test cases, also report the **raw counts** (e.g., "3 of 5 true readmissions correctly flagged") alongside the percentages — percentages alone on tiny denominators can be misleading, and a careful reviewer will want the raw numbers.

### Step 8.2 — Feature importance

```python
importances = best_model.featureImportances
feature_names = [f"{c}_vec" for c in categorical_cols] + [f"{c}_imp" for c in numeric_cols]
# Note: OneHotEncoder expands each categorical into multiple slots — map indices back
# carefully via pipeline_model.stages[...] metadata, or report importance at the
# raw-feature-group level rather than per-one-hot-slot if the mapping gets messy.
```

Discuss which features dominate (commonly: prior admission count, LOS, number of diagnoses) — this is good report/presentation content and is more defensible clinically than a bare AUC number.

### Step 8.3 — Tune against overfitting

Compare train AUC vs. test AUC from your experiment log. If train AUC is much higher than test AUC (very likely with tree ensembles on ~80 training rows), that's expected at this data size — **explicitly discuss it as a limitation** rather than chasing a train-AUC number that won't generalize. Try `maxDepth` reduction, `minInstancesPerNode` increases, and fewer features before concluding you're just data-limited.

### Step 8.4 — Save the final model

```python
best_model.write().overwrite().save("hdfs://namenode:9000/data/mimic/models/readmission_rf")
```

Also save a short `model_card.md`: model type, features used, training data size, class balance, chosen metrics and their values, and known limitations — this is increasingly expected practice for any ML deliverable and doubles as report content.

### ✅ End-of-W9 (eval half) checkpoint

- [ ] Confusion matrix, precision/recall/F1, and AUC all recorded for the final model.
- [ ] Feature importance analysis complete with a plain-language interpretation.
- [ ] Final `PipelineModel`/classifier saved to HDFS; `model_card.md` written.

---

## 9. W9 — Visualization (Matplotlib/Seaborn)

**Goal:** A small, reusable set of publication-quality figures pulled from your Hive/Spark aggregates (never from raw multi-million-row tables — always aggregate in Spark/Hive first, then `.toPandas()`).

### Step 9.1 — Pull only aggregated results to Pandas

```python
import pandas as pd

top_diagnoses_pd = spark.sql("""
    SELECT d.long_title AS diagnosis, COUNT(DISTINCT dx.hadm_id) AS n
    FROM mimic.diagnoses_icd dx
    JOIN mimic.d_icd_diagnoses d ON dx.icd_code=d.icd_code AND dx.icd_version=d.icd_version
    GROUP BY d.long_title ORDER BY n DESC LIMIT 15
""").toPandas()
```

### Step 9.2 — Core figure set

```python
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")

# 1. Top diagnoses
plt.figure(figsize=(8,6))
sns.barplot(data=top_diagnoses_pd, y="diagnosis", x="n", color="#4C72B0")
plt.xlabel("Number of admissions"); plt.title("Top 15 Diagnoses"); plt.tight_layout()
plt.savefig("reports/figures/top_diagnoses.png", dpi=150)

# 2. Admission volume trend
plt.figure(figsize=(8,4))
sns.lineplot(data=utilization_trend_pd, x="admit_month", y="num_admissions", marker="o")
plt.xticks(rotation=45); plt.title("Admission Volume Over Time"); plt.tight_layout()
plt.savefig("reports/figures/admission_trend.png", dpi=150)

# 3. ICU LOS distribution
plt.figure(figsize=(7,5))
sns.histplot(icu_los_pd["los"], bins=20, kde=True, color="#55A868")
plt.xlabel("ICU length of stay (days)"); plt.title("ICU LOS Distribution")
plt.tight_layout(); plt.savefig("reports/figures/icu_los_dist.png", dpi=150)

# 4. Readmission rate by admission type
plt.figure(figsize=(7,5))
sns.barplot(data=readmit_by_type_pd, x="admission_type", y="readmit_rate", color="#C44E52")
plt.xticks(rotation=30); plt.title("30-Day Readmission Rate by Admission Type")
plt.tight_layout(); plt.savefig("reports/figures/readmit_by_type.png", dpi=150)

# 5. ROC curve (from Spark predictions, pulled to Pandas)
from sklearn.metrics import roc_curve, auc
preds_pd = predictions.select("readmit_30d", "probability").toPandas()
preds_pd["prob_pos"] = preds_pd["probability"].apply(lambda v: float(v[1]))
fpr, tpr, _ = roc_curve(preds_pd["readmit_30d"], preds_pd["prob_pos"])
plt.figure(figsize=(6,6))
plt.plot(fpr, tpr, label=f"AUC = {auc(fpr,tpr):.2f}")
plt.plot([0,1],[0,1],"--",color="gray")
plt.xlabel("False Positive Rate"); plt.ylabel("True Positive Rate")
plt.title("ROC Curve — Readmission Model"); plt.legend()
plt.tight_layout(); plt.savefig("reports/figures/roc_curve.png", dpi=150)

# 6. Confusion matrix heatmap
import numpy as np
cm = metrics.confusionMatrix().toArray()
plt.figure(figsize=(5,4))
sns.heatmap(cm, annot=True, fmt=".0f", cmap="Blues",
            xticklabels=["No Readmit","Readmit"], yticklabels=["No Readmit","Readmit"])
plt.xlabel("Predicted"); plt.ylabel("Actual"); plt.title("Confusion Matrix")
plt.tight_layout(); plt.savefig("reports/figures/confusion_matrix.png", dpi=150)

# 7. Feature importance
plt.figure(figsize=(7,6))
sns.barplot(data=importance_pd.sort_values("importance"), y="feature", x="importance", color="#8172B2")
plt.title("Feature Importance — Random Forest"); plt.tight_layout()
plt.savefig("reports/figures/feature_importance.png", dpi=150)
```

### Step 9.3 — Style consistency

Set one `sns.set_theme()` call and one color palette at the top of a shared `src/viz/style.py` and import it everywhere, so all 7+ figures look like they belong to the same report.

### ✅ End-of-W9 checkpoint

- [ ] All 7 figures generated and saved into `reports/figures/`.
- [ ] Each figure has a one-line caption drafted (goes straight into the report).

---

## 10. W9–W10 — Buffer, Testing & Report Writing

**Goal:** A pipeline that reproduces cleanly from a fresh cluster, and a complete written report.

### Step 10.1 — End-to-end reproducibility test

- [ ] Tear down and rebuild the cluster from scratch (`docker compose down -v && docker compose up -d`, or a fresh Dataproc cluster).
- [ ] Re-run ingestion → cleaning → labeling → Hive DDL/analytics → ICU analysis → feature engineering → model training → visualization **in order, from the committed scripts only** (no notebook state, no manual fixes).
- [ ] Time each stage — you'll want rough runtimes for the report/presentation ("ingestion + cleaning: ~4 min on a 2-node local cluster").
- [ ] Fix anything that breaks; this is exactly what "Buffer" time in the timeline is for — don't skip it, this is where most last-minute project failures get caught.

### Step 10.2 — Report outline (mirrors your existing proposal structure — extend it)

1. **Title / Abstract** — reuse and lightly extend your proposal abstract with actual results.
2. **Introduction & Objectives** — ICU analysis, disease statistics, hospital utilization, readmission prediction (as in the proposal).
3. **Related Work** — 3–5 sentences on prior MIMIC-based readmission studies (brief, not a full literature review).
4. **Dataset** — MIMIC demo version used, table list, size (patients/admissions/ICU stays), license, and the small-sample-size caveat stated up front.
5. **System Architecture** — HDFS/YARN/Hive/Spark/MLlib diagram (see Step 10.3 below), deployment choice (Docker vs GCP) and why.
6. **Methodology**
   - 6.1 Ingestion & Cleaning
   - 6.2 Readmission Label Definition (assumptions from Week 3)
   - 6.3 Hive Analytics approach
   - 6.4 ICU Analysis approach
   - 6.5 Feature Engineering
   - 6.6 Model Training & Tuning
7. **Results**
   - 7.1 Disease statistics findings
   - 7.2 Hospital utilization findings
   - 7.3 ICU patient analysis findings
   - 7.4 Readmission model performance (experiment log table + confusion matrix + ROC + feature importance)
8. **Discussion & Limitations** — small-sample caveats, overfitting risk, class imbalance, what would change at full MIMIC scale.
9. **Conclusion & Future Work** — e.g., scaling to full MIMIC-IV, adding clinical notes/NLP, streaming ingestion.
10. **References** — MIMIC citation (required by the license), any papers cited in Related Work.

### Step 10.3 — Architecture diagram

Sketch: `Raw CSVs → HDFS → Spark (clean/transform) → HDFS Parquet ⇄ Hive (analytics) ` and `→ Spark MLlib (features/model) → HDFS model artifact`, with `YARN` boxed around Spark/Hive as the resource manager, and `Python (Matplotlib/Seaborn)` pulling from both Hive query outputs and MLlib evaluation outputs into `reports/figures/`. Keep it to one clean box-and-arrow diagram — this is usually the single most-referenced figure in both the report and the presentation.

### Step 10.4 — Cleanup pass

- [ ] Remove dead code/notebooks that duplicate what's in `src/`.
- [ ] Make sure `README.md` alone is enough for someone else to reproduce your entire pipeline (setup → run → outputs).
- [ ] Spell-check and cross-check every number quoted in the report against `experiment_log.md` and `cleaning_report.md` — mismatches between report text and actual logged numbers are the most common thing graders catch.

### ✅ End-of-W10 (buffer half) checkpoint

- [ ] Fresh cluster → full pipeline → figures, run start-to-finish without manual intervention.
- [ ] Report drafted against the outline above, all figures embedded with captions.
- [ ] README finalized.

---

## 11. W10 — Final Presentation

**Goal:** A tight, demo-backed talk that doesn't oversell the small-data results.

### Step 11.1 — Slide outline (aim for ~10–12 slides for a typical 10–15 min slot)

1. Title + team
2. Problem statement & objectives (4 bullets: ICU, disease, utilization, readmission)
3. Dataset & its scale (be upfront about the demo-size caveat here, not buried)
4. Architecture diagram (from Step 10.3)
5. Pipeline walkthrough (ingestion → cleaning → label → analytics → ML, one slide)
6. Disease statistics results (top diagnoses figure)
7. Hospital utilization results (utilization figure)
8. ICU patient analysis results (LOS/outcomes figure)
9. Readmission model: approach + metrics (ROC + confusion matrix + feature importance, condensed)
10. Live or recorded demo (Step 11.2)
11. Limitations & future work
12. Q&A / thank you

### Step 11.2 — Demo script

Decide **live demo vs. recorded screen capture** based on how reliable your cluster startup is — a recorded 60–90 second clip of `docker compose up` → HDFS listing → a Hive query → a Spark job running on the YARN UI → a figure being generated is much safer than risking a live cluster hang in front of an audience. Have the live option ready as backup only if time allows.

### Step 11.3 — Anticipated questions to prep answers for

- "Why is your dataset so small — how would this scale to real MIMIC-IV (60k+ patients)?" (Have a concrete answer: same pipeline, more HDFS storage/YARN nodes, same Hive/Spark code with wider partitioning.)
- "Why 30 days for readmission, and how did you handle patients with no follow-up admission in the data?" (Point to Step 3.1's documented assumptions.)
- "Why Random Forest/GBT over deep learning?" (Small-n, tabular, interpretability via feature importance — a defensible, honest answer.)
- "What would you do differently with more time?" (Pull from Discussion & Limitations.)

---

## Appendix A — Suggested 2-Person Role Split

A workable split that keeps both members hands-on with the full stack while giving each a clear primary lane:

| Weeks   | Member A (suggested: Data Engineering lead) | Member B (suggested: ML/Analytics lead)              |
| ------- | ------------------------------------------- | ---------------------------------------------------- |
| W1–W2  | Cluster setup (Docker/GCP), HDFS loading    | PhysioNet data download/validation, repo scaffolding |
| W2–W3  | Spark cleaning jobs, data dictionary        | Data quality checks, label definition logic          |
| W4–W5  | Hive DDL + disease/utilization queries      | ICU Spark SQL analysis (parallel workstream)         |
| W6–W7  | Feature engineering pipeline                | Class-imbalance strategy, train/test split design    |
| W7–W8  | Model training infra, CV setup              | Model selection, hyperparameter grids                |
| W8–W9  | Evaluation metrics, feature importance      | Visualization suite                                  |
| W9–W10 | End-to-end reproducibility test             | Report writing (owns final draft)                    |
| W10     | Architecture/demo slides                    | Results/discussion slides                            |

Both members should review and be able to explain every stage, regardless of who wrote the code — split by ownership, not by silo.

---

## Appendix B — Troubleshooting Cheat Sheet

| Symptom                                                     | Likely cause                                                         | Fix                                                                                                                            |
| ----------------------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| DataNode container keeps restarting                         | NameNode not ready yet / clock skew                                  | Check`SERVICE_PRECONDITION` env vars, `docker logs`, ensure host clock is correct                                          |
| `pyspark --master yarn` hangs at `ACCEPTED`             | Not enough YARN memory configured for a laptop                       | Lower`yarn.nodemanager.resource.memory-mb` and `spark.executor.memory`                                                     |
| Hive metastore connection refused                           | Postgres-backed metastore container not up before HiveServer2 starts | Add/verify`SERVICE_PRECONDITION` on `hive-metastore-postgresql:5432`; restart `hive-metastore` after Postgres is healthy |
| `AnalysisException: Table or view not found` in Spark SQL | Spark session isn't Hive-enabled                                     | Use`SparkSession.builder.enableHiveSupport()` and confirm `hive-site.xml` is on the Spark classpath                        |
| Wrong/duplicate diagnosis titles after join                 | Joined`diagnoses_icd` to `d_icd_diagnoses` on `icd_code` only  | Join on**both** `icd_code` and `icd_version` (ICD-9 vs ICD-10 codes overlap)                                         |
| Model AUC looks suspiciously perfect (~1.0)                 | Data leakage — a feature encodes post-discharge or readmission info | Re-audit every feature against the "known at/before this discharge" rule in Step 6.1                                           |
| CrossValidator errors on tiny folds                         | numFolds too high for dataset size                                   | Drop to`numFolds=3`, or use a single stratified train/test split with no CV for the smallest models                          |
| `.toPandas()` step is slow/hangs                          | Pulling an unaggregated large table instead of an aggregate          | Always aggregate in Spark/Hive first; only bring small summary tables to Pandas                                                |

---

## Appendix C — Reference Links

- MIMIC-IV Clinical Database Demo: https://physionet.org/content/mimic-iv-demo/
- MIMIC-III Clinical Database Demo: https://physionet.org/content/mimiciii-demo/
- MIMIC Code Repository (official derivation/query examples): https://github.com/MIT-LCP/mimic-code
- Apache Spark MLlib guide: https://spark.apache.org/docs/latest/ml-guide.html
- Apache Hive Language Manual: https://cwiki.apache.org/confluence/display/hive/languagemanual
- Big Data Europe Docker Hadoop/Hive images: https://github.com/big-data-europe

---

*Keep this file in `docs/` at the repo root and check off boxes as you complete them — it doubles as your progress tracker for the Buffer week's "are we actually done" review.*
