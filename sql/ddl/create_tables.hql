CREATE DATABASE IF NOT EXISTS mimic;

CREATE EXTERNAL TABLE IF NOT EXISTS mimic.admissions_labeled (
  subject_id INT, hadm_id INT, admittime TIMESTAMP, dischtime TIMESTAMP,
  admission_type STRING, insurance STRING, race STRING, hospital_expire_flag INT,
  los_days DOUBLE, days_to_next_admit INT, readmit_30d INT
) STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/mimic/cleaned/admissions_labeled';

CREATE EXTERNAL TABLE IF NOT EXISTS mimic.patients_clean (
  subject_id INT, gender STRING, anchor_age INT, anchor_year INT, anchor_year_group STRING
) STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/mimic/cleaned/patients';

CREATE EXTERNAL TABLE IF NOT EXISTS mimic.diagnoses_icd (
  subject_id INT, hadm_id INT, seq_num INT, icd_code STRING, icd_version INT
) STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/mimic/cleaned/diagnoses';

CREATE EXTERNAL TABLE IF NOT EXISTS mimic.d_icd_diagnoses (
  icd_code STRING, icd_version INT, long_title STRING
) STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/mimic/cleaned/d_icd_diagnoses';

CREATE EXTERNAL TABLE IF NOT EXISTS mimic.icustays_clean (
  subject_id INT, hadm_id INT, stay_id INT, first_careunit STRING, last_careunit STRING,
  intime TIMESTAMP, outtime TIMESTAMP, los DOUBLE
) STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/mimic/cleaned/icustays';

SHOW TABLES IN mimic;
SELECT COUNT(*) AS n_admissions FROM mimic.admissions_labeled;
