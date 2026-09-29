import os
from pyspark.sql import functions as F
from common import get_spark, RAW, CLEAN, REPORTS, VITAL_ITEMIDS

spark = get_spark("01_ingest_clean")
rows = []          # cleaning report lines


def rd(rel):
    # Read everything as string, then cast explicitly (inferSchema would turn ICD codes like "0010" into ints)
    return spark.read.csv(f"{RAW}/{rel}", header=True)


def finish(name, raw_df, clean_df, note):
    n_raw, n_clean = raw_df.count(), clean_df.count()
    nulls = clean_df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in clean_df.columns]).first().asDict()
    nulls = {k: v for k, v in nulls.items() if v}
    rows.append(f"| {name} | {n_raw} | {n_clean} | {n_raw - n_clean} | {note} | {nulls or 'none'} |")
    clean_df.write.mode("overwrite").parquet(f"{CLEAN}/{name}")
    print(f"[{name}] raw={n_raw} clean={n_clean}")


# ---- admissions
raw = rd("hosp/admissions.csv.gz")
clean = (raw.select(
            F.col("subject_id").cast("int"), F.col("hadm_id").cast("int"),
            F.to_timestamp("admittime").alias("admittime"), F.to_timestamp("dischtime").alias("dischtime"),
            "admission_type", "insurance", "race", F.col("hospital_expire_flag").cast("int"))
         .filter(F.col("dischtime") > F.col("admittime"))          # drops missing/negative LOS
         .dropDuplicates(["hadm_id"])
         .withColumn("los_days", F.round((F.col("dischtime").cast("long") - F.col("admittime").cast("long")) / 86400.0, 2)))
finish("admissions", raw, clean, "dischtime>admittime, dedupe hadm_id, los_days derived")

# ---- patients (anchor_age is capped at 91 by MIMIC-IV de-identification)
raw = rd("hosp/patients.csv.gz")
clean = (raw.select(F.col("subject_id").cast("int"), "gender", F.col("anchor_age").cast("int"),
                    F.col("anchor_year").cast("int"), "anchor_year_group")
         .dropDuplicates(["subject_id"]))
finish("patients", raw, clean, "dedupe subject_id")

# ---- diagnoses
raw = rd("hosp/diagnoses_icd.csv.gz")
clean = (raw.select(F.col("subject_id").cast("int"), F.col("hadm_id").cast("int"), F.col("seq_num").cast("int"),
                    "icd_code", F.col("icd_version").cast("int"))
         .dropDuplicates(["hadm_id", "seq_num"]))
finish("diagnoses", raw, clean, "dedupe (hadm_id, seq_num)")

raw = rd("hosp/d_icd_diagnoses.csv.gz")
clean = (raw.select("icd_code", F.col("icd_version").cast("int"), "long_title")
         .dropDuplicates(["icd_code", "icd_version"]))
finish("d_icd_diagnoses", raw, clean, "dedupe (icd_code, icd_version)")

# ---- ICU stays
raw = rd("icu/icustays.csv.gz")
clean = (raw.select(F.col("subject_id").cast("int"), F.col("hadm_id").cast("int"), F.col("stay_id").cast("int"),
                    "first_careunit", "last_careunit",
                    F.to_timestamp("intime").alias("intime"), F.to_timestamp("outtime").alias("outtime"),
                    F.col("los").cast("double"))
         .filter(F.col("outtime") > F.col("intime"))
         .dropDuplicates(["stay_id"]))
finish("icustays", raw, clean, "outtime>intime, dedupe stay_id")

# ---- chartevents: biggest table -> keep only vitals we use + needed columns
vital_ids = [i for ids in VITAL_ITEMIDS.values() for i in ids]
raw = rd("icu/chartevents.csv.gz")
clean = (raw.select(F.col("stay_id").cast("int"), F.col("hadm_id").cast("int"),
                    F.to_timestamp("charttime").alias("charttime"),
                    F.col("itemid").cast("int"), F.col("valuenum").cast("double"))
         .filter(F.col("itemid").isin(vital_ids) & F.col("valuenum").isNotNull() & F.col("stay_id").isNotNull()))
finish("chartevents", raw, clean, f"only vital itemids, non-null valuenum")

os.makedirs(REPORTS, exist_ok=True)
with open(f"{REPORTS}/cleaning_report.md", "w") as f:
    f.write("# Cleaning report\n\n| table | raw rows | clean rows | dropped | rules | remaining null counts |\n|---|---|---|---|---|---|\n")
    f.write("\n".join(rows) + "\n")
print("wrote", f"{REPORTS}/cleaning_report.md")
