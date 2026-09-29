"""W5-W6: ICU analysis with Spark SQL / DataFrames. Also builds per-admission ICU features for ML."""
from pyspark.sql import functions as F
from common import get_spark, CLEAN, VITAL_ITEMIDS, VITAL_RANGES, save_local_csv

spark = get_spark("04_icu_analysis")
icu = spark.read.parquet(f"{CLEAN}/icustays")
chart = spark.read.parquet(f"{CLEAN}/chartevents").select("stay_id", "charttime", "itemid", "valuenum")
adm = spark.read.parquet(f"{CLEAN}/admissions_labeled").drop("subject_id")   # icu already has subject_id
pat = spark.read.parquet(f"{CLEAN}/patients")

# first 24 hours of each ICU stay
first_day = (chart.join(icu.select("stay_id", "intime"), "stay_id")
             .filter((F.col("charttime") >= F.col("intime")) &
                     (F.col("charttime") <= F.col("intime") + F.expr("INTERVAL 1 DAY"))))

vitals = None
for name, ids in VITAL_ITEMIDS.items():
    lo, hi = VITAL_RANGES[name]
    v = (first_day.filter(F.col("itemid").isin(ids) & F.col("valuenum").between(lo, hi))
         .groupBy("stay_id")
         .agg(F.avg("valuenum").alias(f"avg_{name}_24h"),
              F.min("valuenum").alias(f"min_{name}_24h"),
              F.max("valuenum").alias(f"max_{name}_24h")))
    vitals = v if vitals is None else vitals.join(v, "stay_id", "outer")

icu_full = icu.join(vitals, "stay_id", "left").join(adm, "hadm_id").join(pat, "subject_id")
icu_full.write.mode("overwrite").parquet(f"{CLEAN}/icu_analysis")
icu_full.createOrReplaceTempView("icu_full")

# --- Spark SQL analyses (exported for plotting)
save_local_csv(spark.sql("""
    SELECT first_careunit, COUNT(*) AS n_stays, ROUND(AVG(los),2) AS avg_icu_los_days,
           ROUND(AVG(hospital_expire_flag),3) AS mortality_rate
    FROM icu_full GROUP BY first_careunit ORDER BY n_stays DESC"""), "icu_by_unit")

save_local_csv(spark.sql("""
    SELECT admission_type, COUNT(*) AS n_icu_stays, ROUND(AVG(los),2) AS avg_icu_los_days,
           ROUND(AVG(hospital_expire_flag),3) AS mortality_rate
    FROM icu_full GROUP BY admission_type ORDER BY n_icu_stays DESC"""), "icu_by_admission_type")

save_local_csv(spark.sql("""
    SELECT CASE WHEN anchor_age < 40 THEN '<40' WHEN anchor_age < 65 THEN '40-64' ELSE '65+' END AS age_band,
           COUNT(*) AS n_stays, ROUND(AVG(los),2) AS avg_icu_los_days
    FROM icu_full GROUP BY 1 ORDER BY 1"""), "icu_by_age_band")

save_local_csv(icu.select("stay_id", "los"), "icu_los")

# --- per-admission ICU features for the ML step
icu_by_hadm = icu_full.groupBy("hadm_id").agg(
    F.max(F.lit(1)).alias("had_icu_stay"),
    F.sum("los").alias("icu_los_days"),
    F.avg("avg_heart_rate_24h").alias("avg_heart_rate_24h"),
    F.avg("avg_resp_rate_24h").alias("avg_resp_rate_24h"),
    F.avg("avg_spo2_24h").alias("avg_spo2_24h"))
icu_by_hadm.write.mode("overwrite").parquet(f"{CLEAN}/icu_by_hadm")
print("ICU stays:", icu_full.count(), " admissions with ICU:", icu_by_hadm.count())
