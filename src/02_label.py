"""W3: 30-day unplanned readmission label.
Rules: next admission of the same patient within 30 days of discharge, next admission not ELECTIVE,
index admission survived. Patients who died are excluded later from the ML cohort."""
from pyspark.sql import Window, functions as F
from common import get_spark, CLEAN, save_local_csv

spark = get_spark("02_label")
adm = spark.read.parquet(f"{CLEAN}/admissions")

w = Window.partitionBy("subject_id").orderBy("admittime")
labeled = (adm
    .withColumn("next_admittime", F.lead("admittime").over(w))
    .withColumn("next_admission_type", F.lead("admission_type").over(w))
    .withColumn("days_to_next_admit", F.datediff("next_admittime", "dischtime"))
    .withColumn("readmit_30d", F.when(
        (F.col("hospital_expire_flag") == 0)
        & F.col("days_to_next_admit").isNotNull()
        & (F.col("days_to_next_admit") >= 0) & (F.col("days_to_next_admit") <= 30)
        & (F.col("next_admission_type") != "ELECTIVE"), 1).otherwise(0)))

out = labeled.select("subject_id", "hadm_id", "admittime", "dischtime", "admission_type", "insurance", "race",
                     "hospital_expire_flag", "los_days", "days_to_next_admit", "readmit_30d")
out.write.mode("overwrite").parquet(f"{CLEAN}/admissions_labeled")

print("=== Label balance (survivors only) ===")
bal = out.filter("hospital_expire_flag = 0").groupBy("readmit_30d").count().orderBy("readmit_30d")
bal.show()
print("negative days_to_next_admit (should be 0):", out.filter("days_to_next_admit < 0").count())
save_local_csv(bal, "label_balance")
