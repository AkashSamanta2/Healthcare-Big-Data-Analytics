"""W6-W7: feature engineering (MLlib). Patient-level stratified split, pipeline fitted on TRAIN only."""
import json, os
from pyspark.sql import Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, Imputer
from common import get_spark, CLEAN, MODELS, LOCAL_CACHE

spark = get_spark("05_features")
adm = spark.read.parquet(f"{CLEAN}/admissions_labeled")
pat = spark.read.parquet(f"{CLEAN}/patients")
dx = spark.read.parquet(f"{CLEAN}/diagnoses")
icu_h = spark.read.parquet(f"{CLEAN}/icu_by_hadm")

# prior admissions must be computed BEFORE we filter the cohort
w_prior = Window.partitionBy("subject_id").orderBy("admittime").rowsBetween(Window.unboundedPreceding, -1)
adm = adm.withColumn("num_prior_admissions", F.count("hadm_id").over(w_prior))
diag = dx.groupBy("hadm_id").agg(F.count("*").alias("num_diagnoses"))

CAT = ["gender", "admission_type", "insurance"]   # race left out: 200 rows + many rare categories, and it needs a fairness discussion first
NUM = ["age", "los_days", "num_prior_admissions", "num_diagnoses", "had_icu_stay", "icu_los_days"]
VIT = ["avg_heart_rate_24h", "avg_resp_rate_24h", "avg_spo2_24h"]

df = (adm.filter("hospital_expire_flag = 0")                      # cohort: survivors only
      .join(pat.select("subject_id", "gender", "anchor_age"), "subject_id")
      .join(diag, "hadm_id", "left").join(icu_h, "hadm_id", "left")
      .withColumn("age", F.col("anchor_age").cast("double"))
      .fillna({"num_diagnoses": 0, "had_icu_stay": 0, "icu_los_days": 0.0,
               "gender": "UNK", "admission_type": "UNK", "insurance": "UNK"})
      .withColumn("label", F.col("readmit_30d").cast("double")))
for c in NUM + VIT:
    df = df.withColumn(c, F.col(c).cast("double"))

# ---- patient-level stratified 80/20 split (same patient never in both sets)
subj = df.groupBy("subject_id").agg(F.max("label").alias("ever_pos"))
train_subj = subj.sampleBy("ever_pos", {0.0: 0.8, 1.0: 0.8}, seed=42).select("subject_id")
train_df = df.join(train_subj, "subject_id", "inner")
test_df = df.join(train_subj, "subject_id", "left_anti")

pos = train_df.filter("label = 1").count(); neg = train_df.filter("label = 0").count()
print(f"train rows={pos+neg} (pos={pos}, neg={neg}) | test rows={test_df.count()} (pos={test_df.filter('label = 1').count()})")
tot = pos + neg
weight = lambda d: d.withColumn("class_weight", F.when(F.col("label") == 1, neg / tot).otherwise(pos / tot))

stages = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CAT]
stages += [OneHotEncoder(inputCols=[f"{c}_idx" for c in CAT], outputCols=[f"{c}_vec" for c in CAT])]
stages += [Imputer(inputCols=VIT, outputCols=[f"{c}_imp" for c in VIT], strategy="median")]
stages += [VectorAssembler(inputCols=[f"{c}_vec" for c in CAT] + NUM + [f"{c}_imp" for c in VIT],
                           outputCol="features_raw", handleInvalid="error")]
stages += [StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)]
model = Pipeline(stages=stages).fit(train_df)                      # fit on TRAIN only -> no leakage

keep = ["subject_id", "hadm_id", "label", "features_raw", "features"]
train_f = weight(model.transform(train_df).select(*keep))
test_f = model.transform(test_df).select(*keep)
train_f.write.mode("overwrite").parquet(f"{CLEAN}/features_train")
test_f.write.mode("overwrite").parquet(f"{CLEAN}/features_test")
model.write().overwrite().save(f"{MODELS}/feature_pipeline")

# ---- feature names (for importance plots)
attrs = train_f.schema["features_raw"].metadata["ml_attr"]["attrs"]
names = {a["idx"]: a["name"] for grp in attrs.values() for a in grp}
size = train_f.select("features").first()[0].size
os.makedirs(LOCAL_CACHE, exist_ok=True)
json.dump([names.get(i, f"f{i}") for i in range(size)], open(f"{LOCAL_CACHE}/feature_names.json", "w"))
print("feature vector size:", size)
