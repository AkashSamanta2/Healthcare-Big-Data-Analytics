"""Shared paths/helpers used by every Spark script (runs inside the spark-master container)."""
import os
from pyspark.sql import SparkSession

BASE = "hdfs://namenode:8020/data/mimic"
RAW = f"{BASE}/raw"
CLEAN = f"{BASE}/cleaned"
MODELS = f"{BASE}/models"

LOCAL_CACHE = "/workspace/data/local_cache"   # = <project>/data/local_cache on your PC
REPORTS = "/workspace/reports"

VITAL_ITEMIDS = {
    "heart_rate": [220045],
    "resp_rate": [220210],
    "spo2": [220277],
    "sbp": [220179, 220050],
    "temp_c": [223762],
}
VITAL_RANGES = {
    "heart_rate": (20, 250), "resp_rate": (3, 70), "spo2": (50, 100),
    "sbp": (40, 300), "temp_c": (30, 43),
}


def get_spark(app_name):
    spark = (SparkSession.builder
             .appName(app_name)
             .master("spark://spark-master:7077")   # this compose has no spark-defaults.conf, so set it here
             .config("spark.executor.memory", "1g")
             .config("spark.driver.memory", "1g")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def save_local_csv(df, name):
    os.makedirs(LOCAL_CACHE, exist_ok=True)
    path = f"{LOCAL_CACHE}/{name}.csv"
    df.toPandas().to_csv(path, index=False)
    print("saved", path)
