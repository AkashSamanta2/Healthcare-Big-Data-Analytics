"""W7-W9: train LR / RandomForest / GBT, evaluate with imbalance-aware metrics, save RF model + outputs."""
import json, os
import pandas as pd
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.functions import vector_to_array
from common import get_spark, CLEAN, MODELS, LOCAL_CACHE, REPORTS

spark = get_spark("06_train_eval")
train = spark.read.parquet(f"{CLEAN}/features_train").cache()
test = spark.read.parquet(f"{CLEAN}/features_test").cache()
n_pos = train.filter("label = 1").count()
print("positives in train:", n_pos)

roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")


def metrics(model, data):
    p = model.transform(data)
    c = {(int(r["label"]), int(r["prediction"])): r["count"] for r in p.groupBy("label", "prediction").count().collect()}
    tp, fp, fn, tn = c.get((1, 1), 0), c.get((0, 1), 0), c.get((1, 0), 0), c.get((0, 0), 0)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    return dict(auc_roc=round(roc.evaluate(p), 3), auc_pr=round(pr.evaluate(p), 3),
                tp=tp, fp=fp, fn=fn, tn=tn, precision=round(prec, 3), recall=round(rec, 3), f1=round(f1, 3))


results, fitted = [], {}

lr = LogisticRegression(featuresCol="features", labelCol="label", weightCol="class_weight", maxIter=50, regParam=0.1)
fitted["LogisticRegression"] = lr.fit(train)

rf = RandomForestClassifier(featuresCol="features", labelCol="label", weightCol="class_weight", seed=42)
if n_pos >= 9:     # 3-fold CV needs a few positives per fold
    grid = ParamGridBuilder().addGrid(rf.numTrees, [50, 100]).addGrid(rf.maxDepth, [3, 5]).build()
    cv = CrossValidator(estimator=rf, estimatorParamMaps=grid, evaluator=pr, numFolds=3, seed=42)
    fitted["RandomForest"] = cv.fit(train).bestModel
else:
    print("Too few positives for CV -> RandomForest with fixed params")
    fitted["RandomForest"] = rf.setNumTrees(100).setMaxDepth(4).fit(train)

gbt = GBTClassifier(featuresCol="features", labelCol="label", weightCol="class_weight", maxIter=30, maxDepth=3, seed=42)
fitted["GBT"] = gbt.fit(train)

for name, m in fitted.items():
    row = {"model": name, **{f"test_{k}": v for k, v in metrics(m, test).items()}}
    row["train_auc_roc"] = metrics(m, train)["auc_roc"]
    results.append(row)

res = pd.DataFrame(results)
print(res.to_string(index=False))
os.makedirs(LOCAL_CACHE, exist_ok=True); os.makedirs(REPORTS, exist_ok=True)
res.to_csv(f"{LOCAL_CACHE}/model_results.csv", index=False)
with open(f"{REPORTS}/experiment_log.md", "w") as f:
    f.write("# Experiment log\n\n```\n" + res.to_string(index=False) + "\n```\n")

# ---- final model = random forest: predictions + importances + save
best = fitted["RandomForest"]
pred = best.transform(test).select("label", vector_to_array("probability")[1].alias("prob_pos"), "prediction")
pred.toPandas().to_csv(f"{LOCAL_CACHE}/rf_predictions.csv", index=False)
names = json.load(open(f"{LOCAL_CACHE}/feature_names.json"))
imp = pd.DataFrame({"feature": names, "importance": best.featureImportances.toArray()}).sort_values("importance", ascending=False)
imp.to_csv(f"{LOCAL_CACHE}/feature_importance.csv", index=False)
best.write().overwrite().save(f"{MODELS}/readmission_rf")
print(imp.head(10).to_string(index=False))
