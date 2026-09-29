"""W9: run on YOUR PC (not in Docker):  python src/07_visualize.py
Reads data/local_cache/*.tsv|csv and writes reports/figures/*.png"""
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import roc_curve, auc, confusion_matrix

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
C, F = f"{ROOT}/data/local_cache", f"{ROOT}/reports/figures"
os.makedirs(F, exist_ok=True)
sns.set_theme(style="whitegrid")


def load(name):
    for ext, sep in (("tsv", "\t"), ("csv", ",")):
        p = f"{C}/{name}.{ext}"
        if os.path.exists(p):
            return pd.read_csv(p, sep=sep)
    print("skip (file missing):", name)
    return None


def save(name):
    plt.tight_layout(); plt.savefig(f"{F}/{name}.png", dpi=150); plt.close(); print("wrote", name)


d = load("top_diagnoses")
if d is not None:
    plt.figure(figsize=(9, 7)); sns.barplot(data=d.head(15), y="diagnosis", x="admission_count", color="#4C72B0")
    plt.title("Top 15 diagnoses (by admissions)"); save("top_diagnoses")

d = load("admissions_by_month")
if d is not None:
    plt.figure(figsize=(8, 4)); sns.barplot(data=d, x="admit_month", y="num_admissions", color="#4C72B0")
    plt.title("Admissions by month of year"); save("admissions_by_month")

d = load("utilization_by_admission_type")
if d is not None:
    plt.figure(figsize=(8, 5)); sns.barplot(data=d, x="admission_type", y="readmit_rate", color="#C44E52")
    plt.xticks(rotation=30, ha="right"); plt.title("30-day readmission rate by admission type"); save("readmit_by_type")

d = load("icu_by_careunit")
if d is not None:
    plt.figure(figsize=(8, 5)); sns.barplot(data=d, y="first_careunit", x="avg_icu_los_days", color="#55A868")
    plt.title("Average ICU LOS by care unit"); save("icu_los_by_unit")

d = load("icu_los")
if d is not None:
    plt.figure(figsize=(7, 5)); sns.histplot(d["los"], bins=20, kde=True, color="#55A868")
    plt.xlabel("ICU length of stay (days)"); plt.title("ICU LOS distribution"); save("icu_los_dist")

d = load("icu_by_age_band")
if d is not None:
    plt.figure(figsize=(6, 4)); sns.barplot(data=d, x="age_band", y="avg_icu_los_days", color="#8172B2")
    plt.title("Average ICU LOS by age band"); save("icu_los_by_age")

p = load("rf_predictions")
if p is not None and p["label"].nunique() == 2:
    fpr, tpr, _ = roc_curve(p["label"], p["prob_pos"])
    plt.figure(figsize=(5.5, 5.5)); plt.plot(fpr, tpr, label=f"AUC = {auc(fpr, tpr):.2f}")
    plt.plot([0, 1], [0, 1], "--", color="gray"); plt.xlabel("False positive rate"); plt.ylabel("True positive rate")
    plt.title("ROC - readmission model (test set)"); plt.legend(); save("roc_curve")
if p is not None:
    cm = confusion_matrix(p["label"], p["prediction"], labels=[0, 1])
    plt.figure(figsize=(4.5, 4)); sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", cbar=False,
        xticklabels=["No readmit", "Readmit"], yticklabels=["No readmit", "Readmit"])
    plt.xlabel("Predicted"); plt.ylabel("Actual"); plt.title("Confusion matrix (test set)"); save("confusion_matrix")

d = load("feature_importance")
if d is not None:
    plt.figure(figsize=(8, 6)); sns.barplot(data=d.head(15), y="feature", x="importance", color="#8172B2")
    plt.title("Random forest feature importance"); save("feature_importance")
