# Cleaning report

| table | raw rows | clean rows | dropped | rules | remaining null counts |
|---|---|---|---|---|---|
| admissions | 275 | 275 | 0 | dischtime>admittime, dedupe hadm_id, los_days derived | none |
| patients | 100 | 100 | 0 | dedupe subject_id | none |
| diagnoses | 4506 | 4506 | 0 | dedupe (hadm_id, seq_num) | none |
| d_icd_diagnoses | 109775 | 109775 | 0 | dedupe (icd_code, icd_version) | none |
| icustays | 140 | 140 | 0 | outtime>intime, dedupe stay_id | none |
| chartevents | 668862 | 55629 | 613233 | only vital itemids, non-null valuenum | none |
