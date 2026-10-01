SELECT d.long_title AS diagnosis, COUNT(DISTINCT dx.hadm_id) AS admission_count
FROM mimic.diagnoses_icd dx
JOIN mimic.d_icd_diagnoses d ON dx.icd_code = d.icd_code AND dx.icd_version = d.icd_version
GROUP BY d.long_title
ORDER BY admission_count DESC
LIMIT 20;
