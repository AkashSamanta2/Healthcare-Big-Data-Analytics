SELECT p.gender AS gender, d.long_title AS diagnosis, COUNT(*) AS n
FROM mimic.diagnoses_icd dx
JOIN mimic.d_icd_diagnoses d ON dx.icd_code = d.icd_code AND dx.icd_version = d.icd_version
JOIN mimic.patients_clean p ON dx.subject_id = p.subject_id
GROUP BY p.gender, d.long_title
ORDER BY n DESC
LIMIT 30;
