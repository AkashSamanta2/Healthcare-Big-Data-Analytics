-- Readmission rate is computed on survivors only (deaths cannot be readmitted)
SELECT admission_type AS admission_type,
       COUNT(*) AS num_admissions,
       ROUND(AVG(los_days), 2) AS avg_los_days,
       ROUND(SUM(readmit_30d) / COUNT(*), 3) AS readmit_rate
FROM mimic.admissions_labeled
WHERE hospital_expire_flag = 0
GROUP BY admission_type
ORDER BY num_admissions DESC;
