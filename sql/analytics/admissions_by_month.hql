-- MIMIC dates are shifted per patient (years change, season is preserved),
-- so month-of-year is meaningful but the calendar year is not.
SELECT MONTH(admittime) AS admit_month, COUNT(*) AS num_admissions
FROM mimic.admissions_labeled
GROUP BY MONTH(admittime)
ORDER BY admit_month;
