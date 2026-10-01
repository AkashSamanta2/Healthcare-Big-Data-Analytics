SELECT first_careunit AS first_careunit, COUNT(*) AS num_stays, ROUND(AVG(los), 2) AS avg_icu_los_days
FROM mimic.icustays_clean
GROUP BY first_careunit
ORDER BY num_stays DESC;
