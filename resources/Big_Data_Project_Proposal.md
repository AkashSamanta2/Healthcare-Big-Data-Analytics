# **CS404/CS757-Big Data Analytics Project** **<u>Abstract</u>** 

## **Project Title: Healthcare Analysis using Big Data Analytics** 

## **Group Members:** 

|S. No.|Name|Roll No.|Mobile No.|
|---|---|---|---|
|1.|Akash Samanta|2025MPCS-001|+918515072774|
|2.|Harsh Wadhwani|2025MPCS-003|+919131363200|



## **Objectives:** 

- ICU Patient Analysis 

- Disease Statistics 

- Hospital Utilization 

- Readmission Prediction 

## **Abstract:** 

This project aims to analyze large-scale clinical and healthcare datasets, particularly MIMIC-III/IV Demo dataset, a publicly available, subset of the MIMIC dataset provided by the MIT Laboratory for Computational Physiology through PhysioNet. The demo dataset has the same schema of the original dataset. The input data consists of deidentified clinical, ICU, patient admission, diagnosis, and physiological records for a representative sample of patients. 

The proposed approach uses the Hadoop and Spark ecosystem for distributed storage, processing, analysis, and predictive modelling. Healthcare data will be stored in HDFS and processed using Apache Hive and Apache Spark. Spark will be used for data cleaning, transformation, aggregation, and large-scale analytics, while Spark MLlib will be used for developing a machine-learning model for readmission prediction. The analysis will focus on four key areas: ICU patient analysis **,** disease statistics, hospital utilization, and readmission prediction. ICU patient analysis will examine patient characteristics and clinical patterns, disease statistics will identify the prevalence and distribution of major diseases; hospital utilization analysis will evaluate admissions, ICU stays, and resource usage; and machine-learning techniques will be applied to identify factors associated with hospital readmission and predict readmission risk. 

The expected output is a scalable healthcare analytics framework built entirely on the open MIMIC Demo dataset – providing statistical insights, utilization trends, and predictive results to support data-driven clinical research and healthcare decisionmaking. 

## **Big Data Technologies to be used:** 

- **Hadoop HDFS –** Distributed storage for the MIMIC Demo healthcare datasets. 

- **Apache Hive –** SQL-based querying and aggregation for healthcare data statistics & analysis. 

- **Apache Spark & Spark SQL–** Primary engine for data cleaning, transformation, join and large-scale analytics across all areas. 

- **Spark MLlib –** Machine learning model for readmission prediction. 

- **YARN –** Resource management and job scheduling within the cluster. 

- **Python (Matploltlib / Seaborn) –** Visualization of results. 

- **Deployment –** 

   - A Dockerized Hadoop + Spark + Hive stack, **OR** 

   - Google Cloud Platform 

## **Timelines (In weeks):** 





|**Activity**|**W1**|**W2**|**W3**|**W4**|**W5**|**W6**|**W7**|**W8**|**W9**|**W10**|
|---|---|---|---|---|---|---|---|---|---|---|
|PhysioNet Access + Env Setup|||||||||||
|Ingestion & Data Cleaning (Spark)|||||||||||
|Readmission Label Definition|||||||||||
|Hive Analytics: Disease & Utilization Stats|||||||||||
|ICU Patient Analysis (Spark SQL)|||||||||||
|Feature Engineering (MLlib)|||||||||||
|Readmission Prediction Model Training|||||||||||
|Model Evaluation & Tuning|||||||||||
|Visualization (Matplotlib/Seaborn)|||||||||||
|Buffer, Testing & Report Writing|||||||||||
|Final Presentation|||||||||||



