I am doing a Big Data mini-project titled:

**“Comparative Study: Solar vs Wind Output”**

I need to complete ONLY Experiments 1 to 4 from my Big Data laboratory syllabus. Keep the entire project **VERY SIMPLE, beginner-friendly, and easy to explain in a viva**. Do NOT introduce unnecessary machine learning, complex Spark transformations, advanced statistics, cloud deployment, complicated architectures, or difficult algorithms.

### My available datasets

I have found these possible sources:

1. Open Power System Data – Time Series:
   https://data.open-power-system-data.org/time_series/

2. Kaggle – Wind Power Generation Data Forecasting:
   https://www.kaggle.com/datasets/mubashirrahim/wind-power-generation-data-forecasting

3. Kaggle – Solar Power Generation Data:
   https://www.kaggle.com/datasets/anikannal/solar-power-generation-data

4. OpenEI / ARPA-E ERCOT 2017 Solar:
   https://data.openei.org/s3_viewer?bucket=arpa-e-perform&prefix=ERCOT%2F2017%2FSolar%2FActuals%2FZone_level%2F&limit=10

5. OpenEI / ARPA-E ERCOT 2017 Wind:
   https://data.openei.org/s3_viewer?bucket=arpa-e-perform&prefix=ERCOT%2F2017%2FWind%2FActuals%2FZone_level%2F&limit=10

6. OpenEI / ARPA-E ERCOT 2018 Solar:
   https://data.openei.org/s3_viewer?bucket=arpa-e-perform&prefix=ERCOT%2F2018%2FSolar%2FActuals%2FZone_level%2F&limit=10

7. OpenEI / ARPA-E ERCOT 2018 Wind:
   https://data.openei.org/s3_viewer?bucket=arpa-e-perform&prefix=ERCOT%2F2018%2FWind%2FActuals%2FZone_level%2F&limit=10

Choose the **easiest practical combination of one solar dataset and one wind dataset**. Prefer datasets that are simple CSV files and easy to process with Pandas/PySpark. If using two datasets from the same source makes comparison easier, prefer that.

---

# EXPERIMENT 1 — Domain Selection, Setup & EDA

### Objective

Set up the environment and understand the solar and wind datasets.

Show me how to:

1. Verify Python.
2. Verify PySpark.
3. Verify Hadoop/HDFS.
4. Create simple HDFS directories.
5. Download/copy the selected solar and wind CSV files.
6. Load them using Pandas and/or PySpark.
7. Perform very basic EDA:

   * number of records
   * number of columns
   * column names
   * data types
   * missing values
   * first few rows
   * basic descriptive statistics
8. Compare the basic structure of the solar and wind datasets.

Keep the commands extremely simple.

Give me:

* exact commands to run
* simple Python/PySpark code
* expected type of output
* what each step means in one or two lines
* a very short EDA conclusion

Also create a simple **Environment Verification Script** that checks:

* Python
* PySpark
* Hadoop/HDFS

Do not make the EDA complicated.

---

# EXPERIMENT 2 — Project Proposal & Architecture

Create a very simple project proposal for:

**Comparative Study: Solar vs Wind Output**

Include:

1. Title
2. Problem statement
3. Objective
4. Why compare solar and wind?
5. Dataset description
6. Tools used
7. Team roles — keep them simple, for example:

   * Data Engineer
   * Data Analyst
   * Data Scientist
8. Expected outcome
9. Limitations
10. Simple future scope

Create a very basic architecture:

**Solar/Wind CSV Data**
↓
**HDFS**
↓
**PySpark Processing**
↓
**Processed Data**
↓
**MongoDB**
↓
**Basic Analysis / Comparison**

I want a simple architecture that I can easily draw in Draw.io and explain in a viva.

Also give me the exact text that should go inside each architecture box.

Do NOT include Kafka, Airflow, Kubernetes, cloud services, APIs, deep learning, or other unnecessary components.

---

# EXPERIMENT 3 — MapReduce Processing on Unstructured Text Data

My main project is solar vs wind output, but the syllabus specifically requires a MapReduce experiment on text data.

Therefore, create a **small and independent text-processing experiment** that does NOT complicate my main project.

Use a small text dataset or a simple text file related to renewable energy.

Perform:

1. Word count
2. Keyword filtering for words such as:

   * solar
   * wind
   * energy
   * power
   * renewable

Use either:

* Hadoop MapReduce, OR
* PySpark RDDs

Prefer **PySpark RDDs if that is easier**.

Show:

* input text
* mapper/reducer concept
* very simple code
* execution command
* sample output
* short explanation of how distributed processing works

I need to be able to explain:

“What happens in the Map phase?”
“What happens in the Reduce phase?”
“Why is this considered distributed processing?”

Keep this experiment completely beginner-friendly.

Do NOT build sentiment analysis unless absolutely necessary.

---

# EXPERIMENT 4 — Data Ingestion Pipeline & NoSQL Staging

Now return to my main project:

**Comparative Study: Solar vs Wind Output**

Create a very simple ingestion pipeline:

**CSV**
→ **HDFS**
→ **PySpark**
→ **MongoDB**

Use MongoDB rather than Cassandra because I want the easier option.

Show me how to:

1. Create HDFS folders such as:

/renewable_energy/
/renewable_energy/solar/
/renewable_energy/wind/
/renewable_energy/processed/

2. Upload solar and wind CSV files to HDFS.

3. Read them using PySpark.

4. Perform only basic cleaning:

   * remove obvious null rows
   * standardize column names if necessary
   * convert output/power column to numeric if necessary

5. Save the cleaned data.

6. Insert a small/sample amount of processed solar and wind data into MongoDB.

7. Show basic MongoDB commands:

   * insert
   * find
   * count
   * simple filter/query

8. Show basic HDFS validation commands:

   * hdfs dfs -ls
   * hdfs dfs -cat
   * hdfs dfs -du

9. Verify that the records successfully moved through the pipeline.

Give me:

* folder structure
* Python/PySpark ingestion script
* MongoDB commands
* HDFS commands
* sample outputs
* short explanation for each step
* final validation checklist

Do NOT build a production-grade pipeline.

---

# IMPORTANT REQUIREMENTS

This is a **college laboratory mini-project**, not a production system.

Therefore:

* Keep everything extremely easy.
* Prefer short code.
* Avoid unnecessary libraries.
* Avoid complicated Spark SQL.
* Avoid ML.
* Avoid forecasting.
* Avoid advanced statistics.
* Avoid cloud deployment.
* Avoid Kafka/Airflow.
* Avoid complicated NoSQL schemas.
* Avoid complicated MapReduce.
* Use CSV wherever possible.
* Use PySpark because it is part of the syllabus.
* Use MongoDB because it is easier than Cassandra.
* Use small samples where possible so execution is fast.
* Make everything runnable on a normal laptop/local Hadoop setup.

For every experiment, provide:

**Aim → Objective → Dataset → Tools → Steps → Code → Expected Output → Result → Viva Explanation**

At the end, give me a **very short viva cheat sheet** containing the most likely questions and easy 1–3 sentence answers, especially:

* What is HDFS?
* What is Spark?
* Why PySpark?
* What is RDD?
* What is MapReduce?
* Map vs Reduce?
* Why MongoDB?
* What is NoSQL?
* Why use HDFS?
* What is EDA?
* What is the difference between solar and wind generation data?
* Why compare solar and wind?
* What is the purpose of the ingestion pipeline?

Most importantly, **do not make this project look more complicated than it needs to be.**
