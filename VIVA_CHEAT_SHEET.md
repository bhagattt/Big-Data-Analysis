# BIG DATA MINI-PROJECT: VIVA CHEAT SHEET

**Project Title:** Comparative Study: Solar vs Wind Output  
**Format:** Concise 1–3 Sentence Answers for High Scores in Viva Voce

---

### 1. What is HDFS?
**HDFS (Hadoop Distributed File System)** is a distributed file system designed to store massive datasets across clusters of commodity hardware. It achieves high fault tolerance by breaking files into large data blocks (typically 128 MB) and automatically replicating them across multiple DataNodes managed by a master NameNode.

---

### 2. What is Spark?
**Apache Spark** is an open-source, distributed general-purpose cluster computing engine that provides in-memory data processing. It is up to 100 times faster than traditional Hadoop MapReduce because it avoids writing intermediate stage outputs to physical disk.

---

### 3. Why PySpark?
**PySpark** is the Python API for Apache Spark. It combines the simplicity and rapid prototyping of Python with the distributed computing power of the Spark Java Virtual Machine (JVM) engine, allowing data engineers to write scalable Big Data code without writing complex Scala or Java.

---

### 4. What is an RDD?
An **RDD (Resilient Distributed Dataset)** is the fundamental low-level data abstraction in Apache Spark. It is an immutable, partitioned collection of records that can be processed in parallel across cluster nodes with automatic lineage-based fault recovery.

---

### 5. What is MapReduce?
**MapReduce** is a software framework and programming model for processing large datasets in parallel across distributed clusters. It divides computation into two phases: a **Map phase** that filters and projects data into `(key, value)` pairs, and a **Reduce phase** that aggregates values for each distinct key.

---

### 6. Map vs. Reduce?
- **Map Phase:** Takes raw input records, extracts relevant fields, and outputs intermediate key-value pairs `(key, 1)`. Each record is processed independently and concurrently.
- **Reduce Phase:** Receives all values grouped by key `(key, [v1, v2, ...])` after the network shuffle and merges/sums them into a final consolidated result `(key, total)`.

---

### 7. Why MongoDB?
**MongoDB** is a document-oriented NoSQL database that stores data in flexible, JSON-like BSON documents. We use it because it requires no rigid upfront schema, handles semi-structured time-series data seamlessly, and provides fast querying and aggregation with simple Python integration (`pymongo`).

---

### 8. What is NoSQL?
**NoSQL ("Not Only SQL")** refers to non-relational database management systems designed for horizontal scalability, flexible schema models, and high-velocity read/write operations. Unlike relational SQL databases with fixed row-and-column tables, NoSQL databases support document, key-value, column-family, and graph data models.

---

### 9. Why use HDFS in this project?
We use **HDFS** as the central, fault-tolerant raw data lake to store original solar and wind CSV files before processing. It decouples long-term raw storage from computation, ensuring raw logs remain intact even if downstream database schemas change.

---

### 10. What is EDA?
**EDA (Exploratory Data Analysis)** is the critical initial phase in data analysis where datasets are investigated to summarize their main statistical characteristics, check schemas and data types, detect missing or corrupt values, and understand distributions before pushing data into heavy distributed pipelines.

---

### 11. What is the difference between solar and wind generation data?
**Solar generation** is strictly diurnal (daylight-dependent), dropping to 0 MW during all nighttime hours and peaking sharply around midday. **Wind generation** operates continuously across all 24 hours without dropping to zero, often surging during evening, nighttime, and stormy weather when solar output is zero.

---

### 12. Why compare solar and wind?
Comparing solar and wind proves their **complementary nature**: solar provides maximum power during midday peak industrial demand, while wind provides base load during evenings and nights. Analyzing both together shows grid operators how to balance renewable penetration and reduce the need for fossil-fuel backup generators.

---

### 13. What is the purpose of the ingestion pipeline?
The **ingestion pipeline** automates the end-to-end movement of data from raw CSV files into HDFS, performs distributed cleaning and capacity factor calculation using PySpark, and stages validated structured records into MongoDB for fast downstream querying and visualization.
