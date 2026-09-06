# MASTER DELIVERABLES & SCREENSHOTS GUIDE

**Project Title:** Comparative Study: Solar vs Wind Output  
**Syllabus Target:** Big Data Laboratory (Experiments 1 to 4)

---

## 1. Master Deliverables Table

| Exp # | Experiment Name | Code Script to Run | Artifacts / Datasets Generated | Lab Manual Documentation | Screenshots Generated (Ready to Paste) |
| :---: | :--- | :--- | :--- | :--- | :--- |
| **1** | **Domain Selection, Setup & EDA** | `python verify_environment.py`<br>`python exp1_eda.py` | `data/solar_generation.csv` (720 rows)<br>`data/wind_generation.csv` (720 rows) | [EXPERIMENT_1_MANUAL.md](file:///c:/Coding/data%20cleaning/EXPERIMENT_1_MANUAL.md) | 1. `screenshots/exp1_01_environment_check.png`<br>2. `screenshots/exp1_02_eda_comparison.png` |
| **2** | **Project Proposal & Architecture** | `python exp2_generate_architecture.py` | `architecture_diagram.png`<br>`architecture.drawio` | [EXPERIMENT_2_MANUAL.md](file:///c:/Coding/data%20cleaning/EXPERIMENT_2_MANUAL.md) | 1. `screenshots/exp2_01_architecture_diagram.png`<br>2. `screenshots/exp2_02_generator_terminal.png` |
| **3** | **MapReduce Processing on Unstructured Text** | `python exp3_mapreduce.py` | `data/renewable_energy_text.txt` | [EXPERIMENT_3_MANUAL.md](file:///c:/Coding/data%20cleaning/EXPERIMENT_3_MANUAL.md) | 1. `screenshots/exp3_01_map_phase.png`<br>2. `screenshots/exp3_02_reduce_and_filter.png` |
| **4** | **Data Ingestion Pipeline & NoSQL Staging** | `python exp4_pipeline.py` | `hdfs_simulation/renewable_energy/processed/cleaned_energy_data.csv`<br>MongoDB Database: `renewable_db` (1,440 docs) | [EXPERIMENT_4_MANUAL.md](EXPERIMENT_4_MANUAL.md) | 1. `screenshots/exp4_01_pyspark_ingestion.png`<br>2. `screenshots/exp4_02_mongodb_staging_queries.png`<br>3. `screenshots/exp4_03_validation_checklist.png` |
| **Viva** | **Viva Voce Exam Preparation** | — | — | [VIVA_CHEAT_SHEET.md](VIVA_CHEAT_SHEET.md) | — |

---

## 2. One-Click Compile & Screenshot Automation

You do not need to take screenshots manually. Run the master compilation script anytime:
```bash
python compile_all_experiments_and_screenshots.py
```
This script executes all 4 experiments and automatically generates dark-themed, high-resolution PNG screenshot images into the `screenshots/` directory.

---

## 3. Detailed Deliverables Breakdown by Experiment

### Experiment 1: Domain Selection, Setup & EDA
- **Objective:** Verify Big Data environment and conduct initial Exploratory Data Analysis on real Solar vs Wind data.
- **What You Submit:**
  - Lab Manual text from `EXPERIMENT_1_MANUAL.md`.
  - Python scripts `verify_environment.py` and `exp1_eda.py`.
- **Screenshots to Include:**
  1. `screenshots/exp1_01_environment_check.png` (Shows Python 3.12, Java 22, PySpark 4.2.0, and 720 records detected).
  2. `screenshots/exp1_02_eda_comparison.png` (Shows Solar and Wind dimensions, schema, missing values, descriptive statistics, and the Comparative Summary Table).

### Experiment 2: Project Proposal & Architecture
- **Objective:** Articulate problem statement, objectives, team roles, and design a 6-stage linear Big Data architecture.
- **What You Submit:**
  - Lab Manual text from `EXPERIMENT_2_MANUAL.md`.
  - Architecture diagram and Draw.io file.
- **Screenshots to Include:**
  1. `screenshots/exp2_01_architecture_diagram.png` (The 6-stage pipeline diagram: CSV → HDFS → PySpark → Staging → MongoDB → Analytics).
  2. `screenshots/exp2_02_generator_terminal.png` (Terminal output confirming diagram generation).

### Experiment 3: MapReduce Processing on Unstructured Text
- **Objective:** Demonstrate distributed MapReduce concepts (Map, Shuffle & Sort, Reduce, Filter) using PySpark RDDs on real renewable energy text.
- **What You Submit:**
  - Lab Manual text from `EXPERIMENT_3_MANUAL.md`.
  - Python script `exp3_mapreduce.py`.
- **Screenshots to Include:**
  1. `screenshots/exp3_01_map_phase.png` (Shows Map phase emitting 520 tokens and sample `('solar', 1)` pairs).
  2. `screenshots/exp3_02_reduce_and_filter.png` (Shows Reduce phase grouping into 246 unique keys, Top 10 words table, and filtered keyword counts).

### Experiment 4: Data Ingestion Pipeline & NoSQL Staging
- **Objective:** Build an end-to-end data pipeline reading raw CSVs from HDFS, cleaning in PySpark, and staging into MongoDB.
- **What You Submit:**
  - Lab Manual text from `EXPERIMENT_4_MANUAL.md`.
  - Python script `exp4_pipeline.py`.
- **Screenshots to Include:**
  1. `screenshots/exp4_01_pyspark_ingestion.png` (Shows HDFS setup, PySpark raw ingestion of 720 Solar and 720 Wind rows, schema casting, and Solar aggregations).
  2. `screenshots/exp4_02_mongodb_staging_queries.png` (Shows Wind aggregations, combined metrics, cleaned CSV export to HDFS processed staging, and staging 1,440 documents into MongoDB).
  3. `screenshots/exp4_03_validation_checklist.png` (Shows MongoDB NoSQL queries for peak generation, grouped generation metrics, and the 6-point end-to-end validation checklist).
