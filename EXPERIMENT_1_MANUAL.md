# EXPERIMENT 1 — Domain Selection, Setup & EDA

**Title:** Comparative Study: Solar vs Wind Output  
**Dataset Source:** Open Power System Data (OPSD) – European Power System (Germany Grid actuals)  
**Timeframe:** 720 consecutive hourly observations (1 Full Month)

---

## 1. Aim
To set up the Big Data analytical environment and conduct Exploratory Data Analysis (EDA) on real-world Solar and Wind power generation datasets using Pandas and Apache Spark (PySpark).

---

## 2. Objective
1. Verify prerequisite Big Data technologies (**Python**, **PySpark**, **Java**, and **Hadoop/HDFS**).
2. Establish a structured directory layout for raw data storage and HDFS simulation.
3. Acquire authentic, real-world hourly generation datasets for Solar and Wind from **Open Power System Data (OPSD)**.
4. Load datasets into **Pandas** and **PySpark DataFrames**.
5. Perform fundamental Exploratory Data Analysis (EDA):
   - Record counts and dimensions
   - Schema, column names, and data types
   - Missing/null value identification
   - Sample row inspection
   - Basic descriptive statistics (Min, Max, Mean, Standard Deviation)
6. Compare the generation profiles and capacity factors between Solar and Wind.

---

## 3. Dataset Description
- **Selected Source:** [Open Power System Data (OPSD) Time Series](https://data.open-power-system-data.org/time_series/)
- **Geographic Domain:** Germany (`DE`), ENTSO-E transparency grid actuals.
- **Granularity:** Hourly resolution (`60min`).
- **Files Acquired:**
  1. `data/solar_generation.csv` (720 records, ~32 KB)
  2. `data/wind_generation.csv` (720 records, ~32 KB)
- **Schema Attributes:**
  - `utc_timestamp` (*String / Timestamp*): ISO-8601 UTC timestamp of observation.
  - `country` (*String*): Country country code (`DE`).
  - `source` (*String*): Energy type (`Solar` or `Wind`).
  - `power_output_mw` (*Float/Double*): Actual electrical output delivered to the grid in Megawatts (MW).
  - `capacity_mw` (*Float/Double*): Installed nameplate generation capacity in Megawatts (MW).

---

## 4. Tools & Environment
| Tool | Version / Purpose |
| :--- | :--- |
| **Python** | Python 3.12 (Core scripting and runtime engine) |
| **Java** | Java SE 22 (JVM runtime required by Apache Spark) |
| **PySpark** | Apache Spark 4.2.0 (In-memory distributed computing engine) |
| **Pandas** | Pandas 2.2.2 (Data manipulation and quick statistical analysis) |
| **Hadoop / HDFS** | HDFS CLI commands (`hdfs dfs -mkdir`, `hdfs dfs -put`) for distributed file operations |

---

## 5. Step-by-Step Execution Guide

### Step 1: Environment Verification
Before running data processing, verify that Python, Java, PySpark, and Hadoop tooling are functional.
Run the verification script:
```bash
python verify_environment.py
```
*What it means:* Checks compiler paths, imports PySpark, tests a minimal local SparkSession, and validates dataset availability.

### Step 2: HDFS Directory Setup & Data Upload (Hadoop Commands)
In a Hadoop-enabled cluster or terminal, execute the following commands to create the distributed folders and stage raw data:
```bash
# 1. Create directory hierarchy in HDFS
hdfs dfs -mkdir -p /renewable_energy/raw/solar
hdfs dfs -mkdir -p /renewable_energy/raw/wind
hdfs dfs -mkdir -p /renewable_energy/processed

# 2. Copy local real CSV datasets into HDFS
hdfs dfs -put data/solar_generation.csv /renewable_energy/raw/solar/
hdfs dfs -put data/wind_generation.csv /renewable_energy/raw/wind/

# 3. Verify files in HDFS
hdfs dfs -ls /renewable_energy/raw/solar
hdfs dfs -ls /renewable_energy/raw/wind
```
*What it means:* Allocates logical directory blocks in HDFS and copies the raw CSV files from local disk to the Hadoop distributed file system.

### Step 3: Run the Exploratory Data Analysis (EDA)
Run the automated EDA script:
```bash
python exp1_eda.py
```
*What it means:* Loads both datasets into Pandas and PySpark DataFrames, inspects schemas, computes descriptive metrics, and generates a side-by-side comparison table.

---

## 6. Python & PySpark Code

### File 1: `verify_environment.py`
```python
import sys, os, subprocess, shutil

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def main():
    print("=== 1. Python Check ===")
    print(f"Python: {sys.version.split()[0]} at {sys.executable}")

    print("\n=== 2. Java Check ===")
    java_cmd = shutil.which("java")
    if java_cmd:
        res = subprocess.run(["java", "-version"], capture_output=True, text=True)
        print(res.stderr.splitlines()[0] if res.stderr else "Java OK")
    else:
        print("Java not found in PATH.")

    print("\n=== 3. PySpark Check ===")
    try:
        import pyspark
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("EnvCheck").master("local[1]").getOrCreate()
        df = spark.createDataFrame([(1, "Solar"), (2, "Wind")], ["id", "source"])
        print(f"PySpark {pyspark.__version__} initialized successfully. Rows: {df.count()}")
        spark.stop()
    except Exception as e:
        print(f"PySpark issue: {e}")

    print("\n=== 4. Hadoop CLI Check ===")
    hadoop_cmd = shutil.which("hadoop") or shutil.which("hdfs")
    if hadoop_cmd:
        print(f"Hadoop CLI located at: {hadoop_cmd}")
    else:
        print("Hadoop CLI not in global PATH (standard for standalone single-node laptop setups).")

    print("\n=== 5. Real Dataset Check ===")
    for f in ["data/solar_generation.csv", "data/wind_generation.csv"]:
        if os.path.exists(f):
            print(f"Found: {f} ({os.path.getsize(f)/1024:.2f} KB)")
        else:
            print(f"Missing: {f}")

if __name__ == "__main__":
    main()
```

### File 2: `exp1_eda.py`
```python
import os, sys
import pandas as pd

# Set Python worker path for PySpark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

SOLAR_FILE = "data/solar_generation.csv"
WIND_FILE = "data/wind_generation.csv"

def run_eda():
    # 1. Load data
    df_solar = pd.read_csv(SOLAR_FILE)
    df_wind = pd.read_csv(WIND_FILE)

    print("=== Dimensions ===")
    print(f"Solar: {df_solar.shape[0]} rows, {df_solar.shape[1]} columns")
    print(f"Wind : {df_wind.shape[0]} rows, {df_wind.shape[1]} columns")

    print("\n=== Data Types (Schema) ===")
    print(df_solar.dtypes)

    print("\n=== Missing Values ===")
    print("Solar nulls:\n", df_solar.isnull().sum())
    print("Wind nulls:\n", df_wind.isnull().sum())

    print("\n=== First 5 Records (Solar) ===")
    print(df_solar.head(5).to_string(index=False))

    print("\n=== First 5 Records (Wind) ===")
    print(df_wind.head(5).to_string(index=False))

    print("\n=== Descriptive Statistics (Power Output in MW) ===")
    print("--- Solar ---\n", df_solar['power_output_mw'].describe())
    print("--- Wind ---\n", df_wind['power_output_mw'].describe())

    # Comparative Summary
    s_cap = df_solar['capacity_mw'].iloc[0]
    w_cap = df_wind['capacity_mw'].iloc[0]
    s_mean = df_solar['power_output_mw'].mean()
    w_mean = df_wind['power_output_mw'].mean()

    print("\n=== Comparative Summary ===")
    summary = pd.DataFrame({
        "Metric": ["Source", "Total Hours", "Capacity (MW)", "Min (MW)", "Max (MW)", "Mean (MW)", "Zero Gen Hours", "Capacity Factor"],
        "Solar": ["OPSD (DE)", len(df_solar), s_cap, df_solar['power_output_mw'].min(), df_solar['power_output_mw'].max(), round(s_mean, 2), f"{(df_solar['power_output_mw']==0).sum()} hrs", f"{(s_mean/s_cap)*100:.2f}%"],
        "Wind":  ["OPSD (DE)", len(df_wind),  w_cap, df_wind['power_output_mw'].min(),  df_wind['power_output_mw'].max(),  round(w_mean, 2), f"{(df_wind['power_output_mw']==0).sum()} hrs",  f"{(w_mean/w_cap)*100:.2f}%"]
    })
    print(summary.to_string(index=False))

    # PySpark Verification
    print("\n=== PySpark DataFrame Verification ===")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Exp1_EDA").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df_sp = spark.read.option("header", "true").option("inferSchema", "true").csv(SOLAR_FILE)
    df_sp.printSchema()
    df_sp.show(5)
    spark.stop()

if __name__ == "__main__":
    run_eda()
```

---

## 7. Expected Output
```text
=== Dimensions ===
Solar: 720 rows, 5 columns
Wind : 720 rows, 5 columns

=== Data Types (Schema) ===
utc_timestamp       object
country             object
source              object
power_output_mw    float64
capacity_mw        float64

=== Missing Values ===
Solar nulls:
utc_timestamp      0
country            0
source             0
power_output_mw    0
capacity_mw        0

=== First 5 Records (Solar) ===
       utc_timestamp country source  power_output_mw  capacity_mw
2019-06-01T00:00:00Z      DE  Solar              0.0      49497.0
2019-06-01T01:00:00Z      DE  Solar              0.0      49497.0
2019-06-01T02:00:00Z      DE  Solar              2.0      49497.0
2019-06-01T03:00:00Z      DE  Solar            250.0      49497.0
2019-06-01T04:00:00Z      DE  Solar           2006.0      49497.0

=== Descriptive Statistics (Power Output in MW) ===
--- Solar ---
count      720.000000
mean      8964.283333
std       9864.555336
min          0.000000
max      29967.000000
--- Wind ---
count      720.000000
mean      9144.172222
std       5986.608033
min       1196.000000
max      33025.000000

=== Comparative Summary ===
         Metric      Solar       Wind
         Source  OPSD (DE)  OPSD (DE)
    Total Hours        720        720
  Capacity (MW)    49497.0    49370.0
       Min (MW)        0.0     1196.0
       Max (MW)    29967.0    33025.0
      Mean (MW)    8964.28    9144.17
 Zero Gen Hours    179 hrs      0 hrs
Capacity Factor     18.11%     18.52%

=== PySpark DataFrame Verification ===
root
 |-- utc_timestamp: timestamp (nullable = true)
 |-- country: string (nullable = true)
 |-- source: string (nullable = true)
 |-- power_output_mw: double (nullable = true)
 |-- capacity_mw: double (nullable = true)
```

---

## 8. Result
1. The Big Data environment was verified successfully with Python 3.12, Java 22, and PySpark 4.2.0 operational in local mode.
2. Authentic 720-hour (1 month) solar and wind time-series datasets from Open Power System Data were loaded into both Pandas and PySpark DataFrames.
3. Exploratory Data Analysis demonstrated that the data is 100% complete with 0 missing values.
4. **Key Finding:** Solar power is intermittent and strictly diurnal (179 zero-generation night hours, peaking at 29,967 MW during midday), whereas Wind power generates continuously throughout day and night (minimum 1,196 MW, peaking at 33,025 MW). Both achieved comparable monthly capacity factors (~18.1% vs ~18.5%).

---

## 9. Viva Explanation (Questions & Quick Answers)

**Q1: What is the purpose of Exploratory Data Analysis (EDA) in Big Data projects?**  
*Answer:* EDA helps us understand the structure, schema, distribution, and data quality (such as missing values or corrupt records) before pushing large datasets into heavy distributed computing pipelines or databases.

**Q2: What is the difference between loading data in Pandas vs. PySpark?**  
*Answer:* Pandas loads the entire dataset into a single machine's RAM, which works well for small to medium data. PySpark creates a distributed DataFrame that partitions data across cluster nodes and evaluates transformations lazily, allowing it to process massive multi-gigabyte/terabyte datasets.

**Q3: Why do we observe zero output in solar data but not in wind data?**  
*Answer:* Solar generation directly depends on sunlight (solar irradiance), so generation is naturally zero at night. Wind turbines depend on atmospheric pressure gradients and wind currents, which persist during nighttime, allowing continuous 24-hour power generation.

**Q4: What is Capacity Factor and why is it important?**  
*Answer:* Capacity Factor is the ratio of actual energy produced over a period to the maximum theoretical output if the plant operated at 100% capacity. It measures how effectively the installed renewable power capacity is utilized.

---

## 10. Screenshots to Add in Lab Record
When compiling your lab record, paste the following 3 screenshots:

1. **Screenshot 1: Environment Verification Terminal Output**
   - Command: `python verify_environment.py`
   - Content to capture: Terminal showing Python version, Java version, PySpark OK, and the dataset check confirming 720 records found.
2. **Screenshot 2: Exploratory Data Analysis (EDA) Terminal Output**
   - Command: `python exp1_eda.py`
   - Content to capture: The First 5 Rows of Solar & Wind, Descriptive Statistics, and especially the **Comparative Summary Table** (`STEP 6`).
3. **Screenshot 3: PySpark DataFrame Schema & Show Output**
   - Command: `python exp1_eda.py`
   - Content to capture: The `STEP 8` section showing `root |-- utc_timestamp: timestamp ...` and `df_sp_solar.show(5)`.

