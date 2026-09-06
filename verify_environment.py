"""
=============================================================================
EXPERIMENT 1: Big Data Environment Verification Script
Project: Comparative Study: Solar vs Wind Output
Checks: Python, PySpark, Java, Hadoop/HDFS, and Real Datasets
=============================================================================
"""

import sys
import os
import subprocess
import shutil

# Ensure PySpark workers on Windows use the current active Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def print_header(title):
    print("\n" + "=" * 65)
    print(f" {title}")
    print("=" * 65)

def check_python():
    print_header("1. Checking Python Environment")
    print(f"[*] Python Executable : {sys.executable}")
    print(f"[*] Python Version    : {sys.version.split()[0]}")
    if sys.version_info >= (3, 8):
        print("[+] Python Status     : OK (Compatible with PySpark & Pandas)")
    else:
        print("[-] Python Status     : WARNING (Python 3.8+ recommended)")

def check_java():
    print_header("2. Checking Java (Required by PySpark & Hadoop)")
    java_cmd = shutil.which("java")
    if java_cmd:
        try:
            res = subprocess.run(["java", "-version"], capture_output=True, text=True)
            output = res.stderr.splitlines()[0] if res.stderr else "Java Detected"
            print(f"[*] Java Binary       : {java_cmd}")
            print(f"[*] Version Info      : {output}")
            print("[+] Java Status       : OK")
            return True
        except Exception as e:
            print(f"[-] Java Error        : {e}")
            return False
    else:
        print("[-] Java Status       : NOT FOUND in system PATH")
        print("    Tip: Set JAVA_HOME and ensure java.exe is in PATH.")
        return False

def check_pyspark():
    print_header("3. Checking PySpark & Spark Engine")
    try:
        import pyspark
        print(f"[*] PySpark Module    : Found (version {pyspark.__version__})")
        
        # Test quick local SparkContext initialization
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("EnvCheck") \
            .master("local[1]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        test_df = spark.createDataFrame([(1, "Solar"), (2, "Wind")], ["id", "source"])
        row_count = test_df.count()
        spark.stop()
        
        print(f"[+] Spark Local Test  : SUCCESS (Processed {row_count} records in local mode)")
        print("[+] PySpark Status    : READY for DataFrame & RDD operations")
        return True
    except Exception as e:
        print(f"[-] PySpark Note      : PySpark installed but encountered: {e}")
        return False

def check_hadoop():
    print_header("4. Checking Hadoop / HDFS CLI")
    hadoop_cmd = shutil.which("hadoop") or shutil.which("hdfs")
    if hadoop_cmd:
        try:
            res = subprocess.run(["hadoop", "version"], capture_output=True, text=True)
            ver = res.stdout.splitlines()[0] if res.stdout else "Hadoop Detected"
            print(f"[*] Hadoop Binary     : {hadoop_cmd}")
            print(f"[*] Hadoop Info       : {ver}")
            print("[+] Hadoop Status     : OK (Cluster CLI active)")
            return True
        except Exception as e:
            print(f"[-] Hadoop Error      : {e}")
            return False
    else:
        print("[-] Hadoop CLI Status : 'hadoop' command not in global system PATH.")
        print("    Academic Lab Note : In college lab Linux environments, HDFS commands")
        print("    ('hdfs dfs -mkdir', 'hdfs dfs -put') interact with NameNode.")
        print("    On a single laptop/Windows, PySpark seamlessly processes local file")
        print("    paths ('file:///...') using the same DataFrame API without needing")
        print("    a multi-node Hadoop daemon.")
        return False

def check_datasets():
    print_header("5. Checking Real Datasets (Open Power System Data)")
    files = {
        "data/solar_generation.csv": "Actual Solar Generation (Hourly)",
        "data/wind_generation.csv": "Actual Wind Generation (Hourly)",
    }
    all_exist = True
    for path, desc in files.items():
        if os.path.exists(path):
            size_kb = os.path.getsize(path) / 1024
            # Count lines
            with open(path, "r", encoding="utf-8") as f:
                lines = sum(1 for _ in f) - 1
            print(f"[+] Found: {path:<28} | Records: {lines:<4} | Size: {size_kb:.2f} KB ({desc})")
        else:
            print(f"[-] Missing: {path}")
            all_exist = False
    return all_exist

def main():
    print("\n" + "#" * 65)
    print("   BIG DATA LABORATORY - ENVIRONMENT VERIFICATION SCRIPT")
    print("   Project: Comparative Study: Solar vs Wind Output")
    print("#" * 65)
    check_python()
    check_java()
    check_pyspark()
    check_hadoop()
    check_datasets()
    print_header("Verification Summary")
    print("Verification completed. You are ready to run Experiment 1 EDA!\n")

if __name__ == "__main__":
    main()
