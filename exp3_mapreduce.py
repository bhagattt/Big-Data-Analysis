"""
=============================================================================
EXPERIMENT 3: MapReduce Processing on Unstructured Text Data
Title: Renewable Energy Keyword Extraction & Word Count
Framework: PySpark RDD (Resilient Distributed Datasets)
=============================================================================
Aim:
  To demonstrate the distributed MapReduce processing paradigm on unstructured
  renewable energy text data using PySpark RDDs for Word Count and Keyword Filtering.

Objective:
  1. Ingest unstructured text data into a PySpark RDD.
  2. Implement the Map phase (tokenization, cleaning, and emitting (word, 1) pairs).
  3. Implement the Shuffle and Reduce phase using reduceByKey to aggregate counts.
  4. Perform Keyword Filtering for domain-specific terms (solar, wind, energy, power, renewable).
  5. Demonstrate the distributed execution lifecycle for academic viva defense.
=============================================================================
"""

import os
import sys
import re

# Ensure PySpark workers on Windows invoke the active Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

TEXT_FILE = "data/renewable_energy_text.txt"
TARGET_KEYWORDS = ["solar", "wind", "energy", "power", "renewable"]

def banner(title):
    print("\n" + "=" * 70)
    print(f" >>> {title}")
    print("=" * 70)

def clean_and_tokenize(line):
    """
    Cleans punctuation and tokenizes a line into lowercase words.
    Mapper helper function.
    """
    words = re.findall(r'\b[a-zA-Z]{3,}\b', line.lower())
    return words

def run_pyspark_mapreduce():
    from pyspark.sql import SparkSession

    banner("PHASE 1: Initializing Apache Spark Session & SparkContext")
    spark = SparkSession.builder \
        .appName("Exp3_MapReduce_TextProcessing") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    print(f"[+] SparkContext Active: AppName='{sc.appName}', Master='{sc.master}'")

    banner("PHASE 2: Loading Raw Unstructured Text into RDD")
    # Read text file into an RDD of lines
    lines_rdd = sc.textFile(TEXT_FILE)
    total_lines = lines_rdd.count()
    print(f"[+] Loaded file: {TEXT_FILE}")
    print(f"[+] Total Lines in RDD: {total_lines}")
    print("\n--- Sample Raw Line ---")
    print(lines_rdd.take(3)[1][:100] + "...")

    banner("PHASE 3: MAP PHASE (Tokenization & (Word, 1) Pairing)")
    print("Concept: Each line is split into words (flatMap), and mapped to a key-value pair (word, 1).")
    
    # 1. flatMap: One line -> Multiple words
    words_rdd = lines_rdd.flatMap(clean_and_tokenize)
    
    # 2. map: word -> (word, 1)
    word_pairs_rdd = words_rdd.map(lambda word: (word, 1))

    total_words = word_pairs_rdd.count()
    print(f"\n[+] Total Word Tokens Emitted: {total_words}")
    print("[+] Sample Mapper Output (First 8 Pairs):")
    for pair in word_pairs_rdd.take(8):
        print(f"    Map Output: {pair}")

    banner("PHASE 4: SHUFFLE & REDUCE PHASE (reduceByKey Aggregation)")
    print("Concept: Spark groups pairs with identical keys across partitions and sums their values.")
    
    # reduceByKey sums values for each distinct word key
    word_counts_rdd = word_pairs_rdd.reduceByKey(lambda count1, count2: count1 + count2)
    distinct_word_count = word_counts_rdd.count()
    print(f"[+] Distinct Unique Words Counted: {distinct_word_count}")

    # Top 10 most frequent words overall
    top_10_words = word_counts_rdd.takeOrdered(10, key=lambda x: -x[1])
    print("\n--- Top 10 Most Frequent Words Overall ---")
    print(f"{'Rank':<6} | {'Word':<15} | {'Frequency':<10}")
    print("-" * 38)
    for rank, (w, count) in enumerate(top_10_words, 1):
        print(f"{rank:<6} | {w:<15} | {count:<10}")

    banner("PHASE 5: KEYWORD FILTERING (Renewable Energy Terms)")
    print(f"Target Keywords to Filter: {TARGET_KEYWORDS}")
    
    # Filter RDD for only our selected renewable energy keywords
    filtered_keywords_rdd = word_counts_rdd.filter(lambda pair: pair[0] in TARGET_KEYWORDS)
    keyword_results = sorted(filtered_keywords_rdd.collect(), key=lambda x: -x[1])

    print("\n--- Filtered Renewable Energy Keyword Counts ---")
    print(f"{'Keyword':<15} | {'Count':<8} | {'MapReduce Key-Value Result'}")
    print("-" * 55)
    for kw, cnt in keyword_results:
        print(f"{kw:<15} | {cnt:<8} | ('{kw}', {cnt})")

    banner("PHASE 6: MapReduce Paradigm Walkthrough (For Viva)")
    print("""
Execution Pipeline Summary:
1. Input Splitting : File partitioned across nodes in cluster.
2. Map Phase       : line -> words -> (word, 1)
3. Shuffle & Sort  : Intermediate (key, value) pairs sorted and grouped by key.
4. Reduce Phase    : Sums 1s per key: reduceByKey(lambda a, b: a + b) -> (key, total)
5. Filter Phase    : Extracts domain keywords without reprocessing the raw text.
    """)

    spark.stop()
    print("[+] PySpark session terminated cleanly.")

def main():
    print("#" * 70)
    print("  EXPERIMENT 3: MAPREDUCE PROCESSING ON UNSTRUCTURED TEXT DATA")
    print("#" * 70)
    run_pyspark_mapreduce()
    print("\n" + "=" * 70)
    print(" [RESULT] Experiment 3 executed successfully.")
    print("=" * 70)

if __name__ == "__main__":
    main()
