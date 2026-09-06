"""
=============================================================================
MASTER SCRIPT: Compile All Experiments & Generate Lab Screenshots
Title: Comparative Study: Solar vs Wind Output
Function:
  1. Executes all 4 Big Data laboratory experiments sequentially.
  2. Captures stdout/results from each experiment.
  3. Renders high-resolution, dark-themed terminal screenshots into ./screenshots/
  4. Prepares ready-to-paste PNG images for lab records and report submissions.
=============================================================================
"""

import os
import sys
import subprocess
import shutil
from PIL import Image, ImageDraw, ImageFont

# Environment variables for PySpark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

SCREENSHOTS_DIR = "screenshots"
FONT_PATH = os.path.join(os.environ.get("WINDIR", "C:\\Windows"), "Fonts", "consola.ttf")

def get_font(size=15):
    try:
        return ImageFont.truetype(FONT_PATH, size)
    except Exception:
        return ImageFont.load_default()

def render_terminal_screenshot(title, text_lines, output_file, width=1050):
    """
    Renders console text lines into a sleek, professional dark-mode terminal window.
    """
    font = get_font(14)
    title_font = get_font(13)
    
    # Filter out noisy Hadoop winutils or JVM warnings for clean presentation
    clean_lines = []
    for line in text_lines:
        line_clean = line.rstrip()
        if "WARN Shell:" in line_clean or "WARN NativeCodeLoader:" in line_clean:
            continue
        if "Using incubator modules" in line_clean or "Using Spark's default log4j" in line_clean:
            continue
        if "Setting default log level" in line_clean or "SUCCESS: The process with PID" in line_clean:
            continue
        if "toPandas attempted Arrow optimization" in line_clean or "Attempting non-optimization" in line_clean:
            continue
        clean_lines.append(line_clean)

    # Wrap or truncate lines to width
    max_chars_per_line = 115
    wrapped_lines = []
    for line in clean_lines:
        if len(line) > max_chars_per_line:
            wrapped_lines.append(line[:max_chars_per_line] + "...")
        else:
            wrapped_lines.append(line)

    line_height = 20
    header_height = 42
    padding = 24
    height = header_height + (len(wrapped_lines) * line_height) + (padding * 2)

    # Base canvas (Dark background: #1E1E2E)
    img = Image.new("RGB", (width, height), color="#1E1E2E")
    draw = ImageDraw.Draw(img)

    # Header Bar (#2A2A3C)
    draw.rectangle([(0, 0), (width, header_height)], fill="#252538")

    # Terminal buttons (Red, Yellow, Green window dots)
    dot_radius = 6
    dot_y = header_height // 2
    draw.ellipse([(18 - dot_radius, dot_y - dot_radius), (18 + dot_radius, dot_y + dot_radius)], fill="#FF5F56")
    draw.ellipse([(38 - dot_radius, dot_y - dot_radius), (38 + dot_radius, dot_y + dot_radius)], fill="#FFBD2E")
    draw.ellipse([(58 - dot_radius, dot_y - dot_radius), (58 + dot_radius, dot_y + dot_radius)], fill="#27C93F")

    # Header title text
    draw.text((85, 12), f"Terminal - {title}", fill="#A6ADC8", font=title_font)

    # Draw text lines
    y_cursor = header_height + padding
    for line in wrapped_lines:
        # Syntax highlight simulation
        text_color = "#CDD6F4"  # Default off-white
        if line.startswith("===") or line.startswith(">>>") or line.startswith("---") or line.startswith("###"):
            text_color = "#89B4FA"  # Bright Blue header
        elif "[+]" in line or "SUCCESS" in line or "[RESULT]" in line or "[x]" in line:
            text_color = "#A6E3A1"  # Mint Green for success
        elif "[-]" in line or "ERROR" in line or "WARN" in line:
            text_color = "#F38BA8"  # Soft Red for alerts
        elif "Metric" in line or "Rank" in line or "Keyword" in line or "root" in line:
            text_color = "#F9E2AF"  # Yellow for tables
        elif line.strip().startswith("|") or line.strip().startswith("+-"):
            text_color = "#94E2D5"  # Teal for grid tables

        draw.text((padding, y_cursor), line, fill=text_color, font=font)
        y_cursor += line_height

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    img.save(output_file, "PNG", optimize=True)
    print(f"  [+] Saved screenshot: {output_file}")

def run_command_capture(cmd):
    """Executes a python command and captures stdout lines."""
    res = subprocess.run([sys.executable] + cmd, capture_output=True, text=True)
    return res.stdout.splitlines()

def main():
    print("=" * 70)
    print("  BIG DATA MINI-PROJECT: MASTER EXPERIMENT COMPILER & SCREENSHOTS")
    print("=" * 70)
    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

    # -------------------------------------------------------------
    # Experiment 1: Setup & EDA
    # -------------------------------------------------------------
    print("\n[1/4] Running & Capturing Experiment 1...")
    # Verify environment
    verify_lines = run_command_capture(["verify_environment.py"])
    render_terminal_screenshot(
        "Experiment 1 - Environment Verification",
        verify_lines,
        os.path.join(SCREENSHOTS_DIR, "exp1_01_environment_check.png")
    )
    # EDA
    eda_lines = run_command_capture(["exp1_eda.py"])
    render_terminal_screenshot(
        "Experiment 1 - Exploratory Data Analysis (Solar vs Wind)",
        eda_lines,
        os.path.join(SCREENSHOTS_DIR, "exp1_02_eda_comparison.png")
    )

    # -------------------------------------------------------------
    # Experiment 2: Architecture
    # -------------------------------------------------------------
    print("\n[2/4] Running & Capturing Experiment 2...")
    arch_lines = run_command_capture(["exp2_generate_architecture.py"])
    render_terminal_screenshot(
        "Experiment 2 - Architecture Generator Output",
        arch_lines,
        os.path.join(SCREENSHOTS_DIR, "exp2_02_generator_terminal.png")
    )
    # Copy high-res architecture diagram to screenshots folder
    if os.path.exists("architecture_diagram.png"):
        dest_arch = os.path.join(SCREENSHOTS_DIR, "exp2_01_architecture_diagram.png")
        shutil.copyfile("architecture_diagram.png", dest_arch)
        print(f"  [+] Saved screenshot: {dest_arch}")

    # -------------------------------------------------------------
    # Experiment 3: MapReduce (Split into 2 focused screenshots)
    # -------------------------------------------------------------
    print("\n[3/4] Running & Capturing Experiment 3...")
    mr_lines = run_command_capture(["exp3_mapreduce.py"])

    # Split lines into Map Phase and Reduce/Filter Phase
    map_lines = []
    reduce_lines = []
    target_list = map_lines
    for line in mr_lines:
        if "PHASE 4: SHUFFLE & REDUCE PHASE" in line:
            target_list = reduce_lines
        target_list.append(line)

    # Screenshot 1: Map Phase
    render_terminal_screenshot(
        "Experiment 3 (Part 1) - Map Phase & Token Pairing",
        map_lines,
        os.path.join(SCREENSHOTS_DIR, "exp3_01_map_phase.png")
    )
    # Screenshot 2: Reduce & Filter Phase
    render_terminal_screenshot(
        "Experiment 3 (Part 2) - Reduce Phase & Keyword Filtering",
        reduce_lines,
        os.path.join(SCREENSHOTS_DIR, "exp3_02_reduce_and_filter.png")
    )
    # Also maintain full combined screenshot for backwards compatibility
    render_terminal_screenshot(
        "Experiment 3 - Full MapReduce Results",
        mr_lines,
        os.path.join(SCREENSHOTS_DIR, "exp3_01_mapreduce_results.png")
    )

    # -------------------------------------------------------------
    # Experiment 4: Ingestion Pipeline & MongoDB
    # -------------------------------------------------------------
    print("\n[4/4] Running & Capturing Experiment 4...")
    pipeline_lines = run_command_capture(["exp4_pipeline.py"])
    render_terminal_screenshot(
        "Experiment 4 - Data Ingestion Pipeline & MongoDB Staging",
        pipeline_lines,
        os.path.join(SCREENSHOTS_DIR, "exp4_01_pipeline_mongodb.png")
    )

    print("\n" + "=" * 70)
    print(" [SUCCESS] All 4 Experiments executed and screenshots compiled!")
    print(f" Check the '{SCREENSHOTS_DIR}/' folder for ready-to-paste PNG images.")
    print("=" * 70)

if __name__ == "__main__":
    main()
