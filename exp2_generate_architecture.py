"""
=============================================================================
EXPERIMENT 2: System Architecture Diagram Generator
Project: Comparative Study: Solar vs Wind Output
Generates: architecture_diagram.png (High-Resolution Diagram)
=============================================================================
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches

def draw_architecture():
    fig, ax = plt.subplots(figsize=(10, 8), dpi=300)
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 12)
    ax.axis("off")

    # Define the 6 architecture stages
    boxes = [
        {
            "y": 10.0,
            "title": "Stage 1: Raw Data Sources",
            "text": "Solar & Wind CSV Datasets\n(Open Power System Data - 60min Hourly Actuals)",
            "bg": "#E3F2FD",
            "border": "#1976D2"
        },
        {
            "y": 8.0,
            "title": "Stage 2: Distributed Storage (HDFS)",
            "text": "Hadoop Distributed File System\n(/renewable_energy/raw/solar/ & /wind/)",
            "bg": "#FFF3E0",
            "border": "#F57C00"
        },
        {
            "y": 6.0,
            "title": "Stage 3: Distributed Processing (PySpark)",
            "text": "Apache Spark Engine\n(Schema enforcement, null filtering, capacity factor calculation)",
            "bg": "#E8F5E9",
            "border": "#388E3C"
        },
        {
            "y": 4.0,
            "title": "Stage 4: Processed Data Staging",
            "text": "Cleaned Structured Records\n(/renewable_energy/processed/)",
            "bg": "#F3E5F5",
            "border": "#7B1FA2"
        },
        {
            "y": 2.0,
            "title": "Stage 5: NoSQL Document Database",
            "text": "MongoDB Document Store\n(Database: renewable_db | Collection: energy_output)",
            "bg": "#E0F2F1",
            "border": "#00796B"
        },
        {
            "y": 0.3,
            "title": "Stage 6: Comparative Analytics & Reporting",
            "text": "Summary & Output Comparison\n(Peak output, diurnal cycles, capacity utilization)",
            "bg": "#EDE7F6",
            "border": "#512DA8"
        }
    ]

    box_width = 7.6
    box_height = 1.15
    box_x = 1.2

    # Title
    ax.text(5.0, 11.6, "System Architecture: Solar vs Wind Big Data Pipeline", 
            ha="center", va="center", fontsize=14, fontweight="bold", color="#1A237E")
    ax.text(5.0, 11.2, "Simplified Academic Big Data Laboratory Architecture", 
            ha="center", va="center", fontsize=10, style="italic", color="#424242")

    for i, b in enumerate(boxes):
        y = b["y"]
        # Draw Box
        rect = patches.FancyBboxPatch(
            (box_x, y), box_width, box_height,
            boxstyle="round,pad=0.15,rounding_size=0.15",
            linewidth=2, edgecolor=b["border"], facecolor=b["bg"]
        )
        ax.add_patch(rect)

        # Draw Title
        ax.text(box_x + box_width / 2, y + 0.82, b["title"],
                ha="center", va="center", fontsize=10.5, fontweight="bold", color=b["border"])
        # Draw Text
        ax.text(box_x + box_width / 2, y + 0.38, b["text"],
                ha="center", va="center", fontsize=9, color="#212121")

        # Draw connecting arrow to next box
        if i < len(boxes) - 1:
            next_y = boxes[i+1]["y"] + box_height + 0.15
            curr_bottom = y - 0.05
            ax.annotate(
                "",
                xy=(5.0, next_y),
                xytext=(5.0, curr_bottom),
                arrowprops=dict(
                    arrowstyle="->,head_width=0.4,head_length=0.4",
                    linewidth=2.2,
                    color="#37474F"
                )
            )

    plt.tight_layout()
    output_path = "architecture_diagram.png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"[+] Successfully generated high-res architecture diagram: {output_path}")

if __name__ == "__main__":
    draw_architecture()
