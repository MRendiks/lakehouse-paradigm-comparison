"""
Generate BigQuery vs Snowflake cold-run latency comparison chart.

Data source: BENCHMARK_REPORT.md from the lakehouse-paradigm-comparison repo
(github.com/MRendiks/lakehouse-paradigm-comparison)

Usage:
    pip install matplotlib
    python generate_benchmark_chart.py

Output:
    bigquery_vs_snowflake_benchmark.png (saved in the same folder as this script)
"""

import os
import matplotlib.pyplot as plt

# --- Data (edit these if you re-run the benchmark and get new numbers) ---
scenarios = [
    "Star Schema Join",
    "Subquery Semi-Join",
    "Pivot Distribution",
    "String Ops Parsing",
    "Multi-Join Enrichment",
    "Percentile Analytics",
    "Window Function (RFM)",
    "Rolling 30D Average",
    "Group By Aggregation",
    "Full Table Scan",
]
bigquery = [1.389, 1.385, 1.303, 1.244, 1.271, 0.452, 0.331, 0.312, 0.317, 0.512]
snowflake = [0.410, 0.393, 0.369, 0.387, 0.438, 0.303, 0.336, 0.307, 0.322, 0.556]

# How many of the top rows (biggest Snowflake advantage) to highlight + annotate.
# Scenarios are expected to be sorted by descending speedup already.
HIGHLIGHT_TOP_N = 5

# --- Colors (high-contrast, not both blue) ---
COLOR_BQ = "#F4511E"        # warm orange-red — BigQuery
COLOR_SF = "#1E88E5"        # strong blue — Snowflake
HIGHLIGHT_BG = "#FFF3E0"    # soft highlight band behind the big-gap cluster
ANNOTATION_COLOR = "#C2410C"

OUTPUT_FILENAME = "bigquery_vs_snowflake_benchmark.png"


def build_chart():
    speedup = [b / s for b, s in zip(bigquery, snowflake)]

    fig, ax = plt.subplots(figsize=(9.2, 6.4), dpi=200)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    y = list(range(len(scenarios)))
    bar_h = 0.36

    # Highlight band behind the rows with the biggest Snowflake advantage
    ax.axhspan(-0.5, HIGHLIGHT_TOP_N - 0.5, color=HIGHLIGHT_BG, zorder=0)

    bars_bq = ax.barh(
        [i + bar_h / 2 for i in y], bigquery, height=bar_h,
        color=COLOR_BQ, label="BigQuery", zorder=3,
    )
    bars_sf = ax.barh(
        [i - bar_h / 2 for i in y], snowflake, height=bar_h,
        color=COLOR_SF, label="Snowflake", zorder=3,
    )

    # Value labels at the end of each bar
    for bars, vals in ((bars_bq, bigquery), (bars_sf, snowflake)):
        for b, v in zip(bars, vals):
            ax.text(
                b.get_width() + 0.025, b.get_y() + b.get_height() / 2,
                f"{v:.2f}s", va="center", ha="left",
                fontsize=9, color="#333333",
            )

    # "Nx faster" annotation for the highlighted rows
    for i in range(HIGHLIGHT_TOP_N):
        ax.text(
            1.62, i, f"{speedup[i]:.1f}x faster",
            va="center", ha="right", fontsize=9.5,
            fontweight="bold", color=ANNOTATION_COLOR, zorder=4,
        )

    ax.set_yticks(y)
    ax.set_yticklabels(scenarios, fontsize=10.5, color="#222222")
    ax.invert_yaxis()
    ax.set_xlabel(
        "Cold Run Latency (seconds) — lower is better",
        fontsize=10.5, color="#444444",
    )
    ax.set_xlim(0, 1.75)
    ax.set_title(
        "BigQuery vs Snowflake: Cold Run Query Latency\n"
        "Snowflake is 3x+ faster on complex multi-join workloads",
        fontsize=13.5, fontweight="bold", color="#111111", pad=16,
    )

    ax.grid(axis="x", linestyle="-", linewidth=0.6, color="#e5e5e5", zorder=1)
    ax.set_axisbelow(True)
    for spine in ["top", "right", "left"]:
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color("#cccccc")
    ax.tick_params(left=False)

    ax.legend(loc="lower right", frameon=False, fontsize=10.5)

    fig.text(
        0.5, 0.005,
        "Source: lakehouse-paradigm-comparison — github.com/MRendiks",
        ha="center", fontsize=8, color="#999999",
    )

    plt.tight_layout(rect=[0, 0.02, 1, 1])

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUTPUT_FILENAME)
    plt.savefig(out_path, facecolor="white", bbox_inches="tight")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    build_chart()