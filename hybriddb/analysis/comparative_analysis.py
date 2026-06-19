"""
comparative_analysis.py
=======================
Reads benchmark_results.json and produces a focused comparative
analysis between framework and direct database access.

Run AFTER benchmark_runner.py has completed.

Output:
    - comparative_analysis.md
    - charts/comparative_read_latency.png
    - charts/comparative_update_latency.png
    - charts/overhead_breakdown.png
    - charts/comparative_throughput.png
"""

import json
import os
from datetime import datetime

from hybriddb.config import paths

try:
    import matplotlib.pyplot as plt
    plt_available = True
except ImportError:
    plt_available = False
    print("Warning: matplotlib not installed. Install with: pip install matplotlib")

paths.ensure_dirs()
RESULTS_FILE = paths.RESULTS_FILE
REPORT_FILE  = paths.COMPARATIVE_REPORT
CHARTS_DIR   = str(paths.CHARTS_DIR)


# ============================================================================
# Load Results
# ============================================================================

def load_results() -> dict:
    if not os.path.exists(RESULTS_FILE):
        raise FileNotFoundError(
            f"{RESULTS_FILE} not found. Run benchmark_runner.py first."
        )
    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================================
# Comparison Builders
# ============================================================================

def build_read_comparison(results: dict) -> dict:
    lq     = results.get("logical_query", {})
    dsql   = results.get("direct_sql", {})
    dmongo = results.get("direct_mongo", {})

    return {
        "single_record": {
            "Framework (Logical Query)": lq.get("single_record_by_pk", 0),
            "Direct SQL":                dsql.get("single_record_by_pk", 0),
            "Direct MongoDB":            dmongo.get("single_doc_by_pk", 0),
        },
        "all_records": {
            "Framework (Logical Query)": lq.get("all_records", 0),
            "Direct SQL":                dsql.get("all_records", 0),
            "Direct MongoDB":            dmongo.get("all_docs", 0),
        },
        "specific_fields": {
            "Framework (Logical Query)": lq.get("specific_fields", 0),
            "Direct SQL":                dsql.get("single_record_by_pk", 0),
            "Direct MongoDB":            dmongo.get("single_doc_by_pk", 0),
        },
    }


def build_update_comparison(results: dict) -> dict:
    fw_upd = results.get("framework_update", {})
    dsql   = results.get("direct_sql", {})
    dmongo = results.get("direct_mongo", {})

    return {
        "Framework (Coordinated Update)": fw_upd.get("update_latency", 0),
        "Direct SQL UPDATE":              dsql.get("update_latency", 0),
        "Direct MongoDB updateOne":       dmongo.get("update_latency", 0),
    }


def build_overhead_breakdown(results: dict) -> dict:
    coord   = results.get("coordination_overhead", {})
    meta_oh = results.get("metadata_overhead", {})

    framework_total = coord.get("framework_insert_ms", 0)
    direct_sql      = coord.get("direct_sql_insert_ms", 0)
    direct_mongo    = coord.get("direct_mongo_insert_ms", 0)
    direct_combined = direct_sql + direct_mongo
    overhead        = coord.get("coordination_overhead_ms", 0)
    metadata_cost   = meta_oh.get("metadata_load_ms", 0)

    coordination_logic = max(overhead - metadata_cost, 0)

    return {
        "Direct SQL insert":     direct_sql,
        "Direct MongoDB insert": direct_mongo,
        "direct_combined":       direct_combined,
        "Metadata load":         metadata_cost,
        "Coordination logic":    coordination_logic,
        "Framework total":       framework_total,
        "overhead_pct":          coord.get("coordination_overhead_pct", 0),
        "coordination_overhead_ms": overhead,
    }


def build_throughput_comparison(results: dict) -> dict:
    tp         = results.get("throughput", {})
    load_curve = tp.get("read_load_curve", {})

    return {
        "batch_sizes":    load_curve.get("batch_sizes", []),
        "ops_per_sec":    load_curve.get("ops_per_sec", []),
        "writes_per_sec": tp.get("writes_per_second", 0),
        "reads_per_sec":  tp.get("reads_per_second", 0),
    }


# ============================================================================
# Helpers
# ============================================================================

def _overhead_factor(framework_val: float, direct_val: float) -> str:
    if direct_val > 0:
        return f"{round(framework_val / direct_val, 1)}x"
    return "N/A"


def _pct_of_total(value: float, total: float) -> str:
    if total > 0:
        return f"{round(value / total * 100, 1)}%"
    return "0%"


# ============================================================================
# Chart Generation
# ============================================================================

def generate_comparative_charts(results: dict):
    if not plt_available:
        print("[WARNING] matplotlib not installed — skipping charts")
        return

    if not os.path.exists(CHARTS_DIR):
        os.makedirs(CHARTS_DIR)

    read_comp   = build_read_comparison(results)
    update_comp = build_update_comparison(results)
    overhead    = build_overhead_breakdown(results)
    tp          = build_throughput_comparison(results)

    # ── Chart 1: Read Latency Grouped Bar Chart ───────────────────────────
    print("    Generating comparative_read_latency.png ...")

    fig, ax = plt.subplots(figsize=(12, 6))

    categories = [
        "Single Record\n(by Primary Key)",
        "All Records",
        "Specific Fields Only",
    ]

    framework_vals = [
        read_comp["single_record"]["Framework (Logical Query)"],
        read_comp["all_records"]["Framework (Logical Query)"],
        read_comp["specific_fields"]["Framework (Logical Query)"],
    ]
    sql_vals = [
        read_comp["single_record"]["Direct SQL"],
        read_comp["all_records"]["Direct SQL"],
        read_comp["specific_fields"]["Direct SQL"],
    ]
    mongo_vals = [
        read_comp["single_record"]["Direct MongoDB"],
        read_comp["all_records"]["Direct MongoDB"],
        read_comp["specific_fields"]["Direct MongoDB"],
    ]

    x     = list(range(len(categories)))
    width = 0.25

    bars1 = ax.bar(
        [i - width for i in x], framework_vals, width,
        label="Framework", color="#0f5e9c", edgecolor="black",
    )
    bars2 = ax.bar(
        x, sql_vals, width,
        label="Direct SQL", color="#2e7d32", edgecolor="black",
    )
    bars3 = ax.bar(
        [i + width for i in x], mongo_vals, width,
        label="Direct MongoDB", color="#f57c00", edgecolor="black",
    )

    for bars in [bars1, bars2, bars3]:
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    h + 0.3,
                    f"{h:.1f}",
                    ha="center", va="bottom", fontsize=8,
                )

    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Read Query Latency: Framework vs Direct Database Access")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(CHARTS_DIR, "comparative_read_latency.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"    Saved: {path}")

    # ── Chart 2: Update Latency Bar Chart ────────────────────────────────
    print("    Generating comparative_update_latency.png ...")

    fig, ax = plt.subplots(figsize=(8, 5))

    upd_labels = list(update_comp.keys())
    upd_values = list(update_comp.values())
    upd_colors = ["#0f5e9c", "#2e7d32", "#f57c00"]

    bars = ax.bar(
        upd_labels, upd_values,
        color=upd_colors, edgecolor="black", width=0.5,
    )
    for bar, val in zip(bars, upd_values):
        if val > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.3,
                f"{val:.1f} ms",
                ha="center", va="bottom", fontsize=9,
            )

    ax.set_ylabel("Latency (ms)")
    ax.set_title("Update Latency: Framework vs Direct Database Access")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(CHARTS_DIR, "comparative_update_latency.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"    Saved: {path}")

    # ── Chart 3: Overhead Stacked Bar ─────────────────────────────────────
    print("    Generating overhead_breakdown.png ...")

    fig, ax = plt.subplots(figsize=(8, 5))

    stack_labels    = ["Direct Combined\n(SQL + Mongo)", "Framework Total"]
    direct_combined = overhead["direct_combined"]
    meta_cost       = overhead["Metadata load"]
    coord_logic     = overhead["Coordination logic"]
    fw_total        = overhead["Framework total"]

    ax.bar(
        stack_labels,
        [direct_combined, direct_combined],
        color="#2e7d32", edgecolor="black",
        label="Direct DB operations",
    )
    ax.bar(
        stack_labels,
        [0, meta_cost],
        bottom=[direct_combined, direct_combined],
        color="#ff9800", edgecolor="black",
        label="Metadata load overhead",
    )
    ax.bar(
        stack_labels,
        [0, coord_logic],
        bottom=[direct_combined, direct_combined + meta_cost],
        color="#e53935", edgecolor="black",
        label="Coordination logic",
    )

    ax.text(
        1, fw_total + 0.5,
        f"Total: {fw_total:.1f} ms\n(+{overhead['overhead_pct']}% overhead)",
        ha="center", fontsize=9,
        color="#0f5e9c", fontweight="bold",
    )

    ax.set_ylabel("Time (ms)")
    ax.set_title("Framework Insert Overhead Breakdown")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(CHARTS_DIR, "overhead_breakdown.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"    Saved: {path}")

    # ── Chart 4: Throughput Line Graph ───────────────────────────────────
    print("    Generating comparative_throughput.png ...")

    if tp["batch_sizes"] and tp["ops_per_sec"]:
        fig, ax = plt.subplots(figsize=(9, 5))

        ax.plot(
            tp["batch_sizes"],
            tp["ops_per_sec"],
            marker="o", linewidth=2.5, markersize=9,
            color="#0f5e9c",
            label="Framework reads/sec",
        )

        all_records_ms = results.get("direct_sql", {}).get("all_records", 0)
        if all_records_ms > 0:
            direct_sql_ops = int(1000 / all_records_ms)
            ax.axhline(
                y=direct_sql_ops,
                color="#2e7d32", linestyle="--", linewidth=2,
                label=f"Direct SQL (est. {direct_sql_ops} ops/sec)",
            )

        for x_val, y_val in zip(tp["batch_sizes"], tp["ops_per_sec"]):
            ax.annotate(
                str(y_val),
                xy=(x_val, y_val),
                xytext=(0, 10),
                textcoords="offset points",
                ha="center", fontsize=9,
            )

        ax.set_xlabel("Batch Size (number of sequential reads)")
        ax.set_ylabel("Operations per Second")
        ax.set_title("Read Throughput Under Increasing Workload")
        ax.legend()
        ax.grid(alpha=0.3)

        plt.tight_layout()
        path = os.path.join(CHARTS_DIR, "comparative_throughput.png")
        plt.savefig(path, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"    Saved: {path}")


# ============================================================================
# Markdown Report
# ============================================================================

def generate_comparative_report(results: dict, timestamp: str):
    read_comp   = build_read_comparison(results)
    update_comp = build_update_comparison(results)
    overhead    = build_overhead_breakdown(results)
    tp          = build_throughput_comparison(results)
    dist        = results.get("distribution", {})
    dist_total  = max(sum(dist.values()), 1)

    def _pct(key: str) -> str:
        return f"{round(dist.get(key, 0) / dist_total * 100, 1)}%"

    fw_single  = read_comp["single_record"]["Framework (Logical Query)"]
    sql_single = read_comp["single_record"]["Direct SQL"]
    mg_single  = read_comp["single_record"]["Direct MongoDB"]

    fw_all     = read_comp["all_records"]["Framework (Logical Query)"]
    sql_all    = read_comp["all_records"]["Direct SQL"]
    mg_all     = read_comp["all_records"]["Direct MongoDB"]

    fw_spec    = read_comp["specific_fields"]["Framework (Logical Query)"]
    sql_spec   = read_comp["specific_fields"]["Direct SQL"]
    mg_spec    = read_comp["specific_fields"]["Direct MongoDB"]

    fw_upd     = update_comp["Framework (Coordinated Update)"]
    sql_upd    = update_comp["Direct SQL UPDATE"]
    mg_upd     = update_comp["Direct MongoDB updateOne"]

    fw_total   = overhead["Framework total"]
    dc         = overhead["direct_combined"]
    meta_cost  = overhead["Metadata load"]
    coord_cost = overhead["Coordination logic"]
    oh_pct     = overhead["overhead_pct"]

    meta_load_ms = results.get("metadata_overhead", {}).get("metadata_load_ms", 0)

    throughput_rows = "\n".join(
        f"| {b} | {o} |"
        for b, o in zip(tp["batch_sizes"], tp["ops_per_sec"])
    )

    report = f"""# Comparative Analysis: Hybrid Framework vs Direct Database Access

**Generated:** {timestamp}

---

## 1. Overview

This document compares the performance of the hybrid database framework
against direct SQL (PostgreSQL) and direct MongoDB access across three
dimensions: read queries, update operations, and throughput under load.

### How the Framework Works

Every operation through the framework goes through the following steps:

```
Client request
→ Load metadata_store.json ({meta_load_ms:.2f} ms)
→ Route fields to backends
→ Execute SQL query (if needed)
→ Execute MongoDB query (if needed)
→ Query buffer (if needed)
→ Merge results by primary key
→ Coerce field types
→ Return unified result
```

Direct access skips all of this and talks to one backend only.

---

## 2. Read Query Comparison

### 2a. Single Record Retrieval (by Primary Key)

| Method | Latency (ms) | vs Direct SQL |
|--------|-------------|---------------|
| **Framework (Logical Query)** | `{fw_single:.2f}` | `{_overhead_factor(fw_single, sql_single)}` |
| Direct SQL | `{sql_single:.2f}` | baseline |
| Direct MongoDB | `{mg_single:.2f}` | `{_overhead_factor(mg_single, sql_single)}` |

### 2b. All Records Retrieval

| Method | Latency (ms) | vs Direct SQL |
|--------|-------------|---------------|
| **Framework (Logical Query)** | `{fw_all:.2f}` | `{_overhead_factor(fw_all, sql_all)}` |
| Direct SQL | `{sql_all:.2f}` | baseline |
| Direct MongoDB | `{mg_all:.2f}` | `{_overhead_factor(mg_all, sql_all)}` |

### 2c. Specific Fields Only

| Method | Latency (ms) | vs Direct SQL |
|--------|-------------|---------------|
| **Framework (Logical Query)** | `{fw_spec:.2f}` | `{_overhead_factor(fw_spec, sql_spec)}` |
| Direct SQL | `{sql_spec:.2f}` | baseline |
| Direct MongoDB | `{mg_spec:.2f}` | `{_overhead_factor(mg_spec, sql_spec)}` |

---

## 3. Update Operation Comparison

| Method | Latency (ms) | vs Direct SQL |
|--------|-------------|---------------|
| **Framework (Coordinated Update)** | `{fw_upd:.2f}` | `{_overhead_factor(fw_upd, sql_upd)}` |
| Direct SQL UPDATE | `{sql_upd:.2f}` | baseline |
| Direct MongoDB updateOne | `{mg_upd:.2f}` | `{_overhead_factor(mg_upd, sql_upd)}` |

---

## 4. Transaction Coordination Overhead (Insert)

| Component | Time (ms) | % of Framework Total |
|-----------|-----------|----------------------|
| Direct SQL insert | `{overhead['Direct SQL insert']:.2f}` | `{_pct_of_total(overhead['Direct SQL insert'], fw_total)}` |
| Direct MongoDB insert | `{overhead['Direct MongoDB insert']:.2f}` | `{_pct_of_total(overhead['Direct MongoDB insert'], fw_total)}` |
| Metadata load overhead | `{meta_cost:.2f}` | `{_pct_of_total(meta_cost, fw_total)}` |
| Coordination logic | `{coord_cost:.2f}` | `{_pct_of_total(coord_cost, fw_total)}` |
| **Framework total** | `{fw_total:.2f}` | 100% |
| Overhead over direct | `{overhead['coordination_overhead_ms']:.2f}` | `{oh_pct}%` |

---

## 5. Throughput Under Increasing Workload

### 5a. Read Throughput Curve

| Batch Size | Framework (ops/sec) |
|------------|---------------------|
{throughput_rows}

### 5b. Write Throughput

| Method | Throughput (ops/sec) |
|--------|----------------------|
| Framework (coordinated insert) | `{tp['writes_per_sec']}` |

---

## 6. Data Distribution

| Backend | Fields | Percentage |
|---------|--------|------------|
| SQL | `{dist.get('SQL', 0)}` | `{_pct('SQL')}` |
| MongoDB (Embedded) | `{dist.get('Mongo_Embedded', 0)}` | `{_pct('Mongo_Embedded')}` |
| MongoDB (Reference) | `{dist.get('Mongo_Reference', 0)}` | `{_pct('Mongo_Reference')}` |
| Buffer | `{dist.get('Buffer', 0)}` | `{_pct('Buffer')}` |

---

## 7. Summary

| Operation | Overhead Factor | Primary Cause |
|-----------|----------------|---------------|
| Single record read | `{_overhead_factor(fw_single, sql_single)}` vs SQL | Metadata load + routing + merge |
| All records read | `{_overhead_factor(fw_all, sql_all)}` vs SQL | Merge scales with record count |
| Insert | `{oh_pct}%` over direct | Validation + 2PC + snapshot |
| Update | `{_overhead_factor(fw_upd, sql_upd)}` vs SQL | Delete-then-insert with snapshot |

---

## 8. Conclusion

The hybrid framework introduces measurable latency overhead —
`{oh_pct}%` for inserts and `{_overhead_factor(fw_single, sql_single)}` for single-record reads —
compared to direct database access. This cost covers abstraction,
safety (automatic cross-database rollback), and flexibility
(metadata-driven routing without code changes).

Charts generated:
- `charts/comparative_read_latency.png`
- `charts/comparative_update_latency.png`
- `charts/overhead_breakdown.png`
- `charts/comparative_throughput.png`
"""

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\n[INFO] Comparative analysis saved to {REPORT_FILE}")


# ============================================================================
# Main
# ============================================================================

def run_comparative_analysis():
    print("=" * 70)
    print("COMPARATIVE ANALYSIS — Framework vs Direct Database Access")
    print("=" * 70)

    timestamp = datetime.utcnow().isoformat()

    print("\n[INFO] Loading benchmark results...")
    results = load_results()

    if results.get("status") != "SUCCESS":
        print(f"[WARNING] Benchmark status: {results.get('status')}")
        print("          Results may be incomplete — proceeding anyway.")

    print("\n[INFO] Generating charts...")
    generate_comparative_charts(results)

    print("\n[INFO] Generating comparative report...")
    generate_comparative_report(results, timestamp)

    print("\n" + "=" * 70)
    print("COMPARATIVE ANALYSIS COMPLETE")
    print("=" * 70)
    print("\nFiles written:")
    print(f"  {REPORT_FILE}")
    print(f"  charts/comparative_read_latency.png")
    print(f"  charts/comparative_update_latency.png")
    print(f"  charts/overhead_breakdown.png")
    print(f"  charts/comparative_throughput.png")


if __name__ == "__main__":
    run_comparative_analysis()