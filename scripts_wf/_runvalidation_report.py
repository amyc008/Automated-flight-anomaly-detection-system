
"""
Unified Report Generation + Validation Logger
------------------------------------------------
This script combines the logic of:
 - 06_generate_report.py (creates anomaly summary & charts)
 - 07_automate_validation.py (logs validation history)

Behavior:
- Reads DB credentials and paths from config.py.
- Generates anomaly summary and top-10 airline ranking (from DB or CSV fallback).
- Saves outputs in PATHS['outputs'].
- Automatically appends results with timestamps to validation_history.csv.
- Returns a human-readable summary string for CLI or GUI display.
"""
import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]  # project root
if str(root) not in sys.path:
    sys.path.append(str(root))

import os, sys, traceback, pathlib
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from config import DB_CONFIG, PATHS

# ----------------------------------------------------
# Helpers (same as before)
# ----------------------------------------------------
def ensure_outputs():
    os.makedirs(PATHS.get("outputs", os.path.join(PATHS.get("base", "."), "outputs")), exist_ok=True)

def output_csv(df: pd.DataFrame, filename: str) -> str:
    ensure_outputs()
    path = os.path.join(PATHS["outputs"], filename)
    df.to_csv(path, index=False)
    return path

def output_fig(fig: plt.Figure, filename: str, bbox_inches="tight") -> str:
    ensure_outputs()
    path = os.path.join(PATHS["outputs"], filename)
    fig.savefig(path, bbox_inches=bbox_inches)
    plt.close(fig)
    return path

def get_db_connection():
    import mysql.connector
    return mysql.connector.connect(**DB_CONFIG)

# ----------------------------------------------------
# Core Report Logic (from 06_generate_report.py)
# ----------------------------------------------------
def generate_anomaly_report() -> pd.DataFrame:
    """
    Runs anomaly report generation and returns the summary DataFrame.
    """
    from _06_generate_report import run as generate_report  # if your file is named _06_generate_report.py
    # If renamed to this file itself, you can instead copy logic inline
    print("⚙️ Running unified anomaly report generation...")
    generate_report()  # this prints and writes CSV/Charts
    summary_path = os.path.join(PATHS["outputs"], "anomaly_summary.csv")
    if not os.path.exists(summary_path):
        raise FileNotFoundError(f"Expected anomaly_summary.csv not found at {summary_path}")
    return pd.read_csv(summary_path)

# ----------------------------------------------------
# Validation Logger (from 07_automate_validation.py)
# ----------------------------------------------------
def log_validation(summary_df: pd.DataFrame) -> str:
    """
    Appends the anomaly summary to validation_history.csv with timestamp and run ID.
    Returns summary string.
    """
    ensure_outputs()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # add run metadata
    summary_df = summary_df.copy()
    summary_df["Run_Timestamp"] = timestamp
    summary_df["Run_ID"] = run_id

    history_path = os.path.join(PATHS["outputs"], "validation_history.csv")
    if os.path.exists(history_path):
        df_history = pd.read_csv(history_path)
        df_history = pd.concat([df_history, summary_df], ignore_index=True)
    else:
        df_history = summary_df
    df_history.to_csv(history_path, index=False)

    msg = f"🕒 Validation history updated ({timestamp}) at {history_path}"
    print(msg)
    return msg

# ----------------------------------------------------
# Unified Runner
# ----------------------------------------------------
def run() -> str:
    """
    Generate anomaly report, append to validation history, and return summary string.
    """
    ensure_outputs()
    logs = []
    try:
        logs.append("🚀 Starting unified report + validation cycle...\n")

        # Step 1: Generate report (reuse 06 logic)
        from scripts_wf import _06_generate_report
        result_text = _06_generate_report.run()
        logs.append(result_text)

        # Step 2: Load summary CSV
        summary_path = os.path.join(PATHS["outputs"], "anomaly_summary.csv")
        if not os.path.exists(summary_path):
            raise FileNotFoundError(f"Summary file not found at {summary_path}")
        df_summary = pd.read_csv(summary_path)
        logs.append(f"\n✅ Loaded summary: {len(df_summary)} rows.")

        # Step 3: Append validation log
        msg = log_validation(df_summary)
        logs.append(msg)

        # Step 4: Print summary stats
        logs.append("\n📈 Latest Anomaly Summary:")
        logs.append(df_summary[["Anomaly_Type", "Count"]].to_string(index=False))

        logs.append("\n✅ Unified anomaly validation cycle complete!")
        return "\n".join(logs)

    except Exception as e:
        err = f"❌ Error during unified validation: {e}\n{traceback.format_exc()}"
        print(err)
        return err


if __name__ == "__main__":
    print(run())
