import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]  # project root
if str(root) not in sys.path:
    sys.path.append(str(root))


import subprocess
import pandas as pd
from datetime import datetime
import os
from config import DB_CONFIG, PATHS




def run_validation():
    print("🚀 Starting automated anomaly validation...")

    # Step 1: Run the unified report generator
    try:
        subprocess.run(["python", "scripts_wf/_06_generate_report.py"], check=True)
        print("✅ Report generation completed successfully.")
    except subprocess.CalledProcessError:
        print("❌ Report generation failed. Check logs for details.")
        return

    # Step 2: Load summary output
    summary_file = os.path.join("outputs", "anomaly_summary.csv")
    if not os.path.exists(summary_file):
        print("⚠️ Summary file not found. Skipping log update.")
        return

    df_summary = pd.read_csv(summary_file)

    # Step 3: Add timestamp and run ID
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_summary["Run_Timestamp"] = timestamp

    # Step 4: Append to historical log
    history_file = os.path.join("outputs", "validation_history.csv")
    if os.path.exists(history_file):
        df_history = pd.read_csv(history_file)
        df_history = pd.concat([df_history, df_summary], ignore_index=True)
    else:
        df_history = df_summary

    df_history.to_csv(history_file, index=False)
    print(f"🕒 Validation history updated at {timestamp}")

    # Step 5: Print summary stats
    print("\n📈 Latest Anomaly Summary:")
    print(df_summary[["Anomaly_Type", "Count"]].to_string(index=False))

    print("\n✅ Automated anomaly validation cycle complete!")

if __name__ == "__main__":
    run_validation()
