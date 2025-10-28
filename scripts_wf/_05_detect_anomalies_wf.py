# 05_detect_anomalies_wf.py
"""
Window-function based anomaly detection.
- Uses DB_CONFIG and PATHS from config.py (no hard-coded credentials).
- Saves all CSV outputs into PATHS['outputs'].
- Exposes run() -> str which returns a human-readable summary.
- Safe connection handling and clear error reporting.
"""

import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]  # project root
if str(root) not in sys.path:
    sys.path.append(str(root))

from typing import Tuple
import os
import mysql.connector
import pandas as pd
from mysql.connector import Error as MySQLError
from config import DB_CONFIG, PATHS

def ensure_outputs():
    """Ensure the configured outputs directory exists."""
    os.makedirs(PATHS.get("outputs", PATHS.get("base", ".")), exist_ok=True)

def output_csv(df: pd.DataFrame, filename: str) -> str:
    """Save dataframe to PATHS['outputs'] with given filename and return full path."""
    ensure_outputs()
    path = os.path.join(PATHS["outputs"], filename)
    df.to_csv(path, index=False)
    return path

def _connect():
    """Return a mysql.connector connection using DB_CONFIG."""
    return mysql.connector.connect(**DB_CONFIG)

def run() -> str:
    """
    Execute window-function anomaly detection queries, save CSVs into outputs, and return a summary.
    """
    ensure_outputs()
    summary_lines = []
    conn = None

    try:
        conn = _connect()
        summary_lines.append("✅ Connected to MariaDB using DB_CONFIG.")
    except Exception as e:
        return f"❌ Failed to connect to MariaDB using DB_CONFIG: {e}\nCheck DB_CONFIG in config.py."

    try:
        # ---------- 1️⃣ Outlier Airports ----------
        query_outlier_airports = """
            WITH route_counts AS (
              SELECT Source_airport_ID, COUNT(*) AS total_routes
              FROM routes
              GROUP BY Source_airport_ID
            ),
            route_stats AS (
              SELECT *,
                     AVG(total_routes) OVER () AS avg_routes,
                     STDDEV(total_routes) OVER () AS std_routes
              FROM route_counts
            )
            SELECT * FROM route_stats
            WHERE total_routes < (avg_routes - 2 * std_routes)
               OR total_routes > (avg_routes + 2 * std_routes);
        """
        df1 = pd.read_sql(query_outlier_airports, conn)
        path1 = output_csv(df1, "outlier_airports.csv")
        summary_lines.append(f"📊 Outlier Airports: {len(df1)} rows -> saved {os.path.basename(path1)}")

        # ---------- 2️⃣ Airline Ranking ----------
        query_airline_rank = """
            SELECT Airline_ID,
                   COUNT(*) AS route_count,
                   RANK() OVER (ORDER BY COUNT(*) DESC) AS airline_rank
            FROM routes
            GROUP BY Airline_ID;
        """
        df2 = pd.read_sql(query_airline_rank, conn)
        path2 = output_csv(df2, "airline_rank.csv")
        summary_lines.append(f"📈 Airline Rankings: {len(df2)} rows -> saved {os.path.basename(path2)}")

        summary_lines.append("")
        summary_lines.append(f"All outputs saved to: {PATHS['outputs']}")
        return "\n".join(summary_lines)

    except MySQLError as db_e:
        return f"❌ MariaDB error during queries: {db_e}"
    except Exception as e:
        return f"❌ Unexpected error during window function analytics: {e}"
    finally:
        try:
            if conn is not None and conn.is_connected():
                conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    print(run())
