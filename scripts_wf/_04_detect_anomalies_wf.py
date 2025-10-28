# 04_detect_anomalies_wf.py
"""
Workflow for basic anomaly detection in the routes/airlines/airports tables.

Requirements satisfied:
- Reads DB credentials from config.DB_CONFIG
- Writes all CSV outputs into PATHS['outputs']
- Exposes run() -> str (human-readable summary) and keeps CLI runnable
- Closes DB connection safely and reports any errors in the summary
"""
import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]  # project root
if str(root) not in sys.path:
    sys.path.append(str(root))


from typing import Tuple
import os
import mysql.connector
import pandas as pd
from config import DB_CONFIG, PATHS

# --- Helpers for config-driven I/O ---
def ensure_outputs():
    os.makedirs(PATHS["outputs"], exist_ok=True)

def output_csv(df: pd.DataFrame, filename: str) -> str:
    ensure_outputs()
    path = os.path.join(PATHS["outputs"], filename)
    df.to_csv(path, index=False)
    return path

def _connect():
    """Return a mysql.connector connection using DB_CONFIG."""
    return mysql.connector.connect(**DB_CONFIG)

def _safe_read_sql(query: str, conn) -> Tuple[pd.DataFrame, str]:
    """
    Execute query returning (df, error_msg). If success, error_msg is empty string.
    """
    try:
        df = pd.read_sql(query, conn)
        return df, ""
    except Exception as e:
        # return empty dataframe and the error message so caller can continue
        return pd.DataFrame(), str(e)

# --- Core workflow ---
def run() -> str:
    """
    Run basic anomaly detection queries, save CSVs into PATHS['outputs'] and return a summary string.
    """
    ensure_outputs()
    summary_lines = []
    conn = None

    queries = {
        "missing_source_airports": (
            """
            SELECT r.*
            FROM routes r
            LEFT JOIN airports a ON r.Source_airport_ID = a.Airport_ID
            WHERE a.Airport_ID IS NULL;
            """,
            "missing_source_airports.csv"
        ),
        "missing_destination_airports": (
            """
            SELECT r.*
            FROM routes r
            LEFT JOIN airports a ON r.Destination_airport_ID = a.Airport_ID
            WHERE a.Airport_ID IS NULL;
            """,
            "missing_destination_airports.csv"
        ),
        "missing_airlines": (
            """
            SELECT r.*
            FROM routes r
            LEFT JOIN airlines al ON r.Airline_ID = al.Airline_ID
            WHERE al.Airline_ID IS NULL;
            """,
            "missing_airlines.csv"
        ),
        "duplicate_routes": (
            """
            SELECT Airline_ID, Source_airport_ID, Destination_airport_ID, COUNT(*) AS dup_count
            FROM routes
            GROUP BY Airline_ID, Source_airport_ID, Destination_airport_ID
            HAVING dup_count > 1;
            """,
            "duplicate_routes.csv"
        ),
        "incomplete_routes": (
            """
            SELECT *
            FROM routes
            WHERE Airline_ID IS NULL
               OR Source_airport_ID IS NULL
               OR Destination_airport_ID IS NULL;
            """,
            "incomplete_routes.csv"
        ),
    }

    try:
        conn = _connect()
        summary_lines.append("✅ Connected to MariaDB using DB_CONFIG.")
    except Exception as e:
        return f"❌ Failed to connect to MariaDB using DB_CONFIG: {e}\n" \
               f"Check DB_CONFIG in config.py and MariaDB server availability."

    try:
        for key, (sql, filename) in queries.items():
            df, err = _safe_read_sql(sql, conn)
            if err:
                summary_lines.append(f"❌ {key}: query failed -> {err}")
                # still create an empty CSV to indicate failure, with an error note
                err_df = pd.DataFrame([{"error": err}])
                try:
                    path = output_csv(err_df, f"{key}_error.csv")
                    summary_lines.append(f"   ℹ️ Wrote error info to: {path}")
                except Exception as ex_write:
                    summary_lines.append(f"   ⚠️ Failed to write error CSV for {key}: {ex_write}")
                continue

            # Save results
            try:
                path = output_csv(df, filename)
                summary_lines.append(f"✅ {key}: {len(df)} rows -> saved {os.path.basename(path)}")
            except Exception as e_write:
                summary_lines.append(f"❌ {key}: failed to save CSV -> {e_write}")

        summary_lines.append("")
        summary_lines.append(f"All outputs were saved to: {PATHS['outputs']}")
        return "\n".join(summary_lines)

    except Exception as e:
        return f"❌ Unexpected error during anomaly detection workflow: {e}"

    finally:
        try:
            if conn is not None and conn.is_connected():
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    print(run())
