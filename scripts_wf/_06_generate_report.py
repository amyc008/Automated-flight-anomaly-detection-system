# 06_generate_report.py
"""
Generate anomaly report (CSV + charts) — config-driven.

Behavior:
- Reads DB credentials & paths from config.DB_CONFIG and config.PATHS.
- Saves all outputs to PATHS['outputs'].
- Attempts to run SQL queries against MariaDB; if DB is unavailable,
  falls back to computing results from CSV files in PATHS['raw_csv'].
- Exposes run() -> str (summary) and remains runnable from CLI.
"""

import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]  # project root
if str(root) not in sys.path:
    sys.path.append(str(root))

from typing import Dict, Tuple, Optional
import os
import sys
import traceback
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from config import DB_CONFIG, PATHS

# ---- Helpers ----
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
    """Return a mysql.connector connection using DB_CONFIG. Caller handles exceptions."""
    import mysql.connector
    return mysql.connector.connect(**DB_CONFIG)

def find_input_file(basename: str) -> Optional[str]:
    """
    Look for basename.csv or basename.dat inside PATHS['raw_csv'] and PATHS['base'].
    Returns absolute path or None.
    """
    candidates = []
    base_dirs = []
    if PATHS.get("raw_csv"):
        base_dirs.append(PATHS["raw_csv"])
    if PATHS.get("base"):
        base_dirs.append(PATHS["base"])
    base_dirs.append(os.getcwd())

    for d in base_dirs:
        candidates.append(os.path.join(d, basename + ".csv"))
        candidates.append(os.path.join(d, basename + ".dat"))
    for p in candidates:
        if os.path.exists(p):
            return os.path.abspath(p)
    return None

# ---- CSV fallback loaders (used when DB not available) ----
def load_table_from_csv_guess(table_name: str) -> pd.DataFrame:
    """
    Load airports/airlines/routes from CSV/DAT in PATHS['raw_csv'] or PATHS['base'].
    Raises FileNotFoundError if not found.
    """
    p = find_input_file(table_name)
    if not p:
        raise FileNotFoundError(f"No input file found for '{table_name}' (tried .csv/.dat in raw_csv/base).")
    # routes.dat from OpenFlights uses comma-separated values, possibly with quotes
    df = pd.read_csv(p, header=None, dtype=str, na_values=["\\N", ""], keep_default_na=False)
    # Try to assign column names if known (best-effort)
    if table_name == "airports":
        cols = ["Airport_ID","Name","City","Country","IATA","ICAO","Latitude","Longitude","Altitude","Timezone","DST","Tz_database_time_zone","Type","Source"]
    elif table_name == "airlines":
        cols = ["Airline_ID","Name","Alias","IATA","ICAO","Callsign","Country","Active"]
    elif table_name == "routes":
        cols = ["Airline","Airline_ID","Source_airport","Source_airport_ID","Destination_airport","Destination_airport_ID","Codeshare","Stops","Equipment"]
    else:
        cols = None

    if cols and df.shape[1] >= len(cols):
        df = df.iloc[:, : len(cols)]
        df.columns = cols
    return df

# ---- Domain logic for fallback computations ----
def compute_basic_counts_from_csv() -> Dict[str, int]:
    """
    Compute the same counts as queries_basic from CSV data.
    Returns dictionary with counts for each anomaly type.
    """
    # Load routes, airports, airlines (best-effort)
    routes = load_table_from_csv_guess("routes")
    airports = load_table_from_csv_guess("airports")
    airlines = load_table_from_csv_guess("airlines")

    # normalize ID columns to numeric where possible
    routes["Source_airport_ID"] = pd.to_numeric(routes.get("Source_airport_ID"), errors="coerce")
    routes["Destination_airport_ID"] = pd.to_numeric(routes.get("Destination_airport_ID"), errors="coerce")
    routes["Airline_ID"] = pd.to_numeric(routes.get("Airline_ID"), errors="coerce")
    airports["Airport_ID"] = pd.to_numeric(airports.get("Airport_ID"), errors="coerce")
    airlines["Airline_ID"] = pd.to_numeric(airlines.get("Airline_ID"), errors="coerce")

    # Missing source airports
    missing_source = routes[~routes["Source_airport_ID"].isin(airports["Airport_ID"].dropna())]
    missing_source_count = int(missing_source.shape[0])

    # Missing destination airports
    missing_dest = routes[~routes["Destination_airport_ID"].isin(airports["Airport_ID"].dropna())]
    missing_dest_count = int(missing_dest.shape[0])

    # Missing airlines
    missing_airlines = routes[~routes["Airline_ID"].isin(airlines["Airline_ID"].dropna())]
    missing_airlines_count = int(missing_airlines.shape[0])

    # Duplicate routes (Airline_ID, Source_airport_ID, Destination_airport_ID)
    dup = routes.groupby(["Airline_ID","Source_airport_ID","Destination_airport_ID"]).size().reset_index(name="dup_count")
    duplicate_routes_count = int(dup[dup["dup_count"] > 1].shape[0])

    # Incomplete routes (nulls)
    incomplete_routes_count = int(routes[
        routes["Airline_ID"].isna() |
        routes["Source_airport_ID"].isna() |
        routes["Destination_airport_ID"].isna()
    ].shape[0])

    return {
        "missing_source_airports": missing_source_count,
        "missing_destination_airports": missing_dest_count,
        "missing_airlines": missing_airlines_count,
        "duplicate_routes": duplicate_routes_count,
        "incomplete_routes": incomplete_routes_count
    }

def compute_window_analytics_from_csv() -> Tuple[int, pd.DataFrame]:
    """
    Compute outlier airports count and top-10 airline ranking from CSVs as fallback.
    Returns (outlier_count, df_rank_top10)
    """
    routes = load_table_from_csv_guess("routes")
    # ensure numeric
    routes["Source_airport_ID"] = pd.to_numeric(routes.get("Source_airport_ID"), errors="coerce")
    routes["Airline_ID"] = pd.to_numeric(routes.get("Airline_ID"), errors="coerce")

    # route counts per source airport
    rc = routes.groupby("Source_airport_ID").size().reset_index(name="total_routes")
    avg = rc["total_routes"].mean()
    std = rc["total_routes"].std(ddof=0)  # population std like SQL STDDEV
    lower = avg - 2 * std
    upper = avg + 2 * std
    outliers = rc[(rc["total_routes"] < lower) | (rc["total_routes"] > upper)]
    outlier_count = int(outliers.shape[0])

    # airline ranking top 10
    ar = routes.groupby("Airline_ID").size().reset_index(name="route_count").sort_values("route_count", ascending=False)
    ar["airline_rank"] = ar["route_count"].rank(method="dense", ascending=False).astype(int)
    df_rank_top10 = ar.head(10).copy()
    df_rank_top10["Airline_ID"] = df_rank_top10["Airline_ID"].astype(str)

    return outlier_count, df_rank_top10

# ---- Main run function ----
def run() -> str:
    """
    Run unified report generation. Returns a human-readable summary string.
    """
    ensure_outputs()
    outputs_written = []
    summary = {}
    logs = []
    used_db = False

    # Query definitions (we'll use cursor for simple counts and pandas.read_sql for resultsets)
    queries_basic = {
        "missing_source_airports": """
            SELECT COUNT(*) FROM routes r
            LEFT JOIN airports a ON r.Source_airport_ID = a.Airport_ID
            WHERE a.Airport_ID IS NULL;
        """,
        "missing_destination_airports": """
            SELECT COUNT(*) FROM routes r
            LEFT JOIN airports a ON r.Destination_airport_ID = a.Airport_ID
            WHERE a.Airport_ID IS NULL;
        """,
        "missing_airlines": """
            SELECT COUNT(*) FROM routes r
            LEFT JOIN airlines al ON r.Airline_ID = al.Airline_ID
            WHERE al.Airline_ID IS NULL;
        """,
        "duplicate_routes": """
            SELECT COUNT(*) FROM (
                SELECT Airline_ID, Source_airport_ID, Destination_airport_ID
                FROM routes
                GROUP BY Airline_ID, Source_airport_ID, Destination_airport_ID
                HAVING COUNT(*) > 1
            ) AS dup;
        """,
        "incomplete_routes": """
            SELECT COUNT(*) FROM routes
            WHERE Airline_ID IS NULL
               OR Source_airport_ID IS NULL
               OR Destination_airport_ID IS NULL;
        """
    }

    # Window analytics queries (we will try DB first)
    query_outliers = """
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
        SELECT COUNT(*) AS outlier_count
        FROM route_stats
        WHERE total_routes < (avg_routes - 2 * std_routes)
           OR total_routes > (avg_routes + 2 * std_routes);
    """

    query_airline_rank = """
        SELECT Airline_ID,
               COUNT(*) AS route_count,
               RANK() OVER (ORDER BY COUNT(*) DESC) AS airline_rank
        FROM routes
        GROUP BY Airline_ID
        ORDER BY route_count DESC
        LIMIT 10;
    """

    conn = None
    try:
        # Try DB connection
        try:
            conn = get_db_connection()
            used_db = True
            logs.append("Connected to MariaDB (using config.DB_CONFIG).")
        except Exception as e_conn:
            logs.append(f"Could not connect to MariaDB, will attempt CSV fallbacks: {e_conn}")
            used_db = False

        if used_db and conn is not None:
            # Run basic count queries via cursor
            try:
                cur = conn.cursor()
                for name, q in queries_basic.items():
                    cur.execute(q)
                    row = cur.fetchone()
                    summary[name] = int(row[0]) if row and row[0] is not None else 0
                    logs.append(f"Query {name}: {summary[name]} (DB)")
                cur.close()
            except Exception as e_q:
                logs.append(f"Error executing basic queries on DB: {e_q}")
                # fallback to CSV computation if DB failed mid-way
                used_db = False

        if not used_db:
            # CSV fallbacks
            try:
                csv_counts = compute_basic_counts_from_csv()
                summary.update(csv_counts)
                for k, v in csv_counts.items():
                    logs.append(f"Computed {k}: {v} (CSV fallback)")
            except Exception as e_csv:
                logs.append(f"CSV fallback for basic counts failed: {e_csv}\n{traceback.format_exc()}")

        # Window analytics: outlier count and top10 ranking
        df_rank = None
        outlier_count = None
        if used_db and conn is not None:
            try:
                df_out = pd.read_sql(query_outliers, conn)
                outlier_count = int(df_out.iloc[0, 0]) if not df_out.empty else 0
                logs.append(f"Outlier airports (DB): {outlier_count}")
            except Exception as e_w:
                logs.append(f"DB outlier query failed: {e_w}")
                used_db = False

            if used_db:
                try:
                    df_rank = pd.read_sql(query_airline_rank, conn)
                    logs.append(f"Airline rank fetched from DB (rows: {len(df_rank)})")
                except Exception as e_rank:
                    logs.append(f"DB airline rank query failed: {e_rank}")
                    df_rank = None
                    used_db = False

        if not used_db:
            try:
                outlier_count, df_rank = compute_window_analytics_from_csv()
                logs.append(f"Computed outlier_count and airline rank from CSVs (outliers={outlier_count}, rank_rows={len(df_rank)})")
            except Exception as e_csv2:
                logs.append(f"CSV fallback for window analytics failed: {e_csv2}\n{traceback.format_exc()}")

        summary["outlier_airports"] = int(outlier_count) if outlier_count is not None else 0

        # Save top 10 airline ranking if available
        if df_rank is not None and not df_rank.empty:
            # normalize column names to expected ones if needed
            df_rank = df_rank.rename(columns={df_rank.columns[0]: "Airline_ID", df_rank.columns[1]: "route_count"})
            top10_csv = output_csv(df_rank, "top_10_airlines.csv")
            outputs_written.append(top10_csv)
            logs.append(f"Saved top 10 airlines to {top10_csv}")

        # Save summary CSV
        summary_items = [(k, int(v) if v is not None else 0) for k, v in summary.items()]
        summary_df = pd.DataFrame(summary_items, columns=["Anomaly_Type", "Count"])
        summary_csv = output_csv(summary_df, "anomaly_summary.csv")
        outputs_written.append(summary_csv)
        logs.append(f"Saved anomaly summary to {summary_csv}")

        # Create summary chart
        try:
            fig, ax = plt.subplots(figsize=(9, 6))
            # horizontal bar chart sorted
            summary_df_sorted = summary_df.sort_values("Count", ascending=True)
            ax.barh(summary_df_sorted["Anomaly_Type"], summary_df_sorted["Count"])
            ax.set_xlabel("Count")
            ax.set_title("Flight Data Anomalies Summary")
            fig.tight_layout()
            summary_chart = output_fig(fig, "anomaly_summary_chart.png")
            outputs_written.append(summary_chart)
            logs.append(f"Saved anomaly summary chart to {summary_chart}")
        except Exception as e_chart:
            logs.append(f"Failed to create summary chart: {e_chart}")

        # Create top 10 chart if df_rank exists
        try:
            if df_rank is not None and not df_rank.empty:
                fig2, ax2 = plt.subplots(figsize=(9, 5))
                # ensure columns exist
                x = df_rank["Airline_ID"].astype(str)
                y = pd.to_numeric(df_rank["route_count"], errors="coerce").fillna(0)
                ax2.bar(x, y)
                ax2.set_xlabel("Airline_ID")
                ax2.set_ylabel("Route Count")
                ax2.set_title("Top 10 Airlines by Route Count")
                fig2.tight_layout()
                top10_chart = output_fig(fig2, "top_10_airlines_chart.png")
                outputs_written.append(top10_chart)
                logs.append(f"Saved top-10 chart to {top10_chart}")
        except Exception as e_topchart:
            logs.append(f"Failed to create top10 chart: {e_topchart}")

    except Exception as e:
        logs.append(f"Unexpected failure in run(): {e}\n{traceback.format_exc()}")
    finally:
        # cleanup DB connection
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

    # Build summary string
    ts = datetime.utcnow().isoformat() + "Z"
    header = [f"Report run at (UTC): {ts}", f"Outputs directory: {PATHS.get('outputs')}"]
    body = logs + [""] + [f"Saved files:"] + [f"  - {p}" for p in outputs_written]
    footer = ["", "Summary counts:"] + [f"  - {k}: {v}" for k, v in summary.items()]
    result_lines = header + [""] + body + [""] + footer
    result = "\n".join(result_lines)
    # also print to stdout for CLI convenience
    print(result)
    return result


if __name__ == "__main__":
    run()
