# 03_load_data.py
"""
Load OpenFlights data files into MariaDB tables.

Changes made:
- Uses DB_CONFIG and PATHS from config.py (no hard-coded host/user/password).
- Reads input files from PATHS['raw_csv'] (falls back to PATHS['base'] if not set).
- Ensures PATHS['outputs'] exists and writes a small CSV/text summary of loaded row counts.
- Exposes run() -> str which returns a human-readable summary (useful for CLI or GUI later).
- Keeps existing behavior for LOAD DATA LOCAL INFILE but enables it safely.
"""
import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]  # project root
if str(root) not in sys.path:
    sys.path.append(str(root))


from typing import Dict
import os
import csv
import mysql.connector
from mysql.connector import Error as MySQLError
from config import DB_CONFIG, PATHS

# Helper to ensure outputs dir
def ensure_outputs():
    try:
        os.makedirs(PATHS["outputs"], exist_ok=True)
    except Exception:
        pass

def _input_file_path(basename: str) -> str:
    """
    Return the absolute path to the input data file.
    Looks in PATHS['raw_csv'] and then PATHS['base'] as fallback.
    """
    # support both .dat and .csv suffixes if user has either
    candidates = [
        os.path.join(PATHS.get("raw_csv", PATHS.get("base", ".")), basename + ".dat"),
        os.path.join(PATHS.get("raw_csv", PATHS.get("base", ".")), basename + ".csv"),
        os.path.join(PATHS.get("base", "."), basename + ".dat"),
        os.path.join(PATHS.get("base", "."), basename + ".csv"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return os.path.abspath(p)
    # if none found, return the first candidate (useful so caller sees path even if missing)
    return os.path.abspath(candidates[0])

def run() -> str:
    """
    Load airports, airlines, and routes files into the openflights database.
    Returns a summary string describing what happened and where outputs were written.
    """
    ensure_outputs()

    # Prepare file mapping (logical names -> basenames)
    basenames = {
        "airports": "airports",
        "airlines": "airlines",
        "routes": "routes",
    }
    files: Dict[str, str] = {k: _input_file_path(v) for k, v in basenames.items()}

    # Prepare DB connection config (do not remove database if provided)
    conn = None
    cursor = None
    summary_lines = []

    # We'll try to connect with allow_local_infile enabled.
    try:
        # mysql.connector accepts allow_local_infile as a kwarg to connect()
        conn = mysql.connector.connect(**DB_CONFIG, allow_local_infile=True)
        cursor = conn.cursor()
        summary_lines.append("✅ Connected to MariaDB using config.DB_CONFIG.")
    except Exception as e:
        return f"❌ Failed to connect to MariaDB with DB_CONFIG: {e}\n" \
               f"Check your config.py DB_CONFIG values."

    try:
        # Enable local_infile at server/session level where possible
        try:
            cursor.execute("SET GLOBAL local_infile=1;")
        except Exception:
            # Not fatal; some environments disallow GLOBAL change. Try session-level instead.
            try:
                cursor.execute("SET SESSION local_infile=1;")
            except Exception:
                # If both fail, we'll still attempt to run LOAD DATA and let server error explain itself.
                pass

        # LOAD DATA statements (use forward slashes for path)
        # We explicitly replace backslashes so MySQL accepts the path on Windows.
        for key in ["airports", "airlines", "routes"]:
            path = files[key]
            path_for_sql = path.replace("\\", "/")
            if not os.path.exists(path):
                summary_lines.append(f"⚠️ Input file for '{key}' not found: {path}")
                # skip loading this file
                continue

            if key == "airports":
                sql = f"""
                LOAD DATA LOCAL INFILE '{path_for_sql}'
                INTO TABLE airports
                FIELDS TERMINATED BY ',' 
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                (Airport_ID, Name, City, Country, IATA, ICAO, Latitude, Longitude, Altitude, Timezone, DST, Tz_database_time_zone, Type, Source);
                """
            elif key == "airlines":
                sql = f"""
                LOAD DATA LOCAL INFILE '{path_for_sql}'
                INTO TABLE airlines
                FIELDS TERMINATED BY ',' 
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                (Airline_ID, Name, Alias, IATA, ICAO, Callsign, Country, Active);
                """
            elif key == "routes":
                sql = f"""
                LOAD DATA LOCAL INFILE '{path_for_sql}'
                INTO TABLE routes
                FIELDS TERMINATED BY ',' 
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                (Airline, Airline_ID, Source_airport, Source_airport_ID, Destination_airport, Destination_airport_ID, Codeshare, Stops, Equipment);
                """
            else:
                continue

            try:
                cursor.execute(sql)
                summary_lines.append(f"📥 Loaded '{key}' from: {path}")
            except MySQLError as le:
                summary_lines.append(f"❌ Failed to LOAD DATA for '{key}': {le}")
            except Exception as e:
                summary_lines.append(f"❌ Unexpected error while loading '{key}': {e}")

        # Commit after attempted loads
        try:
            conn.commit()
        except Exception:
            # if commit fails, still try to report counts
            pass

        # Verify counts and save to outputs as CSV
        counts = []
        for table in ["airports", "airlines", "routes"]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table};")
                count = cursor.fetchone()[0]
                counts.append({"table": table, "rows": int(count)})
                summary_lines.append(f"📊 {table}: {count} rows loaded.")
            except Exception as e:
                counts.append({"table": table, "rows": None})
                summary_lines.append(f"⚠️ Could not fetch count for {table}: {e}")

        # Save counts CSV to outputs
        import pandas as pd
        df_counts = pd.DataFrame(counts)
        counts_csv = os.path.join(PATHS["outputs"], "load_counts.csv")
        try:
            df_counts.to_csv(counts_csv, index=False)
            summary_lines.append(f"✅ Wrote load counts CSV to: {counts_csv}")
        except Exception as e:
            summary_lines.append(f"⚠️ Failed to write load counts CSV: {e}")

    except Exception as e:
        summary_lines.append(f"❌ General error during load: {e}")

    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None and conn.is_connected():
                conn.close()
        except Exception:
            pass

    # Final human readable summary
    summary = "\n".join(summary_lines)
    # Also print for CLI convenience
    print(summary)
    return summary


if __name__ == "__main__":
    run()
