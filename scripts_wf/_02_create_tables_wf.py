# create_schema.py
"""
Create the OpenFlights schema (airports, airlines, routes) using DB settings from config.py.

Changes made:
- Reads DB credentials (host, port, user, password, optional database) from config.DB_CONFIG.
- Connects without a preselected database (so CREATE DATABASE / USE work reliably).
- Exposes run() which returns a human-readable summary string (suitable for printing or GUI display).
- Safely closes cursor/connection even on error.
- Ensures PATHS['outputs'] exists (keeps project-wide consistency; no files are written by this script).
"""

import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]  # project root
if str(root) not in sys.path:
    sys.path.append(str(root))

from typing import List
import mysql.connector
from mysql.connector import Error as MySQLError
from config import DB_CONFIG, PATHS
import os
import copy

DDL_STATEMENTS: List[str] = [
    """
    CREATE DATABASE IF NOT EXISTS openflights;
    """,
    "USE openflights;",
    """
    CREATE TABLE IF NOT EXISTS airports (
      Airport_ID INT PRIMARY KEY,
      Name VARCHAR(150),
      City VARCHAR(100),
      Country VARCHAR(100),
      IATA CHAR(3),
      ICAO CHAR(4),
      Latitude DECIMAL(10,6),
      Longitude DECIMAL(10,6),
      Altitude INT,
      Timezone DECIMAL(4,1),
      DST CHAR(1),
      Tz_database_time_zone VARCHAR(50),
      Type VARCHAR(50),
      Source VARCHAR(100)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS airlines (
      Airline_ID INT PRIMARY KEY,
      Name VARCHAR(100),
      Alias VARCHAR(100),
      IATA VARCHAR(10),
      ICAO VARCHAR(10),
      Callsign VARCHAR(100),
      Country VARCHAR(100),
      Active CHAR(1)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS routes (
      Route_ID INT AUTO_INCREMENT PRIMARY KEY,
      Airline VARCHAR(10),
      Airline_ID INT,
      Source_airport VARCHAR(10),
      Source_airport_ID INT,
      Destination_airport VARCHAR(10),
      Destination_airport_ID INT,
      Codeshare CHAR(1),
      Stops INT,
      Equipment VARCHAR(100),
      FOREIGN KEY (Airline_ID) REFERENCES airlines(Airline_ID),
      FOREIGN KEY (Source_airport_ID) REFERENCES airports(Airport_ID),
      FOREIGN KEY (Destination_airport_ID) REFERENCES airports(Airport_ID)
    ) ENGINE=InnoDB;
    """
]


def ensure_outputs_dir():
    """Create outputs directory (keeps consistency across project even if this script doesn't write files)."""
    try:
        os.makedirs(PATHS["outputs"], exist_ok=True)
    except Exception:
        # Non-fatal — schema creation shouldn't fail because of outputs dir
        pass


def run() -> str:
    """
    Execute the DDL statements against the MariaDB server using credentials from config.DB_CONFIG.
    Returns a short human-readable summary.
    """
    ensure_outputs_dir()

    # Connect without a selected database so CREATE DATABASE / USE can run reliably.
    conn = None
    cursor = None
    executed = 0

    connect_cfg = copy.deepcopy(DB_CONFIG) if DB_CONFIG else {}
    # remove database if present to allow CREATE DATABASE/USE
    if "database" in connect_cfg:
        connect_cfg.pop("database")

    try:
        conn = mysql.connector.connect(**connect_cfg)
        cursor = conn.cursor()
        for ddl in DDL_STATEMENTS:
            sql = ddl.strip()
            if not sql:
                continue
            cursor.execute(sql)
            executed += 1
        conn.commit()

        host = DB_CONFIG.get("host", "<unknown>")
        port = DB_CONFIG.get("port", "<default>")
        user = DB_CONFIG.get("user", "<unknown>")

        summary = (
            f"✅ Schema created/verified successfully.\n"
            f"Executed {executed} DDL statements.\n"
            f"Connection (from config): host={host} port={port} user={user}\n"
            f"All outputs (if any) will be saved to: {PATHS.get('outputs')}\n"
        )
        return summary

    except MySQLError as db_err:
        return f"❌ MariaDB error while creating schema: {db_err}"
    except Exception as e:
        return f"❌ Unexpected error while creating schema: {e}"
    finally:
        # safe cleanup
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


if __name__ == "__main__":
    print(run())
