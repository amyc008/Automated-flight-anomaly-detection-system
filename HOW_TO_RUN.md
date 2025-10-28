🧭 HOW_TO_RUN.md (Final Safe Version)

# HOW TO RUN — MariaDB Flight Anomaly Detector

_A short, step-by-step guide to set up and run the project easily._

---

## 🧩 What this project does

This tool checks flight data for **mistakes or unusual patterns** using **MariaDB** and **Python**.

It:

- Detects missing or duplicate routes,
- Finds airlines with unusually high/low route counts,
- Generates reports and charts in the `outputs/` folder.

---

## ⚙️ Step 1. Install Required Software

Before running anything, install:

- **Python 3.9+**
- **MariaDB Server 10.5+**
- **OpenFlights dataset** (`airports.dat`, `airlines.dat`, `routes.dat`)

---

## 🗃️ Step 2. Set up MariaDB manually

We will set up the database manually before connecting Python.

1. **Open MariaDB (CLI or HeidiSQL)**
   ```sql
   mariadb -u root -p
   ```

Create a user and database

CREATE USER 'your_username'@'%' IDENTIFIED BY 'your_password';
CREATE DATABASE openflights;
GRANT ALL PRIVILEGES ON openflights.\* TO 'your_username'@'%';
FLUSH PRIVILEGES;

✅ This creates:

User: your_username

Database: openflights

Password: your_password

(You can change these in config.py later.)

🧱 Step 3. Prepare configuration file

Copy the template:

cp config.py.template config.py

Edit config.py and update your database details:

DB_CONFIG = {
"host": "127.0.0.1",
"user": "your_username",
"password": "your_password",
"database": "openflights",
"port": 3306
}

Ensure your OpenFlights data files (airports.dat, airlines.dat, routes.dat) are in the openflights/data folder.

🧰 Step 4. Set up Python environment

From your project root folder:

python -m venv venv

# Windows:

venv\Scripts\activate

# Linux/Mac:

source venv/bin/activate

pip install -r requirements.txt

✅ This installs all required Python libraries (pandas, matplotlib, mysql-connector-python, etc.)

🏗️ Step 5. Create Tables in MariaDB

You already created the openflights database manually.
Now let’s create the tables inside it.

Run:

python scripts_wf/02_create_tables_wf.py

This will:

Connect to MariaDB using your credentials,

Create tables: airports, airlines, and routes.

🛫 Step 6. Load Data into MariaDB

Load the OpenFlights data into the tables.

Run:

python scripts_wf/\_03_load_data.py

This uses LOAD DATA LOCAL INFILE to import data into MariaDB.

Output:

A file named load_counts.csv will be created inside outputs/
showing how many rows were loaded into each table.

🔍 Step 7. Run Anomaly Detection Scripts
1️⃣ Basic anomalies (missing or duplicate routes)
python scripts_wf/\_04_detect_anomalies_wf.py

Generates:

missing_source_airports.csv

missing_destination_airports.csv

missing_airlines.csv

duplicate_routes.csv

incomplete_routes.csv

2️⃣ Window-function anomalies (outliers and airline ranking)
python scripts_wf/\_05_detect_anomalies_wf.py

Uses MariaDB’s Window Functions (RANK(), AVG(), STDDEV())
to detect outlier airports and rank airlines by route count.

Generates:

outlier_airports.csv

airline_rank.csv

📊 Step 8. Generate & Validate Reports

Run the combined report generator:

python scripts_wf/\_runvalidation_report.py

This script:

Runs both analyses together,

Creates summary charts (.png files),

Updates validation_history.csv with timestamps.

All reports are saved inside outputs/.

🖥️ Step 9. (Optional) Run the GUI

To use a simple graphical interface:

python app_main.py

You’ll see four buttons:

Load Data

Detect Basic Anomalies

Detect WF Anomalies

Generate & Validate Reports

Each button runs the respective script and displays logs on screen.

📁 Step 10. Check your results

Open the outputs/ folder.
You’ll find:

CSV files (missing, outliers, rankings)

PNG charts (if report generator ran)

validation_history.csv — keeps log of all previous runs

✅ You’re done!

You now have a fully working MariaDB-based flight anomaly detector.

The core innovation:

It uses MariaDB’s SQL Window Functions for outlier detection — a feature normally seen in enterprise analytics — combined with Python for automation and reporting.
