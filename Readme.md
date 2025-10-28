# ✈️ MariaDB Flight Anomaly Detector

A Python + MariaDB project that automatically finds errors and unusual patterns in flight data.

---

## 🌟 What this project does — in simple terms

Airlines and travel systems store huge amounts of data: airports, airlines, and routes.  
This project checks that data for **mistakes and anomalies** — such as:

- Missing or invalid airports / airlines,
- Duplicate or incomplete flight routes,
- Airlines with an unusually high or low number of routes.

It uses **MariaDB’s analytical SQL capabilities (Window Functions)** to find these outliers efficiently — a powerful feature usually used in enterprise-grade analytics.

---

## 🧠 Why this project is special

✅ **Built entirely on MariaDB features — not just Python logic**

- **MariaDB Window Functions:**  
  The project uses SQL window functions like `RANK()` and statistical functions like `AVG()` and `STDDEV()` inside queries.  
  These help detect _outlier airports_ and _rank airlines_ by route count — showcasing MariaDB’s advanced analytics ability (similar to what you’d use in BI systems).

- **Foreign Keys & Data Integrity Checks (InnoDB):**  
  Schema design uses **foreign key relationships** between airports, airlines, and routes to ensure referential integrity — a MariaDB relational feature.

- **LOAD DATA LOCAL INFILE:**  
  Used for **fast bulk loading** of OpenFlights datasets into MariaDB — leveraging MariaDB’s optimized import command.

✅ **Fully config-driven:**  
All DB credentials and folder paths are managed in one place (`config.py`), making it easy to port to any environment.

✅ **Educational and Reusable:**  
Every script is documented and returns clear, human-readable summaries.

---

## ⚙️ Architecture at a glance

| OpenFlights Dataset |
+----------+------------+
|
v
+-----------------------+
| Load Data (03) | --> LOAD DATA INFILE --> MariaDB Tables
+-----------------------+
|
v
+-----------------------+
| Detect Basic Issues | --> Missing / Duplicate / Incomplete Routes
+-----------------------+
|
v
+-----------------------+
| Detect WF Anomalies | --> Outlier Airports / Airline Ranking
| (uses Window Functions) |
+-----------------------+
|
v
+-----------------------+
| Generate & Validate Reports |
| --> CSVs, Charts, Logs |
+-----------------------+

---

## 🧰 Key Components

| Script                          | Purpose                                                                                                       |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| **\_03_load_data.py**           | Loads OpenFlights data into MariaDB using `LOAD DATA LOCAL INFILE`.                                           |
| **\_04_detect_anomalies_wf.py** | Detects missing or duplicate routes (basic anomaly detection).                                                |
| **\_05_detect_anomalies_wf.py** | Detects _outliers_ and _airline rankings_ using **MariaDB Window Functions** (`RANK()`, `AVG()`, `STDDEV()`). |
| **\_runvalidation_report.py**   | Combines report generation and validation logging.                                                            |
| **app_main.py**                 | Simple Tkinter GUI to trigger all modules easily.                                                             |

---

## 🗂️ Project Folder Layout

/
├── scripts_wf/
│ ├── \_03_load_data.py
│ ├── \_04_detect_anomalies_wf.py
│ ├── \_05_detect_anomalies_wf.py
│ ├── \_06_generate_report.py
│ ├── \_runvalidation_report.py
│ └── create_schema.py
├── app_main.py
├── config.py.template
├── README.md
├── requirements.txt
├── .gitignore
├── LICENSE
└── outputs/ (auto-created)

---

## 💡 About MariaDB Feature Usage

| Feature                                              | Description                                                                                                      | How it’s used                                                                                     |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| **Window Functions (`RANK()`, `AVG()`, `STDDEV()`)** | Powerful analytical SQL feature introduced in MariaDB 10.2+ that allows row-by-row analytics without subqueries. | Used to rank airlines by route count and detect airports with unusually high or low route volume. |
| **LOAD DATA LOCAL INFILE**                           | MariaDB’s bulk import mechanism for CSV-like files.                                                              | Used in `_03_load_data.py` to import OpenFlights datasets efficiently.                            |
| **Foreign Keys / Constraints**                       | Core relational feature to maintain referential integrity.                                                       | Used in schema to ensure that routes reference valid airports and airlines.                       |
| **InnoDB Engine**                                    | Reliable transactional storage engine with indexing and foreign key support.                                     | Used for all tables. (ColumnStore was evaluated but not used due to connectivity issues.)         |

---

## 🧪 How to Run(For a step by step execution please refer to HOW_TO_RUN.md)

### 1️⃣ Prerequisites

- Python 3.9 or later
- MariaDB (v10.5 or newer)
- Datasets from [OpenFlights.org](https://openflights.org/data.html)
- VScode

### 2️⃣ Installation

```bash
git clone https://github.com/amyc008/Automated-flight-anomaly-detection-system.git
cd maria-db-flight-anomaly-detector
python -m venv venv
source venv/bin/activate     # or venv\Scripts\activate on Windows
pip install -r requirements.txt
3️⃣ Configure Database

Copy the template and fill in your credentials:

cp config.py.template config.py


Edit config.py:

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "your_user",
    "password": "your_password",
    "database": "openflights",
    "port": 3306
}

4️⃣ Load Data and Run
python scripts_wf/_03_load_data.py
python scripts_wf/_04_detect_anomalies_wf.py
python scripts_wf/_05_detect_anomalies_wf.py
python scripts_wf/_runvalidation_report.py


Or open the GUI:

python app_main.py


All reports and charts appear in the outputs/ folder.

📈 Outputs
File	Description
missing_airlines.csv	Routes with invalid/missing airline reference.
duplicate_routes.csv	Duplicate source–destination combinations.
outlier_airports.csv	Airports with route counts far outside normal range.
airline_rank.csv	Airlines ranked by total routes (uses RANK() OVER()).
anomaly_summary.csv	Combined summary of anomalies.
validation_history.csv	Historical run log (timestamped).
🧱 Technology Stack

Python (scripts and Tkinter GUI)

MariaDB (InnoDB) — database backend

Pandas — for DataFrame operations and CSV creation

Matplotlib — for summary chart generation

MySQL Connector for Python — to connect MariaDB and Python

Threading (Tkinter) — for responsive GUI

💬 Future Work

Upgrade schema to MariaDB ColumnStore for large-scale analytics.

Add MariaDB Vector for similarity search (e.g., similar airports or routes).

Integrate ML anomaly detection models.

Build a web dashboard for real-time visualization.

📢 Transparency and Acknowledgements

⚡ Development Transparency:
ChatGPT was used heavily throughout the project — from shaping the original idea, designing the database schema, writing and debugging code, and creating this documentation.
The author (Amartya) has reviewed, tested, and finalized all code for accuracy and educational value.

Acknowledgements:

https://openflights.org/
 for open datasets

https://mariadb.com/
 for providing powerful analytical SQL features

ChatGPT for AI-assisted development support
```
