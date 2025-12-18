# 🌦️ Weather Data Pipeline: Almaty Real-Time Analytics

This project implements a complete **End-to-End Data Pipeline** that collects, processes, and analyzes high-frequency weather data for Almaty. It combines **Streaming** (via Kafka) and **Batch** (via Airflow) processing to provide daily weather insights.

---

## 👥 Team Information

* **Student 1:** Zhilikbay Arman  #22B
* **Student 2:** Shakhizada Zgansulu #22B030468
* **Student 3:** Myrzakhankyzy Arailym #22B030408

---

## 🏗️ System Architecture

The pipeline is divided into three main stages, each orchestrated by a dedicated **Apache Airflow DAG**:

### 1️⃣ DAG 1: Continuous Ingestion (Pseudo-Streaming)

* **Source:** [Tomorrow.io API](https://www.tomorrow.io/weather-api/)
* **Interval:** Fetches data every 3 minutes.
* **Target:** Sends raw JSON events to a **Kafka** topic (`raw_weather_events`).

### 2️⃣ DAG 2: Cleaning & Storage (Hourly Batch)

* **Source:** Consumes messages from Kafka.
* **Processing:** Uses **Pandas** for data cleaning (handling duplicates, type conversion, and missing values).
* **Target:** Saves cleaned data into **SQLite** (`events` table).

### 3️⃣ DAG 3: Daily Analytics (Daily Batch)

* **Source:** Reads from the `events` table.
* **Processing:** Uses **Pandas** to aggregate data (min/max/avg temperatures, total precipitation).
* **Target:** Stores summarized data in the `daily_summary` table.

---

## 🛠️ Tech Stack

| Component | Technology |
| --- | --- |
| **Orchestration** | Apache Airflow (LocalExecutor) |
| **Message Broker** | Apache Kafka & Zookeeper |
| **Data Processing** | Python, Pandas |
| **Database** | SQLite & PostgreSQL (for Airflow) |
| **Infrastructure** | Docker & Docker Compose |

---

## 📂 Project Structure

```text
project/
├── airflow/
│   └── dags/                  # Airflow DAG files
├── src/                       # Core Logic
│   ├── job1_producer.py       # API -> Kafka logic
│   ├── job2_cleaner.py        # Kafka -> SQLite logic (Pandas)
│   ├── job3_analytics.py      # Analytics logic (Pandas)
│   └── db_utils.py            # SQLite helper functions
├── data/                      # Local storage for app.db
├── Dockerfile                 # Custom Airflow image with dependencies
├── docker-compose.yml         # Full infrastructure setup
└── requirements.txt           # Python libraries

```

---

## 🚀 How to Run

1. **Clone the repository:**
```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name

```
2. **Launch the environment:**
```bash
docker-compose up --build

```


4. **Access Airflow UI:**
Navigate to `http://localhost:8080`
* **Login:** `admin`
* **Password:** `admin`



---

## 📊 Database Schema

### `events` table (Cleaned Data)

| Column | Type | Description |
| --- | --- | --- |
| `timestamp` | TEXT | Event time |
| `location` | TEXT | Almaty (lat/lon) |
| `temperature` | REAL | Celsius |
| `humidity` | REAL | Percentage |

### `daily_summary` table (Analytics)

| Column | Type | Description |
| --- | --- | --- |
| `date` | TEXT | Summary date |
| `avg_temperature` | REAL | Mean temperature |
| `record_count` | INT | Total samples per day |
