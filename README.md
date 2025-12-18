# 🌦️ Weather Data Pipeline: Almaty Real-Time Analytics

This project implements a complete **End-to-End Data Pipeline** that collects, processes, and analyzes high-frequency weather data for Almaty. It combines **Streaming** (via Kafka) and **Batch** (via Airflow) processing to provide daily weather insights.

---

## 👥 Team Information

* **Student 1:** Zhilikbay Arman  #22B030358
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

---

## 🚀 How to Run

1. **Clone the repository:**
```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name

```
2. **Launch the environment:**
```bash
docker-compose up -d

```


4. **Access Airflow UI:**
Navigate to `http://localhost:8080`
* **Login:** `admin`
* **Password:** `admin`

---

## 📊 Database Schema

### `daily_summary` table (Cleaned Data)

| id | date | location | avg_temp | min_temp | max_temp | avg_humidity | avg_wind | precip | records | created_at |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 3 | 2025-12-17 | Almaty | 2.75 | 2.0 | 2.8 | 80.33 | 0.3 | 0.0 | 15 | 2025-12-18 09:43:04 |

### `events` table (Analytics)

| id | timestamp | location | temperature | humidity | wind_speed | precipitation | weather_code | created_at |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | 2025-12-17 09:08:55 | Almaty | 2.8 | 80.0 | 0.3 | NULL | 5001 | 2025-12-17 09:40:54 |
| 2 | 2025-12-17 09:09:02 | Almaty | 2.8 | 80.0 | 0.3 | NULL | 5001 | 2025-12-17 09:40:54 |
| 3 | 2025-12-17 09:09:31 | Almaty | 2.8 | 80.0 | 0.3 | NULL | 5001 | 2025-12-17 09:40:54 |
| 15 | 2025-12-17 10:00:02 | Almaty | 2.0 | 85.0 | 0.3 | NULL | 5001 | 2025-12-17 10:00:14 |

