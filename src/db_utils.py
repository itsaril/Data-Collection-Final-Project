import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("/opt/airflow/data/app.db")

def init_database():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            location TEXT NOT NULL,
            temperature REAL,
            humidity REAL,
            wind_speed REAL,
            precipitation REAL,
            weather_code INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            location TEXT NOT NULL,
            avg_temperature REAL,
            min_temperature REAL,
            max_temperature REAL,
            avg_humidity REAL,
            avg_wind_speed REAL,
            total_precipitation REAL,
            record_count INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, location)
        )
    """)
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")

def get_connection():
    return sqlite3.connect(DB_PATH)

def insert_events(df: pd.DataFrame):
    conn = get_connection()
    df.to_sql('events', conn, if_exists='append', index=False)
    conn.close()
    print(f"Inserted {len(df)} records into events table")

def insert_daily_summary(df: pd.DataFrame):
    conn = get_connection()
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute(
            "DELETE FROM daily_summary WHERE date = ? AND location = ?",
            (row['date'], row['location'])
        )
    
    conn.commit()
    
    df.to_sql('daily_summary', conn, if_exists='append', index=False)
    conn.close()
    print(f"Inserted {len(df)} records into daily_summary table")

def read_events(date_from=None):
    conn = get_connection()
    query = "SELECT * FROM events"
    if date_from:
        query += f" WHERE DATE(timestamp) >= '{date_from}'"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df