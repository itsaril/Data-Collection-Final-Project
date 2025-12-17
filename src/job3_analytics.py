import pandas as pd
from datetime import datetime, timedelta
from db_utils import read_events, insert_daily_summary, get_connection

def compute_daily_analytics(target_date=None):
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"Computing analytics for date: {target_date}")

    df = read_events(date_from=target_date)
    
    if df.empty:
        print(f"No data available for analytics on {target_date}")
        return
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df[df['timestamp'].dt.strftime('%Y-%m-%d') == target_date]
    
    if df.empty:
        print(f"No data matched the date {target_date} after filtering")
        return

    print(f"Total records to analyze: {len(df)}")
    
    df['date'] = df['timestamp'].dt.date
    
    grouped = df.groupby(['date', 'location'])
    
    summary = grouped.agg({
        'temperature': ['mean', 'min', 'max'],
        'humidity': 'mean',
        'wind_speed': 'mean',
        'precipitation': 'sum',
        'id': 'count'  
    }).reset_index()
    
    summary.columns = [
        'date', 'location', 
        'avg_temperature', 'min_temperature', 'max_temperature',
        'avg_humidity', 'avg_wind_speed', 'total_precipitation', 
        'record_count'
    ]
    
    for col in ['avg_temperature', 'min_temperature', 'max_temperature', 'avg_humidity', 'avg_wind_speed']:
        summary[col] = summary[col].round(2)
    summary['total_precipitation'] = summary['total_precipitation'].round(4)
    
    summary['date'] = summary['date'].astype(str)
    
    print("\nDaily Summary Statistics:")
    print(summary.to_string(index=False))
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM daily_summary WHERE date = ?", (target_date,))
    conn.commit()
    conn.close()
    
    insert_daily_summary(summary)
    return summary

if __name__ == "__main__":
    today = datetime.now().strftime('%Y-%m-%d')
    compute_daily_analytics(target_date=today)