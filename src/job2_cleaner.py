import json
import pandas as pd
from kafka import KafkaConsumer
from db_utils import insert_events, init_database

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "raw_weather_events"
KAFKA_GROUP_ID = "weather_cleaner_group"

def create_kafka_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000  
    )

def clean_weather_data(messages):

    if not messages:
        print("No messages to clean")
        return pd.DataFrame()
    
    df = pd.DataFrame(messages)
    
    print(f"Initial records: {len(df)}")
    
    df = df.drop_duplicates(subset=['location', 'timestamp'])
    print(f"After removing duplicates: {len(df)}")
    
    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
    df['humidity'] = pd.to_numeric(df['humidity'], errors='coerce')
    df['wind_speed'] = pd.to_numeric(df['wind_speed'], errors='coerce')
    df['precipitation'] = pd.to_numeric(df['precipitation'], errors='coerce')
    df['weather_code'] = pd.to_numeric(df['weather_code'], errors='coerce').astype('Int64')
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    df = df.dropna(subset=['timestamp'])
    print(f"After removing invalid timestamps: {len(df)}")

    df = df[(df['temperature'].isna()) | 
            ((df['temperature'] >= -50) & (df['temperature'] <= 50))]
    
    df = df[(df['humidity'].isna()) | 
            ((df['humidity'] >= 0) & (df['humidity'] <= 100))]
    
    df = df[(df['wind_speed'].isna()) | 
            ((df['wind_speed'] >= 0) & (df['wind_speed'] <= 200))]
    
    df = df[(df['precipitation'].isna()) | (df['precipitation'] >= 0)]
    
    print(f"After range validation: {len(df)}")
    
    numeric_columns = ['temperature', 'humidity', 'wind_speed', 'precipitation']
    for col in numeric_columns:
        if col in df.columns and df[col].notna().any():
            median_value = df[col].median()
            df[col] = df[col].fillna(median_value)
    
    df['temperature'] = df['temperature'].round(2)
    df['humidity'] = df['humidity'].round(2)
    df['wind_speed'] = df['wind_speed'].round(2)
    df['precipitation'] = df['precipitation'].round(4)
    
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    columns_to_keep = [
        'timestamp', 'location', 'temperature', 'humidity', 
        'wind_speed', 'precipitation', 'weather_code'
    ]
    df = df[columns_to_keep]
    
    print(f"Final cleaned records: {len(df)}")
    
    return df

def run_cleaner():

    init_database()
    
    consumer = create_kafka_consumer()
    messages = []
    
    print("Starting to consume messages from Kafka...")
    
    try:
        for message in consumer:
            messages.append(message.value)
            print(f"Consumed message: {message.value['location']} at {message.value['timestamp']}")
        
    except Exception as e:
        print(f"Consumer stopped: {e}")
    
    finally:
        consumer.close()
    
    if messages:
        print(f"\nTotal messages consumed: {len(messages)}")
        cleaned_df = clean_weather_data(messages)
        
        if not cleaned_df.empty:
            insert_events(cleaned_df)
            print(f"Successfully inserted {len(cleaned_df)} cleaned records")
        else:
            print("No valid records to insert after cleaning")
    else:
        print("No messages were consumed")

if __name__ == "__main__":
    run_cleaner()