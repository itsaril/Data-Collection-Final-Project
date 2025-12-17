import os
import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

API_KEY = "gXtHoXgapISBqMEnSz3kQw99MKlLsj63"
URL = "https://api.tomorrow.io/v4/weather/realtime"

LOCATIONS = [
    {"name": "Almaty", "lat": 43.2220, "lon": 76.8512}
]

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "raw_weather_events"

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_block_ms=5000
    )

def fetch_weather_data(location):
    url = "https://api.tomorrow.io/v4/weather/realtime"
    params = {
        "location": f"{location['lat']},{location['lon']}",
        "apikey": API_KEY,
        "units": "metric"
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 429:
            print(f"!!! Rate limit hit (429). Waiting...")
            time.sleep(300)
            return None
            
        response.raise_for_status()
        data = response.json()
        
        if 'data' in data and 'values' in data['data']:
            values = data['data']['values']
            return {
                "timestamp": datetime.now().isoformat(),
                "location": location['name'],
                "temperature": values.get('temperature'),
                "humidity": values.get('humidity'),
                "wind_speed": values.get('windSpeed'),
                "precipitation": values.get('precipitationIntensity'),
                "weather_code": values.get('weatherCode', 0)
            }
        else:
            print(f"Unexpected JSON structure for {location['name']}: {data.keys()}")
            return None
            
    except Exception as e:
        print(f"API error for {location['name']}: {e}")
        return None
    
def run_producer(duration_minutes=50): 
    producer = create_kafka_producer()
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    print(f"The producer is launched on {duration_minutes} min. Interval: 3 minutes.")

    while time.time() < end_time:
        cycle_start = time.time()
        
        for location in LOCATIONS:
            weather_data = fetch_weather_data(location)
            if weather_data:
                producer.send(KAFKA_TOPIC, weather_data)
                print(f"Sent: {location['name']} в {weather_data['timestamp']}")
            
            time.sleep(2) 
        
        elapsed = time.time() - cycle_start
        wait_time = max(0, 180 - elapsed) 
        
        print(f"Cycle completed. Waiting {round(wait_time, 1)} sec. until next data collection...")
        
        if time.time() + wait_time < end_time:
            time.sleep(wait_time)
        else:
            break