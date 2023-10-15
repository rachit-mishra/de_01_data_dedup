from kafka import KafkaConsumer, TopicPartition
import psycopg2
import json
import datetime

# Connect to PostgreSQL
conn = psycopg2.connect(database="de_01_dedup", user="rachitmishra25", password="dedup1234", host="127.0.0.1", port="5432")
cur = conn.cursor()

# Flush tables
cur.execute("TRUNCATE raw_weather, dedup_weather")
conn.commit()
print("[INFO] Flushed existing data from raw_weather and dedup_weather tables.")

# Kafka Consumers
consumer = KafkaConsumer(bootstrap_servers='localhost:9092', value_deserializer=lambda v: json.loads(v.decode('utf-8')))
consumer.subscribe(['weather_data_raw', 'weather_data_dedup'])

while True:
    msg = consumer.poll(timeout_ms=1000)  # Poll for a second
    
    if msg:
        for tp, messages in msg.items():
            for message in messages:
                data = message.value
                timestamp = datetime.datetime.fromtimestamp(data['timestamp'])
                
                if tp.topic == 'weather_data_raw':
                    cur.execute("INSERT INTO raw_weather (sensor_id, temperature, humidity, timestamp) VALUES (%s, %s, %s, %s)", (data['sensor_id'], data['temperature'], data['humidity'], timestamp))
                    print(f"[{datetime.datetime.now()}] Inserted raw data for sensor_id: {data['sensor_id']} at timestamp: {data['timestamp']}")
                elif tp.topic == 'weather_data_dedup':
                    cur.execute("INSERT INTO dedup_weather (sensor_id, temperature, humidity, timestamp) VALUES (%s, %s, %s, %s)", (data['sensor_id'], data['temperature'], data['humidity'], timestamp))
                    print(f"[{datetime.datetime.now()}] Inserted deduplicated data for sensor_id: {data['sensor_id']} at timestamp: {data['timestamp']}")
                
                conn.commit()
