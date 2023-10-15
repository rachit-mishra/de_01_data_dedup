from kafka import KafkaProducer
import random
import time
import json

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Set some constants
NUM_SENSORS = 10
TOPIC_NAME = "dedup_weather_topic"
LOG_INTERVAL = 4  # in seconds
CACHE_SIZE = 100  # Decreased from 100

# Counter for messages produced
msg_count = 0

# Maintain a cache for recently generated records
recent_records = []

start_time = time.time()

def modify_data(data):
    """Function to slightly modify the temperature and humidity."""
    data['temperature'] += random.uniform(-0.5, 0.5)
    data['humidity'] += random.uniform(-1, 1)
    return data

try:
    while True:
        if random.random() < 0.3 and len(recent_records) > 0:  # Reduced probability to 30%
            data = random.choice(recent_records)
            # if random.random() < 0.2:  # 20% chance to modify the data slightly
            #     data = modify_data(data)
        else:
            # Create mock sensor data
            data = {
                "sensor_id": f"sensor_{random.randint(1, NUM_SENSORS)}",
                "temperature": random.uniform(20.0, 30.0),
                "humidity": random.uniform(30.0, 70.0),
                "timestamp": int(time.time())
            }
            # Update the cache
            recent_records.append(data)
            if len(recent_records) > CACHE_SIZE:
                recent_records.pop(0)

        # Send data to Kafka topic
        producer.send(TOPIC_NAME, value=data)
        msg_count += 1

        # Log data every LOG_INTERVAL seconds
        elapsed_time = time.time() - start_time
        if elapsed_time > LOG_INTERVAL:
            print(f"Produced {msg_count} messages to {TOPIC_NAME} in the last {LOG_INTERVAL} seconds.")
            msg_count = 0
            start_time = time.time()

        # Optional: Sleep for a bit to simulate some delay in producing data
        time.sleep(0.2)

except KeyboardInterrupt:
    print("\nSimulation interrupted. Exiting...")
    producer.close()
