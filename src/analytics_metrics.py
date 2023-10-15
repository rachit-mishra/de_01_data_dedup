import psycopg2
import time

# Connect to PostgreSQL
conn = psycopg2.connect(database="de_01_dedup", user="rachitmishra25", password="dedup1234", host="127.0.0.1", port="5432")
cur = conn.cursor()

# Variables to keep track of cumulative costs
cumulative_raw_cost = 0.0
cumulative_dedup_cost = 0.0
batch_count = 0

while True:
    batch_count += 1

    # Raw data metrics
    start_raw = time.time()
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT(sensor_id, timestamp)) FROM raw_weather")
    raw_count, raw_unique_count = cur.fetchone()
    duplicates_raw = raw_count - raw_unique_count
    end_raw = time.time()
    raw_time = round((end_raw - start_raw) * 1000, 2)  # Convert time to milliseconds

    # Deduplicated data metrics
    start_dedup = time.time()
    cur.execute("SELECT COUNT(*) FROM dedup_weather")
    dedup_count = cur.fetchone()[0]
    end_dedup = time.time()
    dedup_time = round((end_dedup - start_dedup) * 1000, 2)  # Convert time to milliseconds

    # Table sizes
    cur.execute("SELECT pg_total_relation_size('raw_weather')")
    raw_table_size = cur.fetchone()[0] / 1_073_741_824  # Convert bytes to GB

    
    cur.execute("SELECT pg_total_relation_size('dedup_weather')")
    dedup_table_size = cur.fetchone()[0] / 1_073_741_824  # Convert bytes to GB


    # Assuming a hypothetical cost metric (e.g., $0.001 per millisecond) to illustrate cost
    raw_cost = raw_time * 0.001
    dedup_cost = dedup_time * 0.001

    cumulative_raw_cost += raw_cost
    cumulative_dedup_cost += dedup_cost

    # Print metrics
    print(f"---- METRICS Reported at {time.strftime('%Y-%m-%d %H:%M:%S')} [Batch-{batch_count}] ----")
    print(f"Total records in raw data: {raw_count}")
    print(f"Total records in dedup data: {dedup_count}")
    print(f"Total duplicate records in raw data: {duplicates_raw}")
    print(f"Size of raw data table: {raw_table_size:.5f} GB")
    print(f"Time taken to query raw data: {raw_time} ms")
    print(f"Time taken to query deduplicated data: {dedup_time} ms")
    print(f"Size of deduplicated data table: {dedup_table_size:.5f} GB")
    print(f"Cumulative cost for querying raw data: ${cumulative_raw_cost:.5f}")
    print(f"Cumulative cost for querying deduplicated data: ${cumulative_dedup_cost:.5f}")
    print("\n")

    time.sleep(4)

cur.close()
conn.close()
