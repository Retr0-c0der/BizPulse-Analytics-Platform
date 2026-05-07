import time
import json
import numpy as np
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

KAFKA_BROKER = 'localhost:9093'
KAFKA_TOPIC = 'latency_final_v2'
NUM_MESSAGES = 500

# --- Create fresh topic ---
print("Setting up topic...")
admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
existing = admin.list_topics()
if KAFKA_TOPIC in existing:
    admin.delete_topics([KAFKA_TOPIC])
    time.sleep(2)
admin.create_topics([NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)])
admin.close()
time.sleep(3)
print("Topic ready.")

# --- Send messages WITH timestamps baked in ---
print(f"Sending {NUM_MESSAGES} messages...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

for i in range(NUM_MESSAGES):
    producer.send(KAFKA_TOPIC, value={
        'msg_id': i,
        'publish_time_ms': time.time() * 1000
    })

producer.flush()
producer.close()
send_done_time = time.time() * 1000
print(f"All {NUM_MESSAGES} messages sent.")

# --- Now consume from offset 0 ---
print("Consuming from offset 0...")
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=10000,
    enable_auto_commit=False,
    group_id=None,
    fetch_min_bytes=1,
    fetch_max_wait_ms=100
)

tp = TopicPartition(KAFKA_TOPIC, 0)
consumer.assign([tp])
consumer.seek(tp, 0)  # start from very beginning

latencies = []
for msg in consumer:
    receive_time = time.time() * 1000
    latency = receive_time - msg.value['publish_time_ms']
    latencies.append(latency)
    if len(latencies) >= NUM_MESSAGES:
        break

consumer.close()

# --- Results ---
print(f"\nReceived: {len(latencies)} messages")
if len(latencies) == 0:
    print("ERROR: No messages received.")
else:
    latencies = np.array(latencies)
    print(f"\n========== KAFKA LATENCY RESULTS ==========")
    print(f"  Messages measured:  {len(latencies)}")
    print(f"  Min latency:        {np.min(latencies):.1f} ms")
    print(f"  Median (p50):       {np.percentile(latencies, 50):.1f} ms")
    print(f"  p95:                {np.percentile(latencies, 95):.1f} ms")
    print(f"  p99:                {np.percentile(latencies, 99):.1f} ms")
    print(f"  Max latency:        {np.max(latencies):.1f} ms")
    print(f"  Mean latency:       {np.mean(latencies):.1f} ms")