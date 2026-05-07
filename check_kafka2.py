from kafka.admin import KafkaAdminClient

try:
    admin = KafkaAdminClient(bootstrap_servers='localhost:9093')
    topics = admin.list_topics()
    print("Connected! Topics found:")
    for t in topics:
        print(f"  {t}")
    admin.close()
except Exception as e:
    print(f"Error: {e}")