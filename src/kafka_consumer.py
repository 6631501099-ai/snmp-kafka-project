import json
import os

from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "snmp-data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "snmp-monitor")


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        key_deserializer=lambda key: (
            key.decode("utf-8") if key else None
        ),
    )

    print("=" * 60)
    print("Kafka Consumer - SNMP Monitoring")
    print(f"Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("=" * 60)

    try:
        for message in consumer:
            print("\n" + "-" * 60)
            print(f"Topic     : {message.topic}")
            print(f"Partition : {message.partition}")
            print(f"Offset    : {message.offset}")
            print(f"Key       : {message.key}")
            print("Value:")

            print(json.dumps(
                message.value,
                indent=2,
                ensure_ascii=False,
            ))
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
