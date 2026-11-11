import asyncio
import json
import os
import socket
from datetime import datetime, timezone

from kafka import KafkaProducer
from pysnmp.hlapi.v3arch.asyncio import (
    CommunityData,
    ContextData,
    ObjectIdentity,
    ObjectType,
    SnmpEngine,
    UdpTransportTarget,
    get_cmd,
)


SNMP_HOST = os.getenv("SNMP_HOST", "192.168.64.134")
SNMP_PORT = int(os.getenv("SNMP_PORT", "161"))
SNMP_COMMUNITY = os.getenv("SNMP_COMMUNITY", "public")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "snmp-data")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))

OIDS = {
    "sysDescr": "1.3.6.1.2.1.1.1.0",
    "sysUpTime": "1.3.6.1.2.1.1.3.0",
    "sysName": "1.3.6.1.2.1.1.5.0",
    "sysLocation": "1.3.6.1.2.1.1.6.0",
}


def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
    )


async def get_snmp_data():
    engine = SnmpEngine()

    error_indication, error_status, error_index, var_binds = await get_cmd(
        engine,
        CommunityData(SNMP_COMMUNITY, mpModel=1),
        await UdpTransportTarget.create(
            (SNMP_HOST, SNMP_PORT),
            timeout=2,
            retries=2,
        ),
        ContextData(),
        *[
            ObjectType(ObjectIdentity(oid))
            for oid in OIDS.values()
        ],
    )

    if error_indication:
        raise RuntimeError(str(error_indication))

    if error_status:
        index = int(error_index) - 1
        name = list(OIDS.keys())[index] if 0 <= index < len(OIDS) else "unknown"
        raise RuntimeError(f"{error_status.prettyPrint()} at {name}")

    data = {}
    for name, var_bind in zip(OIDS.keys(), var_binds):
        data[name] = str(var_bind[1])

    return data


async def main():
    print("=" * 60)
    print("SNMP -> Kafka Producer")
    print(f"SNMP device : {SNMP_HOST}:{SNMP_PORT}")
    print(f"Kafka       : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic       : {KAFKA_TOPIC}")
    print(f"Interval    : {POLL_INTERVAL}s")
    print("=" * 60)

    producer = build_producer()

    while True:
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            snmp_data = await get_snmp_data()

            message = {
                "device": SNMP_HOST,
                "hostname": socket.gethostname(),
                "protocol": "SNMPv2c",
                "timestamp": timestamp,
                "data": snmp_data,
            }

            future = producer.send(
                KAFKA_TOPIC,
                key=SNMP_HOST,
                value=message,
            )
            metadata = future.get(timeout=10)
            producer.flush()

            print(
                f"[SENT] {timestamp} | "
                f"topic={metadata.topic} | "
                f"partition={metadata.partition} | "
                f"offset={metadata.offset}"
            )
            print(json.dumps(message, indent=2, ensure_ascii=False))

        except Exception as exc:
            print(f"[ERROR] {datetime.now().isoformat()} - {exc}")

        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
