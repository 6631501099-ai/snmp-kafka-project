# SNMP Kafka Project

SNMP monitoring project that collects device information using SNMPv2c and publishes the result to Apache Kafka.

## Architecture

SNMP Device
    |
    | SNMPv2c UDP/161
    v
snmp_producer.py
    |
    | JSON
    v
Kafka topic: snmp-data
    |
    v
kafka_consumer.py

Kafka UI is available at http://localhost:8080.

## 1. Start Kafka

```bash
docker compose up -d
docker compose ps
```

## 2. Create Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Configure SNMP

```bash
cp .env.example .env
nano .env
```

Set the SNMP device address and community to match your router/device.

Example:

```env
SNMP_HOST=192.168.64.134
SNMP_PORT=161
SNMP_COMMUNITY=public
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=snmp-data
POLL_INTERVAL=10
```

## 4. Start the consumer

```bash
source .venv/bin/activate
python3 src/kafka_consumer.py
```

## 5. Start the producer

Open another terminal:

```bash
source .venv/bin/activate
python3 src/snmp_producer.py
```

The producer collects:

- sysDescr
- sysUpTime
- sysName
- sysLocation

and publishes JSON messages to `snmp-data`.

## 6. Check Kafka topic

```bash
docker exec -it snmp-kafka /opt/kafka/bin/kafka-topics.sh   --bootstrap-server localhost:9092   --list
```

Check topic information:

```bash
docker exec -it snmp-kafka /opt/kafka/bin/kafka-topics.sh   --bootstrap-server localhost:9092   --describe   --topic snmp-data
```

## 7. Kafka UI

Open:

http://localhost:8080

Cluster:

```text
kafka-local
```

Bootstrap server inside Docker:

```text
kafka:29092
```

Topic:

```text
snmp-data
```

## GitHub

Do not commit `.env`.

```bash
git init
git add .
git status
git commit -m "Add SNMP Kafka monitoring project"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/snmp-kafka-project.git
git push -u origin main
```
