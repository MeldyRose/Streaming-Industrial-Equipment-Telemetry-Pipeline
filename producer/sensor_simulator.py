import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

from config.equipment_config import (
    EQUIPMENT,
    EVENT_INTERVAL_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)

def generate_telemetry_data(machine):
    """ Generate one sensor telemetry event."""

    return {
        "machine_id": machine["machine_id"],
        "equipment_type": machine["equipment_type"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_c": round(random.uniform(50.0, 90.0), 2),
        "humidity_percent": round(random.uniform(40.0, 80.0), 2),
        "vibration_mm_s": round(random.uniform(1.0, 6.0), 2),
        "pressure_bar": round(random.uniform(3.0, 8.0), 2),
    }

def create_kafka_producer():
    """ Create a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def main():
    producer = create_kafka_producer()

    print("Starting industrial equipment sensor simulator...")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Event interval: {EVENT_INTERVAL_SECONDS} seconds")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            for machine in EQUIPMENT:
                event = generate_telemetry_data(machine)
                producer.send(KAFKA_TOPIC, value=event)
                print(
                    f"[PRODUCER] "
                    f"{event['machine_id']} | "
                    f"{event['equipment_type']} | "
                    f"Temp: {event['temperature_c']}°C | "
                    f"Pressure: {event['pressure_bar']} bar | "
                    f"Vibration: {event['vibration_mm_s']} mm/s | "
                    f"Humidity: {event['humidity_percent']}%"
                )

                producer.flush()  # Ensure the message is sent before proceeding
                time.sleep(EVENT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Telemetry simulation stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()