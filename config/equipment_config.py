EQUIPMENT = [
    {
        "machine_id": "TURB-001",
        "equipment_type": "Turbine",
    },
    {
        "machine_id": "TURB-002",
        "equipment_type": "Turbine",
    },
    {
        "machine_id": "COMP-001",
        "equipment_type": "Compressor",
    },
    {
        "machine_id": "COMP-002",
        "equipment_type": "Compressor",
    },
    {
        "machine_id": "PUMP-001",
        "equipment_type": "Pump",
    },
    {
        "machine_id": "PUMP-002",
        "equipment_type": "Pump",
    }
]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Replace with your Kafka bootstrap servers
KAFKA_TOPIC = "machine-telemetry"  # Replace with your Kafka topic name

EVENT_INTERVAL_SECONDS = 2