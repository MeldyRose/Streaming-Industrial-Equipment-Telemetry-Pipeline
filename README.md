# Streaming Industrial Equipment Telemetry Pipeline

Streaming Industrial Equipment Telemetry Pipeline is an individual project for learning data engineering and building an end-to-end, containerized streaming data pipeline to collect, transform, and analyze real-time telemetry data from industrial equipment.

## Project Overview

This project builds a fully containerized streaming telemetry data pipeline that:
- Simulates telemetry sensor data from industrial equipment (Turbines, Compressors, Pumps) using Python and streams events via Apache Kafka on Docker
- Consumes streaming telemetry events and batches raw data into AWS S3 as raw JSON (Bronze layer)
- Transforms and cleans raw data using PySpark Structured Streaming on Docker to add equipment status indicators (`NORMAL` / `WARNING`) and partition by equipment type (Silver layer)
- Aggregates daily telemetry metrics and warning counts using PySpark on Docker and partitions by date (Gold layer)
- Stores structured Medallion Architecture datasets (Bronze, Silver, Gold) in AWS S3 cloud storage
- Enables SQL queries in AWS Athena for equipment health and anomaly analysis
- Prepares analytical data for interactive reporting in Power BI (In Progress)

> **Architectural Note:** The entire pipeline (Kafka broker, sensor simulation producer, S3 batch consumer, and PySpark streaming jobs) is fully containerized and automatically managed via **Docker Compose**. Orchestration tools like Apache Airflow were intentionally omitted to avoid unnecessary complexity, as the producer serves as a continuous simulation source and all pipeline components run seamlessly as containerized streaming services.

## Tech Stack

- Python
- Apache Kafka
- PySpark (Apache Spark Structured Streaming)
- AWS S3
- AWS Athena
- Docker & Docker Compose
- Power BI

## Prerequisites

- Python 3.12+
- Docker & Docker Compose
- AWS Account (S3 Bucket & IAM Access Keys with Athena access)
- Git
> I personally developed/tested on Python 3.14.6

## Setup

### 1. Clone the repository

```bash
    git clone https://github.com/MeldyRose/Streaming-Industrial-Equipment-Telemetry-Pipeline.git
    cd Streaming-Industrial-Equipment-Telemetry-Pipeline
```

### 2. Create a virtual environment

```bash
    python -m venv .venv
```

Activate it:

**Windows**
```bash
    .venv\Scripts\activate
```

### 3. Install dependencies

* **Docker (Recommended)**: Dependencies listed in `requirements.txt` are automatically installed inside the containers via the `Dockerfile` when you run `docker compose up --build`. No manual installation is needed!
* **Local Python Execution (Fallback)**: If running producer or consumer scripts manually outside Docker, install dependencies inside your virtual environment:

  ```bash
  pip install -r requirements.txt
  ```

### 4. Configure environment variables

Create a `.env` file by copying `.env.example`:

**Linux / macOS / Git Bash / PowerShell:**
```bash
cp .env.example .env
```

Or on **Windows Command Prompt (cmd)**:
```cmd
copy .env.example .env
```

Open `.env` and fill in your AWS credentials (`KEY_ID`, `SECRET_KEY`, `AWS_REGION`, `S3_BUCKET_NAME`).

> **Security Note:** Never commit your `.env` file or API keys to Git (`.env` is included in `.gitignore`).

### 5. Run the pipeline

The entire streaming pipeline (Kafka, sensor producer, S3 consumer, and Spark streaming containers) is fully automated with Docker Compose.

#### 5.1 Build and Start Containerized Pipeline with Docker Compose

Run all services in background mode:

```bash
docker compose up --build -d
```

This single command builds the custom Python container image using `Dockerfile` and launches:
- **`kafka`**: Apache Kafka message broker for real-time telemetry streaming
- **`producer`**: Python sensor simulator producing real-time telemetry events to Kafka
- **`consumer`**: Python S3 consumer batching raw telemetry messages into JSON and uploading to AWS S3 (`bronze/` folder)
- **`spark-silver`**: PySpark Structured Streaming job enriching raw Bronze data, adding warning flags, and writing Parquet files to AWS S3 (`silver/` folder, partitioned by `equipment_type`)
- **`spark-gold`**: PySpark Structured Streaming job calculating 1-day aggregated operational metrics and writing Parquet files to AWS S3 (`gold/` folder, partitioned by `date`)

#### 5.2 Verify Running Services

To verify all pipeline containers are running:

```bash
docker compose ps
```

To view logs for any specific container (e.g. producer or consumer):

```bash
docker compose logs -f producer
docker compose logs -f consumer
```

#### 5.3 Stop Pipeline Services

To stop all running services:

```bash
docker compose down
```

#### 5.4 Query Telemetry Data via AWS Athena (SQL)

1. **Create Table**: Execute `config/sql/create_database.sql` in AWS Athena to register the external table `silver_db.telemetry_data` referencing the Parquet data in `s3://<bucket>/silver/`.
2. **Run Analytical Queries**: Execute `config/sql/analysis_queries.sql` to run anomaly detection, machine metric averages, peak readings, and warning rate percentage queries.

## ELT Pipeline

```text
[ Sensor Simulator (Python) ] ──> [ Apache Kafka (Docker) ] ──> [ S3 Consumer (Python) ] ──> [ AWS S3 (Bronze JSON) ]
  (Containerized Producer)        (Message Broker)              (Containerized Consumer)            │
                                                                                                    ▼
[ Power BI ] <── [ AWS Athena (SQL Queries) ] <── [ AWS S3 (Gold/Silver Parquet) ] <── [ PySpark Silver / Gold ]
(In Progress)   (config/sql/)                    (Partitioned Datasets)                 (Containerized Spark Jobs)
```

## Project Structure
```
Streaming-Industrial-Equipment-Telemetry-Pipeline/
│
├── config/
│   ├── sql/
│   │   ├── analysis_queries.sql
│   │   └── create_database.sql
│   └── equipment_config.py
│
├── consumer/
│   └── s3.py
│
├── producer/
│   ├── create_topic.py
│   ├── delete_topic.py
│   ├── sensor_simulator.py
│   ├── test_kafka.py
│   └── test_s3.py
│
├── spark/
│   ├── gold_stream.py
│   ├── silver_stream.py
│   └── test_spark.py
│
├── tests/
│
├── .env.example
├── .gitignore
├── docker-compose.yaml
├── Dockerfile
├── LICENSE
├── README.md
└── requirements.txt
```

## Data Source

- Simulated Industrial Equipment Sensor Telemetry (Turbines, Compressors, Pumps) generating metrics for temperature, humidity, vibration, and pressure.

## Data Dictionary

The processed equipment telemetry data includes:

- `machine_id` = unique identifier for equipment (e.g. `TURB-001`, `COMP-001`, `PUMP-001`)
- `equipment_type` = machine classification (`Turbine`, `Compressor`, `Pump`)
- `timestamp` = ISO-8601 UTC event timestamp
- `temperature_c` = operating temperature in Celsius (°C)
- `humidity_percent` = relative humidity in percentage (%)
- `vibration_mm_s` = vibration velocity in millimeters per second (mm/s)
- `pressure_bar` = operational pressure in bar
- `equipment_status` = health/operational condition status
    - `NORMAL` = operational parameters within safe limits
    - `WARNING` = abnormal parameters detected (`temperature_c > 85` or `vibration_mm_s > 5`)

## Analysis

The telemetry data produced by the streaming pipeline is structured using a Medallion Architecture (Bronze -> Silver -> Gold) stored in AWS S3 and queried in AWS Athena:

- **Bronze Layer**: Raw JSON records ingested directly from Kafka streams.
- **Silver Layer**: Cleaned telemetry with timestamps and status condition flags (`NORMAL` / `WARNING`), partitioned by `equipment_type`.
- **Gold Layer**: Daily aggregated operational metrics (average/max temperature, vibration, pressure, and total warning counts), partitioned by `date`.

### Athena SQL Analytical Queries

The SQL scripts in `config/sql/` provide insights into machine operational health:

1. **Abnormal Sensor Conditions**: Counts warning/critical events per machine.
2. **Equipment Type Averages**: Calculates average temperature, humidity, vibration, and pressure grouped by machine.
3. **Peak Temperature**: Identifies machines with maximum temperature readings.
4. **Peak Humidity**: Identifies machines with maximum humidity readings.
5. **Peak Vibration**: Identifies machines with maximum vibration levels.
6. **Peak Pressure**: Identifies machines with maximum pressure readings.
7. **Abnormal Condition Rate**: Calculates warning rate percentage (`warning_count / total_readings * 100`) for each machine.

### Current Status (In Progress)

- [x] Full end-to-end containerization with `Dockerfile` and `docker-compose.yaml` (running Kafka broker, sensor producer simulator, S3 batch consumer, PySpark Silver, and PySpark Gold containers).
- [x] Python data generation simulating industrial equipment sensors streaming to Kafka on Docker.
- [x] Raw Bronze data ingested from Kafka and loaded into AWS S3 cloud storage.
- [x] PySpark streaming on Docker transforming Bronze data to Silver layer with partitioning by `equipment_type`.
- [x] PySpark streaming aggregating Silver data to Gold layer with partitioning by `date` and writing back to S3.
- [x] Created AWS Athena DDL table definition (`create_database.sql`) and 7 analytical queries (`analysis_queries.sql`) in `config/sql/`.
- [ ] Connect AWS Athena queries to Power BI for interactive dashboard reports (*In Progress*).

## Future Improvements

- Finalize Power BI dashboard visualizations connected to AWS Athena.
- Add data quality tests and assertions (e.g. Great Expectations / Pytest).
