# Metadata-Driven IoT Sensor Monitoring Platform

A production-inspired, metadata-driven IoT data engineering project that simulates sensor telemetry from a solar panel manufacturing plant and processes it through a modern Medallion Data Lake architecture.

Instead of hardcoding devices, thresholds, schemas and business rules, the complete platform is driven through metadata, making the system scalable, configurable and maintainable.

The project demonstrates how industrial IoT telemetry can be ingested, validated, enriched, monitored and transformed into business-ready datasets while supporting real-time alert generation.

---

# Project Objectives

The primary goals of this project are:

- Simulate realistic manufacturing IoT sensor telemetry
- Demonstrate metadata-driven pipeline design
- Build an enterprise Medallion Architecture
- Perform data quality validation
- Detect operational anomalies
- Generate real-time alerts
- Produce analytical datasets for reporting
- Showcase production-grade Data Engineering practices

---

# Architecture

                    devices.yaml
                          │
                          │
                    sensor_config.yaml
                          │
                          ▼
                Sensor Data Generator
                          │
                          ▼
               HuggingFace Object Storage
                    (Raw Landing Zone)
                          │
          ┌───────────────┴───────────────┐
          │                               │
          ▼                               ▼
     Alert Engine                  Bronze Pipeline
         │                                │
         ▼                                ▼
     Alert Table                     Silver Layer
                                          │
                                          ▼
                                      Gold Layer
                                          │
                                          ▼
                                 Power BI / Analytics

<!-- Power BI connecting both pipeline outputs in different dashboards -->

                        Sensor Simulator
                               │
                         Raw Telemetry
                               │
                  ┌────────────┴────────────┐
                  │                         │
                  ▼                         ▼
           Alert Engine             Medallion Pipeline
                  │                         │
                  ▼                         ▼
            Alert Fact Table      Bronze → Silver → Gold
                  │                         │
                  └────────────┬────────────┘
                               ▼
                      Power BI Dashboards
---

# Metadata Driven Design

Every component of the platform is driven by metadata.

The simulator never contains device-specific logic.

Instead it dynamically reads metadata describing:

- Plants
- Production Lines
- Machines
- Sensors
- Firmware Versions
- Sampling Frequency
- Thresholds
- Units
- Safety Criticality
- Calibration Information

Adding a new sensor requires only updating the metadata files.

No application code needs to change.

---

# Repository Structure

```
project/

│
├── config/
│     ├── devices.yaml
│     └── sensor_config.yaml
│
├── generator/
│     └── simulate_sensors.py
│
├── pipelines/
│     ├── bronze/
│     ├── silver/
│     └── gold/
│
├── alert_engine/
│
├── docs/
│
├── .github/
│     └── workflows/
│
├── requirements.txt
│
└── README.md
```

---

# Technology Stack

| Component | Technology |
|------------|------------|
| Programming | Python |
| Metadata | YAML |
| Storage | HuggingFace Object Storage (S3 Compatible) |
| Data Lake | Medallion Architecture |
| Workflow | GitHub Actions |
| Serialization | NDJSON |
| Cloud API | boto3 |
| Dashboard | Power BI |
| Version Control | Git |

---

# Metadata Configuration

The entire system is configured through two metadata files.

## devices.yaml

Contains:

- Plant hierarchy
- Production lines
- Machines
- Sensor inventory
- Firmware versions
- Installation dates

## sensor_config.yaml

Defines:

- Units
- Sampling frequency
- Normal ranges
- Warning thresholds
- Critical thresholds
- Noise characteristics
- Drift rates
- Offline timeout
- Safety critical flag

---

# Sensor Data Generation Engine

The simulator automatically:

- Loads metadata
- Discovers every configured sensor
- Generates realistic sensor readings
- Simulates Gaussian noise
- Simulates gradual sensor drift
- Injects spikes
- Simulates flatline sensors
- Simulates null readings
- Simulates offline devices
- Simulates late arriving events
- Uploads partitioned NDJSON files

The simulator is completely metadata driven.

---

# Raw Data Format

Each record represents a single sensor observation.

Example:

```json
{
  "device_id": "...",
  "device_type": "...",
  "plant_id": "...",
  "machine_id": "...",
  "timestamp": "...",
  "reading": {
      "value": 120.35,
      "unit": "celsius"
  },
  "firmware_version": "...",
  "quality_flag": "OK",
  "ingestion_timestamp": "...",
  "source_file_name": "..."
}
```

---

# Storage Layout

Upload a batch of readings as a single NDJSON file.

Path convention: raw/year=YYYY/month=MM/day=DD/hour=HH/{batch_id}.json

Example:

```json
raw/
   year=2026/
      month=08/
         day=04/
            hour=05/
                batch_xxxxx.json
```

---

# Storage Format

| Layer | Format | Reason | Responsibilities |
|------------|------------|------------|------------|
| Raw |	NDJSON	| Immutable ingestion, replay, audit | Store exactly what devices sent
| Bronze | Parquet	| Efficient columnar storage, schema enforcement | Parse, validate, enforce schema, quarantine bad records
| Silver | Parquet	| Cleansed analytics-ready data | Clean, enrich, deduplicate, derive business attributes
| Gold	| Parquet	| Aggregated KPIs for BI | Aggregate into KPIs and reporting datasets


---

# Data Lake Architecture

The project follows the Medallion Architecture.

## Raw

Immutable landing zone.

Contains exactly what devices produced.

No transformation.

---

## Bronze

Schema enforcement

Data validation

Metadata enrichment

Corrupt record isolation

Audit columns

Partition management

---

## Silver

Business validation

Deduplication

Data standardization

Unit normalization

Derived columns

Machine health indicators

---

## Gold

Business-ready datasets.

Examples:

- Plant KPIs
- Machine KPIs
- Sensor Performance
- Alert Statistics
- Equipment Health
- Production Analytics

---

# Alert Engine

The alert engine continuously evaluates incoming sensor readings.

Rules are metadata driven.

No thresholds are hardcoded.

Examples include:

- High Temperature
- Low Pressure
- Excessive Vibration
- Smoke Detection
- Sensor Offline
- Sensor Drift
- Consecutive Missing Readings

Alerts are classified as:

- INFO
- WARNING
- CRITICAL

| Column             | Description                  |
| ------------------ | ---------------------------- |
| alert_id           | Unique ID                    |
| device_id          | Sensor                       |
| machine_id         | Machine                      |
| plant_id           | Plant                        |
| alert_timestamp    | Detection time               |
| alert_type         | HIGH_TEMP, SMOKE, etc.       |
| severity           | WARNING / CRITICAL           |
| measured_value     | Reading                      |
| threshold_value    | Trigger threshold            |
| rule_name          | Rule executed                |
| status             | OPEN / ACKNOWLEDGED / CLOSED |
| resolved_timestamp | Nullable                     |
| processing_job     | Traceability                 |



```json
Alerts Table

{
  "alert_id":"A-10493",

  "device_id":"TEMP-001",

  "machine_id":"MC-05",

  "timestamp":"...",

  "severity":"CRITICAL",

  "rule":"HIGH_PROCESS_TEMPERATURE",

  "measured_value":215,

  "threshold":210,

  "status":"OPEN"
}
```
---

# Data Quality

The platform validates:

- Schema correctness
- Required fields
- Duplicate events
- Invalid timestamps
- Invalid units
- Missing values
- Corrupted JSON
- Out-of-order events

Rejected records are stored separately for investigation.

---

# GitHub Actions

The project automatically generates synthetic telemetry using GitHub Actions.

Features include:

- Scheduled execution
- Manual execution
- Configurable runtime
- Batch uploads
- Automatic storage upload
- Pipeline summary generation

---

# Future Enhancements

Planned improvements include:

- Correlated machine failures
- Multi-sensor anomaly simulation
- Predictive maintenance
- Streaming ingestion
- Kafka integration
- Delta Lake
- Apache Spark processing
- Great Expectations validation
- ML-based anomaly detection
- Real-time dashboards

---

# Running Locally

Install dependencies

```
pip install -r requirements.txt
```

Run simulator

```
python simulate_sensors.py --dry-run
```

Generate production data

```
python simulate_sensors.py \
    --duration-minutes 5 \
    --batch-size 500 \
    --tick-seconds 30
```

---

# Project Highlights

✔ Metadata Driven

✔ Cloud Native

✔ Enterprise Data Lake

✔ Production Inspired

✔ Automated Data Generation

✔ Configurable Sensors

✔ Alert Engine

✔ Data Quality Validation

✔ Analytics Ready

---

# License

This project is intended for educational, portfolio and learning purposes.