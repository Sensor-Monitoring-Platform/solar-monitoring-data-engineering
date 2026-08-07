# Silver Layer Design

## Overview

The Silver layer is the second stage of the Medallion Architecture.

Its primary responsibility is to transform technically valid Bronze data into clean, standardized, enriched, and analytics-ready datasets.

Unlike the Bronze layer, which focuses on technical correctness, the Silver layer applies business-aware transformations while preserving the original sensor readings.

The Silver layer serves as the single source of truth for downstream analytics, alert generation, and Gold layer aggregations.

---

# Objectives

The Silver layer is responsible for:

- Data cleansing
- Data standardization
- Metadata enrichment
- Business rule validation
- Unit normalization
- Time normalization
- Duplicate removal
- Late-arriving data handling
- Missing value handling
- Sensor health derivation
- Machine state derivation
- Feature engineering
- Data quality scoring

The Silver layer **does not** generate KPIs or business dashboards.

---

# Input

The Silver layer consumes validated Bronze Parquet datasets.

```text
bronze/

year=YYYY/
    month=MM/
        day=DD/
            hour=HH/
                plant_id=...
                    *.parquet
```

---

# Output

Silver datasets are written as partitioned Parquet files.

```text
silver/

year=YYYY/
    month=MM/
        day=DD/
            plant_id=...
                *.parquet
```

---

# Primary Responsibilities

Unlike Bronze, Silver is responsible for improving data quality rather than validating structure.

Typical responsibilities include:

- Cleaning
- Enrichment
- Standardization
- Feature creation
- Business validations

---

# Metadata Enrichment

Join every telemetry record with metadata.

Sources

```text
devices.yml

sensor_config.yml
```

Additional columns added

| Column | Source |
|---------|--------|
| plant_name | devices.yml |
| production_line | devices.yml |
| machine_name | devices.yml |
| machine_type | devices.yml |
| manufacturer | devices.yml |
| sensor_unit | sensor_config.yml |
| safety_critical | sensor_config.yml |
| sampling_frequency | sensor_config.yml |

The goal is to eliminate future metadata lookups.

---

# Unit Standardization

All readings must use standardized units.

Example

Temperature

```text
°C
```

Pressure

```text
bar
```

Current

```text
A
```

Future firmware versions may introduce different units.

Example

```text
fahrenheit
```

↓

Converted into

```text
celsius
```

---

# Timestamp Standardization

Convert timestamps into UTC.

Generate additional columns

| Column |
|----------|
| event_date |
| event_hour |
| event_minute |
| event_week |
| event_month |
| event_year |
| day_of_week |

These improve analytical queries.

---

# Duplicate Removal

Bronze preserves duplicates.

Silver removes them.

Duplicate definition

```text
device_id

timestamp

reading_value
```

Only one valid record remains.

---

# Late Arriving Data

Late telemetry is accepted.

Records receive

```text
is_late_arrival
```

Late records continue downstream but are clearly identified.

---

# Missing Value Handling

Depending on sensor type.

Possible strategies

Temperature

```text
Forward Fill
```

Pressure

```text
Linear Interpolation
```

Smoke Alarm

```text
Leave NULL
```

Access Control

```text
Not Applicable
```

The strategy should be configurable.

---

# Outlier Validation

Outliers are identified using sensor metadata.

Example

Temperature

Normal

```text
60°C – 180°C
```

Incoming

```text
420°C
```

Result

```text
is_outlier = TRUE
```

The record is **not removed**.

Instead, it is flagged.

---

# Reading Status

Every record receives a derived status.

Possible values

```text
NORMAL

WARNING

CRITICAL

UNKNOWN
```

Derived from

```text
sensor_config.yml
```

Thresholds.

Example

Temperature

```
Normal

60–180
```

```
Warning

181–190
```

```
Critical

>190
```

---

# Sensor Health

A derived column indicating the overall health of the sensor.

Possible values

```text
GOOD

DRIFTING

OFFLINE

FLATLINE

UNSTABLE

UNKNOWN
```

Derived from

- Recent history
- Missing values
- Drift detection
- Flatline detection

---

# Machine Operating Condition

Silver derives the machine's current condition from multiple sensors.

Possible values

```text
NORMAL

WARNING

FAULT

EMERGENCY

MAINTENANCE
```

Example

High vibration

+

High current

↓

Machine Fault

---

# Feature Engineering

Additional derived columns

| Column | Description |
|----------|-------------|
| moving_average | Rolling average |
| rolling_stddev | Rolling standard deviation |
| delta_previous | Difference from previous reading |
| percent_change | Percentage change |
| reading_velocity | Rate of change |
| reading_acceleration | Second derivative |
| consecutive_nulls | Missing streak |
| consecutive_outliers | Outlier streak |

These features simplify downstream analytics.

---

# Time Window Features

Examples

Rolling

```text
5 minutes

15 minutes

1 hour
```

Generate

- Moving Average
- Moving Maximum
- Moving Minimum
- Rolling Standard Deviation

---

# Data Quality Score

Every record receives a quality score.

Example

```text
100

↓

Perfect record
```

```
80

↓

Late arrival
```

```
60

↓

Missing values
```

```
30

↓

Multiple quality issues
```

Useful for operational dashboards.

---

# Record Classification

Every record is categorized.

Possible values

```text
VALID

LATE

OUTLIER

MISSING

CORRUPTED

ESTIMATED
```

---

# Silver Schema

Existing Bronze columns

+

Derived columns

| Column |
|----------|
| plant_name |
| machine_name |
| machine_type |
| manufacturer |
| safety_critical |
| reading_status |
| sensor_health |
| machine_condition |
| moving_average |
| rolling_stddev |
| delta_previous |
| percent_change |
| reading_velocity |
| reading_acceleration |
| quality_score |
| is_outlier |
| is_late_arrival |
| processing_timestamp |

---

# Data Quality Rules

Silver enforces business-level quality.

Examples

✔ Remove duplicates

✔ Standardize timestamps

✔ Standardize units

✔ Normalize null values

✔ Flag outliers

✔ Derive status

✔ Calculate rolling metrics

---

# Partition Strategy

Partition using

```text
year

month

day

plant_id
```

Avoid partitioning by device.

---

# Processing Metrics

Each pipeline execution publishes

| Metric | Description |
|----------|-------------|
| records_read | Bronze input |
| records_written | Silver output |
| duplicates_removed | Count |
| outliers_detected | Count |
| missing_values_filled | Count |
| late_records | Count |
| average_quality_score | Average |
| processing_duration | Runtime |

---

# Silver Processing Workflow

```text
                 Bronze Dataset
                       │
                       ▼
             Read Bronze Parquet
                       │
                       ▼
             Remove Duplicates
                       │
                       ▼
          Join Metadata Repository
                       │
                       ▼
       Standardize Units & Timestamps
                       │
                       ▼
         Handle Missing Values
                       │
                       ▼
           Detect Outliers
                       │
                       ▼
      Derive Reading Status
                       │
                       ▼
      Calculate Rolling Features
                       │
                       ▼
      Derive Sensor Health
                       │
                       ▼
     Derive Machine Condition
                       │
                       ▼
       Calculate Quality Score
                       │
                       ▼
            Write Silver Layer
```

---

# Responsibilities

The Silver layer **must**

- Produce clean analytical datasets
- Enrich telemetry with metadata
- Standardize units
- Normalize timestamps
- Remove duplicates
- Derive machine and sensor status
- Compute analytical features
- Publish processing metrics

The Silver layer **must not**

- Aggregate business KPIs
- Generate dashboards
- Produce executive summaries
- Trigger business alerts
- Store alert history

Those responsibilities belong to the Gold layer and the Alert Engine.

---

# Relationship with Other Components

```text
                    Bronze Layer
                         │
                         ▼
                   Silver Layer
                (Clean & Enriched Data)
                   ┌───────────┴────────────┐
                   ▼                        ▼
             Gold Layer              Alert Engine
                   │                        │
                   └───────────┬────────────┘
                               ▼
                        Power BI Dashboard
```

The Silver layer acts as the canonical, business-ready dataset for both the Gold layer and the Alert Engine, ensuring that all downstream consumers work from a consistent, high-quality source of telemetry.