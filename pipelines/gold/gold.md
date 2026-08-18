Absolutely. Here is a **short, developer-oriented `README.md`** you can put directly inside `pipelines/gold/`. It focuses on *what we decided, how it works, and why*, rather than documenting every implementation detail.

````md
# Gold Layer

The Gold layer converts the cleaned and enriched **Silver sensor data** into
a business-oriented dimensional model for Power BI.

The overall flow is:

```text
Bronze → Silver → Gold → Power BI
              │
              └──────→ Alert Engine (independent)
````

The Alert Engine is intentionally separate from the Medallion pipeline.
It handles real-time threshold detection, notifications and alert history.
Its `alerts_fact_table` will be linked with Gold tables later in Power BI,
rather than being produced by this Gold pipeline.

---

## Gold Approach

Gold is not another cleaning layer. Silver has already handled:

* schema enforcement and metadata enrichment
* invalid/missing data handling
* late-arrival detection
* outlier detection
* rolling statistics and derived sensor metrics

Gold focuses on **business aggregation and dimensional modeling**.

The model uses:

```text
Dimensions
├── dim_sensor
└── dim_date

Facts
├── fact_sensor_reading_daily
├── fact_sensor_health_daily
└── fact_machine_daily
```

Each fact has a clearly defined grain to avoid duplicated data and
incorrect Power BI aggregations.

---

## Dimensions

### dim_sensor

**Grain:** one row per sensor.

It contains the flattened hierarchy:

```text
Plant
  └── Production Line
        └── Machine
              └── Sensor
```

It is built mainly from:

```text
devices.yaml
sensor_config.yaml
```

It contains descriptive and configuration information such as:

* `sensor_key`
* `device_id`
* `device_type`
* plant / line / machine information
* manufacturer and firmware
* sensor installation and calibration dates
* unit and sampling frequency
* safety-critical flag
* normal / warning / critical thresholds

`device_id` is the natural/business key, while `sensor_key` is the
surrogate key used by facts.

The first implementation follows SCD Type 1. The structure leaves room
for SCD Type 2 later if sensor history becomes important.

---

### dim_date

**Grain:** one row per calendar date.

It provides standard calendar attributes for Power BI:

```text
date_key
full_date
year
quarter
month
month_name
week
day
day_name
is_weekend
```

Using a date dimension keeps time-based reporting consistent across all
Gold facts.

---

## Facts

### fact_sensor_reading_daily

**Grain:** one sensor per day.

It is the main analytical fact.

It aggregates Silver readings into measures such as:

```text
reading_count
avg_reading_value
min_reading_value
max_reading_value

normal_count
warning_count
critical_count
outlier_count
late_arrival_count
```

Additional useful statistical metrics from Silver can also be aggregated.

We prefer storing **counts** rather than percentages. Power BI can then
calculate reusable measures such as:

```text
critical_percentage
outlier_percentage
data_completeness_percentage
```

---

### fact_sensor_health_daily

**Grain:** one sensor per day.

This focuses on **sensor reliability**, rather than the physical reading.

Important metrics include:

```text
expected_reading_count
actual_reading_count
missing_reading_count
data_completeness_percentage
outlier_count
late_arrival_count
health_score
```

For periodic sensors:

```text
expected_readings = 86400 / sampling_frequency_seconds
```

`access_control` is event-driven and has a sampling frequency of `0`, so
normal frequency-based completeness is not applied to it.

Sensor availability and sensor health are treated as different concepts.

---

### fact_machine_daily

**Grain:** one machine per day.

It aggregates sensor information to the machine level.

Examples:

```text
sensor_count
active_sensor_count
total_readings
warning_readings
critical_readings
outlier_readings
avg_sensor_health_score
critical_sensor_count
unhealthy_sensor_count
```

Sensor-specific metrics such as temperature, vibration, pressure and
current are calculated only when that sensor type exists.

Missing sensor types remain `NULL`, not `0`.

---

## Alert Engine Separation

The Alert Engine is **not part of the current Gold transformation**.

Its responsibility is real-time operation:

```text
sensor event
    ↓
Alert Engine
    ├── threshold evaluation
    ├── emergency notification
    ├── deduplication/cooldown
    ├── alert lifecycle
    └── alert history
```

For example, it may inspect only the relevant measurement for a sensor:

```text
temperature → temperature value
pressure    → pressure value
smoke       → smoke value
vibration   → vibration value
```

Rows that are missing or invalid for the alert rule are not treated as
alerts simply because they exist in the sensor stream.

The Alert Engine independently stores its alert fact/history in
Hugging Face.

We will later connect:

```text
Gold facts
    +
Alert Engine alerts_fact
    ↓
Power BI data model
```

This keeps real-time alerting separate from daily analytical processing.

---

## Important Logic Decisions

### Outliers

Gold does not silently remove Silver outliers.

They remain visible through:

```text
outlier_count
```

If a KPI excludes outliers, that definition must be explicit.

### Critical readings

Critical readings are retained and counted because they are important for
historical analysis.

### Machine operating condition

`machine_operating_condition` from Silver is currently treated as a
**derived/proxy condition**, not a confirmed machine failure state.

A proper machine-state engine can be introduced later.

### Unsupported manufacturing KPIs

We do not calculate:

```text
OEE
production efficiency
yield
throughput
capacity utilization
maintenance cost
```

because the current sensor data does not contain the required production
or maintenance information.

---

## Metadata Loader

`metadata_loader.py` reads:

```text
devices.yaml
sensor_config.yaml
```

and flattens the YAML hierarchy into metadata usable by Gold.

Two main lookups are produced:

```text
device_lookup
    device_id → plant / line / machine / sensor metadata

sensor_lookup
    device_type → unit / thresholds / sampling / safety configuration
```

The metadata is the **configuration authority**, while Silver remains the
authority for observed sensor events.

---

## Gold Code Structure

The implementation is intentionally split into small responsibilities:

```text
schemas.py
    ↓
metadata_loader.py
    ↓
dimensions.py
    ├── dim_sensor
    └── dim_date
    ↓
facts.py
    ├── fact_sensor_reading_daily
    ├── fact_sensor_health_daily
    └── fact_machine_daily
    ↓
validations.py
    ↓
main.py
    ↓
Hugging Face Gold storage
    ↓
Gold checkpoint
```

`main.py` is only the orchestration layer.

The Silver reader will be implemented separately using
`SILVER_INPUT_PATH` from `config.py`, reading the Silver Delta output.

---

## Validation

Before Gold is written, we validate:

* required columns exist
* fact grains are unique
* `sensor_key` exists in `dim_sensor`
* `date_key` exists in `dim_date`
* duplicate business rows are not created

The goal is idempotency:

> Running Gold again on the same Silver input should not create duplicate
> business records.

---

## Storage

Gold outputs will be organized as:

```text
gold/
├── dimensions/
│   ├── dim_sensor
│   └── dim_date
│
└── facts/
    ├── fact_sensor_reading_daily
    ├── fact_sensor_health_daily
    └── fact_machine_daily
```

They will ultimately be stored in Hugging Face using the configured
`HF_GOLD_PREFIX`.

---

## Gold Checkpoint

Gold maintains:

```text
checkpoint/gold_watermark.json
```

The checkpoint records the successful processing state.

It must only be updated **after all required Gold outputs are written
successfully**.

If Gold fails halfway, the checkpoint must not advance so the run can be
retried safely.

---

## Final Model

The current Gold scope is intentionally small:

```text
DIM_SENSOR
DIM_DATE

FACT_SENSOR_READING_DAILY
FACT_SENSOR_HEALTH_DAILY
FACT_MACHINE_DAILY
```

The Alert Engine remains independent and will be connected to this model
later through Power BI.

The main idea is:

```text
Silver = cleaned and enriched sensor events

Gold = stable business model for historical analysis

Alert Engine = real-time operational alerting

Power BI = semantic model, relationships and business visualization
```

