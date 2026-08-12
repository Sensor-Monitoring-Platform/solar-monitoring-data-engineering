Absolutely. Below is the **complete Markdown document** capturing the alert-engine architecture and reasoning we discussed, structured so you can directly save it as something like:

`docs/alert_engine_design.md`

````md
# 🚨 Alert Engine — Architecture & Design

## Solar Panel Manufacturing Sensor Monitoring System

---

## 1. Purpose

The Alert Engine is the **real-time safety and operational monitoring component** of the sensor monitoring platform.

Its responsibility is fundamentally different from the Medallion data pipeline.

The system has two parallel paths after sensor data is generated/ingested:

```text
                         Sensor Data
                              │
                              ▼
                    ┌──────────────────┐
                    │  Raw Sensor Data │
                    └────────┬─────────┘
                             │
               ┌─────────────┴─────────────┐
               │                           │
               ▼                           ▼
       ┌────────────────┐         ┌──────────────────┐
       │  Alert Engine  │         │ Medallion Pipeline│
       │                │         │                  │
       │ Low latency    │         │ Data quality     │
       │ Safety first   │         │ Transformation   │
       │ Event driven   │         │ Historical data  │
       └───────┬────────┘         └────────┬─────────┘
               │                           │
               ▼                           ▼
        Immediate Alerts             Bronze → Silver
               │                           │
               ▼                           ▼
        Backend / Frontend               Gold
               │                           │
               ▼                           ▼
        Operator Action             Analytics / BI
````

The Alert Engine should **not wait for the Medallion pipeline to finish**.

The factory safety system and the analytical data platform have different priorities.

---

# 2. Core Design Principle

The most important principle is:

> **The Alert Engine is optimized for low latency, while the Medallion pipeline is optimized for data quality, durability, transformation, and analytics.**

Therefore:

### Alert Engine

Cares primarily about:

* Immediate threshold violations
* Critical sensor values
* Sensor malfunction
* Sensor silence / missing readings
* Safety-critical sensors
* Rapidly changing values
* Machine-level abnormal conditions
* Alert severity
* Alert lifecycle
* Notification latency

### Medallion Pipeline

Cares primarily about:

* Schema enforcement
* Schema evolution
* Data quality
* Corrupt records
* Metadata enrichment
* Deduplication
* Historical storage
* Data transformations
* Aggregations
* Analytics
* BI
* Auditing

The Alert Engine should **not depend on Silver or Gold** for initial alert generation.

---

# 3. Important Correction: Batch Input Does NOT Mean Batch Alert Processing

The generated sensor data may arrive as JSON files containing many records:

```text
batch_001.json
batch_002.json
batch_003.json
```

and records may not be ordered:

```text
05:20:10 vibration
05:18:34 temperature
05:19:05 smoke
05:17:58 pressure
05:20:02 temperature
```

This does not mean the Alert Engine should process the entire batch first.

Instead:

```text
Batch/File
   │
   ▼
Read record
   │
   ▼
Normalize
   │
   ▼
Evaluate rules
   │
   ├── No alert ───────► Continue
   │
   └── Alert ──────────► Emit immediately
                              │
                              ├── Backend
                              ├── WebSocket
                              └── Alert Store
```

The engine can process records incrementally.

---

# 4. WebSocket Is NOT the Alert Engine

This distinction is extremely important.

A WebSocket should generally be used for:

> **Delivering alerts to connected frontend clients in real time.**

It should not be responsible for:

> Detecting alerts.

The architecture should instead be:

```text
Sensor Data
    │
    ▼
Alert Engine
    │
    ├── Rule Evaluation
    │
    ├── State Tracking
    │
    ├── Alert Creation
    │
    ▼
Alert/Event Bus or Backend
    │
    ├── Persist Alert
    │
    ├── Trigger notifications
    │
    └── WebSocket
          │
          ▼
       Frontend
```

So:

```text
Alert Engine ≠ WebSocket
```

WebSocket is only one **delivery mechanism**.

---

# 5. Recommended Alert Architecture

For the current project, the recommended architecture is:

```text
                         RAW SENSOR EVENTS
                                │
                                ▼
                     ┌─────────────────────┐
                     │   Alert Ingestion   │
                     └──────────┬──────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │ Event Normalization │
                     └──────────┬──────────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │ Metadata / Rule      │
                    │ Lookup                │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Rule Evaluation       │
                    │ Engine                │
                    └──────────┬───────────┘
                               │
                  ┌────────────┴─────────────┐
                  │                          │
             No anomaly                  Anomaly
                  │                          │
                  ▼                          ▼
              Continue               Alert State Manager
                                             │
                                             ▼
                                    Alert/Event Object
                                             │
                         ┌───────────────────┼───────────────────┐
                         │                   │                   │
                         ▼                   ▼                   ▼
                     Database          WebSocket           Notification
                         │                   │                   │
                         ▼                   ▼                   ▼
                    Alert History        Frontend          Email/SMS/etc.
```

---

# 6. What Should the Alert Engine Consume?

The Alert Engine should consume the **raw/generated sensor event**, not the final Gold record.

A typical event is:

```json
{
    "device_id": "TEMP-GGN01-L01-MC01-001",
    "device_type": "temperature_process",
    "plant_id": "PLT-GGN-01",
    "machine_id": "MC-ASM-01",
    "timestamp": "2026-08-04T05:18:34.215849+00:00",
    "reading": {
        "value": 117.7582,
        "unit": "celsius"
    },
    "firmware_version": "1.3.2",
    "quality_flag": "OK",
    "ingestion_timestamp": "2026-08-04T05:18:34.215883+00:00"
}
```

The Alert Engine can also consume additional operational metadata.

For example:

```json
{
    "device_id": "...",
    "device_type": "...",
    "plant_id": "...",
    "machine_id": "...",
    "timestamp": "...",
    "reading": {
        "value": 215.5,
        "unit": "celsius"
    },
    "quality_flag": "OK"
}
```

---

# 7. What Should the Alert Engine Ignore?

The Alert Engine does not need to perform every operation that the Medallion pipeline performs.

For example, it does not need to wait for:

* Silver transformations
* Gold aggregations
* Historical partitioning
* BI calculations
* Long-running data quality jobs
* Complex analytical transformations

However, it **must not blindly ignore data quality either**.

It should perform the minimum validation required to safely interpret an event.

For example:

```text
Does device_id exist?
Does device_type exist?
Can the reading be interpreted?
Is the value numeric when numeric data is expected?
Is the sensor known?
Is the sensor configuration available?
```

This is lightweight **event validation**, not full Medallion data validation.

---

# 8. Alert Engine vs Medallion Validation

This is an important architectural distinction.

## Medallion

```text
Raw
 │
 ▼
Bronze
 │
 ├── schema validation
 ├── type validation
 ├── required fields
 ├── corrupt record handling
 ├── metadata validation
 └── quarantine
```

## Alert Engine

```text
Event
 │
 ▼
Can I safely interpret this event?
 │
 ├── NO → discard/log/invalid-event stream
 │
 └── YES
       │
       ▼
    Evaluate safety rules
```

The Alert Engine should not reject an event simply because it has some analytical-quality problem that does not prevent alert evaluation.

For example:

```text
temperature = 215°C
quality_flag = "WARNING"
```

Even if the quality flag is not perfect, the value itself may represent a real emergency.

Therefore:

> **Never let unnecessary data-quality processing delay a safety-critical alert.**

---

# 9. Rule Evaluation

The engine should NOT become a giant collection of hardcoded `if/else` statements.

Avoid:

```python
if device_type == "temperature_process":
    if value > 210:
        ...

elif device_type == "vibration":
    if value > 10:
        ...

elif device_type == "pressure":
    if value > 10.5:
        ...
```

This becomes difficult to maintain.

Instead, the existing `sensors.yml` should act as the **rule configuration source**.

For example:

```yaml
temperature_process:
  warning_threshold:
    high: 190.0

  critical_threshold:
    high: 210.0
```

The Alert Engine can dynamically load these rules.

---

# 10. Rule Evaluation Model

For every numeric event:

```text
device_type
      │
      ▼
Load sensor configuration
      │
      ▼
Extract reading.value
      │
      ▼
Compare with thresholds
      │
      ├── Normal
      │
      ├── Warning
      │
      └── Critical
```

Example:

```text
temperature_process

value = 195

warning_high = 190
critical_high = 210
```

Result:

```text
WARNING
```

If:

```text
value = 215
```

Result:

```text
CRITICAL
```

---

# 11. Alert Severity

The engine should use standardized severity levels.

Recommended initial model:

```text
INFO
WARNING
CRITICAL
EMERGENCY
```

For example:

### INFO

Normal operational notification.

### WARNING

Sensor is outside normal operating conditions but immediate emergency action may not be required.

### CRITICAL

Dangerous operating condition.

### EMERGENCY

Immediate safety threat.

For the current project:

```text
normal_range
       │
       ▼
     NORMAL

warning_threshold
       │
       ▼
    WARNING

critical_threshold
       │
       ▼
    CRITICAL
```

An additional `EMERGENCY` level can later be used for domain-specific conditions such as:

```text
smoke > emergency threshold
```

or combinations such as:

```text
high temperature + smoke
```

---

# 12. Safety-Critical Sensors

The metadata already contains:

```yaml
is_safety_critical: true
```

This is extremely useful.

Current safety-critical sensors include:

```text
vibration
pressure
smoke_alarm
```

These sensors should receive special treatment.

For example:

```text
Normal sensor
      │
      ▼
Standard processing

Safety-critical sensor
      │
      ▼
Higher priority
      │
      ▼
Immediate alert
```

A safety-critical event should never wait behind a large queue of low-priority events.

---

# 13. Example: Smoke Detection

Current metadata:

```yaml
smoke_alarm:
  normal_range:
    min: 0.0
    max: 50.0

  warning_threshold:
    high: 100.0

  critical_threshold:
    high: 200.0

  is_safety_critical: true
```

Suppose:

```json
{
    "device_id": "SMOKE-CHN01-L01-MC01-001",
    "device_type": "smoke_alarm",
    "plant_id": "PLT-CHN-01",
    "machine_id": "MC-PKG-01",
    "reading": {
        "value": 245.3,
        "unit": "ppm"
    }
}
```

The engine produces:

```text
device_id:
SMOKE-CHN01-L01-MC01-001

machine:
MC-PKG-01

value:
245.3 ppm

severity:
CRITICAL / EMERGENCY

reason:
Smoke concentration exceeded critical threshold
```

---

# 14. Missing Sensor Data

This is another responsibility of the Alert Engine.

The metadata already provides:

```yaml
sampling_frequency_seconds: 60
offline_grace_period_seconds: 120
```

This allows the engine to detect missing sensors.

For example:

```text
Expected reading every 60 seconds.

Grace period = 120 seconds.
```

If the last reading was:

```text
10:00:00
```

and current time becomes:

```text
10:02:30
```

then:

```text
150 seconds since last reading
```

which exceeds:

```text
120 second grace period
```

Therefore:

```text
SENSOR_OFFLINE
```

can be generated.

---

# 15. Important: Missing Data Is Not Detected by Processing One Row

A missing reading cannot be detected simply by looking at the current row.

The engine needs **state**.

For example:

```text
device_id → last_seen_timestamp
```

Example:

```python
last_seen = {
    "TEMP-GGN01-L01-MC01-001": "10:00:00",
    "VIB-GGN01-L01-MC01-001": "10:00:10"
}
```

Every incoming event updates the state.

```text
Incoming event
      │
      ▼
device_id
      │
      ▼
update last_seen
```

Separately, a lightweight monitoring process checks:

```text
current_time - last_seen
```

against:

```text
offline_grace_period_seconds
```

This is much better than trying to infer missing data from the batch itself.

---

# 16. Missing Data Percentage

The idea of:

> "60% values are missing"

is useful, but should be implemented carefully.

There are two different conditions.

## Sensor Offline

No readings received for a defined period.

Example:

```text
last_seen = 10:00
current_time = 10:03
grace = 120 sec

→ OFFLINE
```

## Data Completeness Problem

Over a longer time window:

```text
Expected:
120 readings

Received:
48 readings
```

Completeness:

```text
48 / 120 = 40%
```

Missing:

```text
60%
```

This should generate a different alert:

```text
SENSOR_DATA_QUALITY_DEGRADED
```

rather than pretending the sensor is necessarily physically offline.

---

# 17. State Management

The Alert Engine therefore needs state.

At minimum:

```text
device_id
    │
    ├── last_seen_timestamp
    ├── current alert state
    ├── previous value
    ├── previous severity
    └── active alerts
```

Example:

```python
sensor_state = {
    "TEMP-GGN01-L01-MC01-001": {
        "last_seen": "...",
        "last_value": 117.5,
        "current_status": "NORMAL",
        "active_alert": False
    }
}
```

For a production system, this state should eventually move outside process memory.

Possible technologies include:

```text
Redis
Kafka Streams state store
Flink state
Database
```

For the current PoC, in-memory state is acceptable.

---

# 18. Avoiding Duplicate Alerts

Suppose a sensor sends:

```text
215°C
216°C
218°C
219°C
220°C
```

The engine should NOT necessarily generate:

```text
ALERT
ALERT
ALERT
ALERT
ALERT
```

every second.

Instead:

```text
215°C
  │
  ▼
CRITICAL ALERT CREATED
  │
  ├── 216°C → update existing alert
  ├── 218°C → update existing alert
  ├── 219°C → update existing alert
  └── 220°C → update existing alert
```

The alert should have a lifecycle.

---

# 19. Alert Lifecycle

Recommended states:

```text
NORMAL
   │
   ▼
TRIGGERED
   │
   ▼
ACTIVE
   │
   ▼
RECOVERED
```

Potentially:

```text
TRIGGERED
    │
    ├── ACKNOWLEDGED
    │
    └── ACTIVE
```

Example:

```text
Temperature = 215°C
        │
        ▼
Alert CREATED
        │
        ▼
Alert ACTIVE
        │
        ▼
Operator acknowledges
        │
        ▼
ACKNOWLEDGED
        │
        ▼
Temperature returns to normal
        │
        ▼
RECOVERED
```

---

# 20. Alert Object

The Alert Engine should produce a standardized alert object.

Example:

```json
{
    "alert_id": "ALT-20260804-000001",

    "device_id": "TEMP-GGN01-L02-MC05-001",
    "device_type": "temperature_process",

    "plant_id": "PLT-GGN-01",
    "machine_id": "MC-LAM-05",

    "event_timestamp": "2026-08-04T05:20:12Z",
    "alert_timestamp": "2026-08-04T05:20:12.010Z",

    "metric": "temperature",

    "value": 215.42,
    "unit": "celsius",

    "threshold": 210.0,

    "severity": "CRITICAL",

    "alert_type": "THRESHOLD_BREACH",

    "status": "ACTIVE",

    "message": "Process temperature exceeded critical threshold",

    "rule_version": "1.0",

    "source": "alert_engine"
}
```

This object is what should be sent to:

```text
Backend
Database
WebSocket
Notification system
```

---

# 21. Where Should Alert History Be Stored?

The Alert Engine should persist alerts independently of Gold.

This is important.

The architecture should not be:

```text
Alert Engine
      │
      ▼
Wait for Gold
      │
      ▼
Save alert
```

That defeats the purpose of real-time alerts.

Instead:

```text
Sensor Event
      │
      ▼
Alert Engine
      │
      ├──────────────► Alert Store
      │
      └──────────────► WebSocket
```

The Alert Store becomes the operational source of truth for alert history.

---

# 22. Alert Database

A simple relational table could be:

```text
alerts
------------------------------------------------
alert_id
device_id
device_type
plant_id
machine_id
event_timestamp
alert_timestamp
alert_type
severity
value
unit
threshold
status
message
created_at
updated_at
resolved_at
acknowledged_at
rule_version
```

Example:

```text
ALT-001
TEMP-GGN01-L02-MC05-001
PLT-GGN-01
MC-LAM-05
CRITICAL
215.42
210
ACTIVE
```

---

# 23. How Alert Data Eventually Connects to Gold

The Alert Engine and Medallion pipeline are independent.

But they should share common identifiers.

For example:

```text
device_id
machine_id
plant_id
event_timestamp
```

This allows later reconciliation.

Architecture:

```text
                    SENSOR EVENT
                         │
             ┌───────────┴───────────┐
             │                       │
             ▼                       ▼
       ALERT ENGINE             MEDALLION
             │                       │
             ▼                       ▼
        ALERT STORE             BRONZE
                                     │
                                     ▼
                                   SILVER
                                     │
                                     ▼
                                    GOLD
             │                       │
             └───────────┬───────────┘
                         │
                         ▼
                 RECONCILIATION /
                 ANALYTICS / AI
```

---

# 24. Future AI Validation

The future AI layer should NOT be responsible for the initial emergency decision.

For example:

```text
Smoke = 250 ppm
```

The deterministic rule engine should immediately generate:

```text
CRITICAL
```

AI should not be placed before that decision.

Instead:

```text
Rule Engine
     │
     ▼
Immediate Alert
     │
     ▼
Historical Analysis
     │
     ▼
AI Validation
```

AI can later answer questions such as:

```text
Was this alert legitimate?

Was it a false positive?

Did the sensor malfunction?

How many alerts were generated?

How many were acknowledged?

How quickly was the alert generated?

How quickly did an operator respond?

Was the threshold too sensitive?

Could a combination of sensor signals have predicted the failure earlier?
```

---

# 25. Alert Performance Metrics

The system should eventually track:

### Detection latency

```text
alert_timestamp - event_timestamp
```

Example:

```text
Sensor event:
10:00:00.000

Alert generated:
10:00:00.120

Detection latency:
120 ms
```

### Delivery latency

```text
frontend_received - alert_created
```

### Acknowledgement latency

```text
acknowledged_at - alert_timestamp
```

### Resolution latency

```text
resolved_at - alert_timestamp
```

### False-positive rate

```text
false_alerts / total_alerts
```

### Sensor availability

```text
received_readings / expected_readings
```

---

# 26. Why Millions of `if/else` Statements Are Not the Main Problem

The concern that:

> "If every row executes many if/else conditions, the system will become slow."

is valid conceptually, but ordinary rule evaluation itself is usually extremely cheap.

The real bottlenecks are more likely to be:

```text
Network I/O
Database writes
Message queues
Serialization
Frontend delivery
Lock contention
```

not:

```python
if value > threshold:
```

Therefore, the design should optimize the architecture rather than trying to eliminate simple comparisons.

---

# 27. Rule Lookup Should Be O(1)

Instead of scanning all sensor configurations for every event:

```python
for sensor in all_sensors:
    if sensor.device_type == event.device_type:
        ...
```

build a dictionary once:

```python
rules = {
    "temperature_process": {...},
    "temperature_ambient": {...},
    "vibration": {...},
    "pressure": {...},
    "current_draw": {...},
    "smoke_alarm": {...}
}
```

Then:

```python
rule = rules[event["device_type"]]
```

This is effectively constant-time lookup.

---

# 28. Device Lookup Should Also Be O(1)

Similarly:

```python
devices = {
    "TEMP-GGN01-L01-MC01-001": {
        "plant_id": "PLT-GGN-01",
        "machine_id": "MC-ASM-01"
    }
}
```

Then:

```python
device = devices[event["device_id"]]
```

No repeated traversal of the YAML hierarchy.

---

# 29. Recommended Processing Model

For the current project:

```text
             JSON / Batch Input
                     │
                     ▼
              Event Reader
                     │
                     ▼
              Event Queue
                     │
                     ▼
          ┌─────────────────────┐
          │ Alert Workers       │
          │                     │
          │ Worker 1            │
          │ Worker 2            │
          │ Worker 3            │
          │ ...                 │
          └──────────┬──────────┘
                     │
                     ▼
              Rule Evaluation
                     │
          ┌──────────┴───────────┐
          │                      │
       Normal                  Alert
          │                      │
          ▼                      ▼
       Continue            Alert Manager
                                  │
                     ┌────────────┼────────────┐
                     ▼            ▼            ▼
                  Database     WebSocket   Notification
```

This architecture allows the processing workload to scale horizontally.

---

# 30. Event Queue

For a production-grade architecture, a message broker is preferable.

Examples:

```text
Kafka
Redpanda
RabbitMQ
Redis Streams
AWS Kinesis
Azure Event Hubs
```

For this project, Kafka is particularly relevant because this is an IoT/event-streaming use case.

Conceptually:

```text
Sensor Data
    │
    ▼
Kafka Topic
    │
    ├── Alert Engine Consumer
    │
    └── Medallion Consumer
```

This gives true decoupling.

---

# 31. Ideal Future Architecture

The mature version of the system could look like:

```text
                    ┌──────────────────────┐
                    │ Sensor / Generator   │
                    └──────────┬───────────┘
                               │
                               ▼
                         Kafka Topic
                               │
                 ┌─────────────┴──────────────┐
                 │                            │
                 ▼                            ▼
          Alert Engine                 Medallion Pipeline
                 │                            │
        ┌────────┼────────┐                   ▼
        │        │        │                Bronze
        ▼        ▼        ▼                   │
     Rules    State    Alert Store            ▼
     Engine   Store                       Silver
        │                                     │
        ▼                                     ▼
   Alert Event                              Gold
        │                                     │
   ┌────┴────┐                         ┌──────┴──────┐
   ▼         ▼                         ▼             ▼
WebSocket  Notifications             BI          Analytics
   │
   ▼
Frontend
```

---

# 32. Current Project vs Production Architecture

The project does not need to implement everything immediately.

## Phase 1 — Current PoC

Use:

```text
Python
YAML metadata
JSON files
In-memory state
Simple rule engine
FastAPI
WebSocket
SQLite/PostgreSQL
```

Architecture:

```text
JSON
 │
 ├──► Alert Engine ──► DB
 │          │
 │          └────────► WebSocket ──► Frontend
 │
 └──► Medallion
```

---

# 33. Phase 2 — Streaming

Introduce:

```text
Kafka
```

Architecture:

```text
Generator
    │
    ▼
Kafka
    │
    ├──► Alert Engine
    │
    └──► Medallion
```

---

# 34. Phase 3 — Distributed Alert Engine

Introduce:

```text
multiple workers
external state store
distributed event processing
```

Example:

```text
Kafka
 │
 ├── Worker 1
 ├── Worker 2
 ├── Worker 3
 └── Worker 4
```

---

# 35. Phase 4 — AI / Analytics

Add:

```text
Alert history
      │
      ▼
Feature engineering
      │
      ▼
ML / AI
      │
      ├── False-positive detection
      ├── Anomaly validation
      ├── Predictive maintenance
      └── Threshold optimization
```

---

# 36. Important Design Decision: Deterministic Rules First

For a safety system:

```text
Deterministic rule
        ↓
Immediate decision
        ↓
Alert
```

is preferable to:

```text
Sensor value
        ↓
AI model
        ↓
Decision
        ↓
Alert
```

because the latter introduces:

* Model latency
* Model uncertainty
* Model failures
* Explainability issues
* Training-data dependency

AI can enhance the system later, but should not replace deterministic safety rules.

---

# 37. Handling Corrupted Data

The generator will eventually introduce corrupted records.

Examples:

```json
{
    "device_id": null,
    "reading": {
        "value": "INVALID"
    }
}
```

or:

```json
{
    "device_type": "unknown_sensor"
}
```

The Alert Engine should have a lightweight invalid-event path:

```text
Incoming Event
      │
      ▼
Basic Parsing
      │
      ├── Invalid ──► Invalid Event Stream / Log
      │
      └── Valid
            │
            ▼
       Rule Evaluation
```

The Alert Engine should not spend significant time trying to repair corrupt data.

The Medallion pipeline is responsible for detailed data-quality processing and quarantine.

---

# 38. Example End-to-End Scenario

Suppose:

```text
Machine:
MC-PKG-01

Sensors:
Temperature
Smoke
```

At:

```text
10:00:00
```

temperature:

```text
30°C
```

No alert.

At:

```text
10:00:10
```

temperature:

```text
39°C
```

Potential warning depending on configured rule.

At:

```text
10:00:20
```

smoke:

```text
130 ppm
```

Result:

```text
WARNING
```

At:

```text
10:00:30
```

smoke:

```text
240 ppm
```

Result:

```text
EMERGENCY
```

The system immediately produces:

```json
{
    "alert_id": "ALT-...",
    "device_id": "SMOKE-CHN01-L01-MC01-001",
    "plant_id": "PLT-CHN-01",
    "machine_id": "MC-PKG-01",
    "severity": "EMERGENCY",
    "alert_type": "THRESHOLD_BREACH",
    "value": 240,
    "threshold": 200,
    "status": "ACTIVE"
}
```

Then:

```text
Alert Engine
     │
     ├──► PostgreSQL
     │
     ├──► WebSocket
     │
     └──► Notification service
```

The frontend immediately displays:

```text
🚨 EMERGENCY

Plant:
Chennai Production Facility

Machine:
Carton Sealer C1

Sensor:
Smoke Alarm

Value:
240 ppm

Threshold:
200 ppm
```

---

# 39. Key Principle About Historical Storage

The Alert Engine should write an alert as soon as it is generated.

Do not wait for:

```text
Bronze → Silver → Gold
```

because the alert itself is an operational event.

Later, the Medallion pipeline can ingest the alert history as another dataset if required.

For example:

```text
Alert Store
     │
     ▼
alert_history
     │
     ▼
Bronze
     │
     ▼
Silver
     │
     ▼
Gold
```

This allows alert analytics without coupling real-time detection to the analytical pipeline.

---

# 40. Final Recommended Architecture

For this project, the clean architecture is:

```text
                         SENSOR DATA
                              │
                              ▼
                         Event Source
                              │
                              ▼
                       ┌──────────────┐
                       │ Message/Event│
                       │    Queue     │
                       └──────┬───────┘
                              │
                ┌─────────────┴──────────────┐
                │                            │
                ▼                            ▼
        ┌────────────────┐          ┌─────────────────┐
        │  ALERT ENGINE  │          │    MEDALLION    │
        └───────┬────────┘          └────────┬────────┘
                │                            │
        ┌───────┴────────┐                  ▼
        │                │               BRONZE
        ▼                ▼                  │
   Rule Engine       State Manager          ▼
        │                │               SILVER
        └───────┬────────┘                  │
                │                           ▼
                ▼                          GOLD
          Alert Manager                     │
                │                     ┌─────┴─────┐
        ┌───────┼────────┐             ▼           ▼
        │       │        │            BI        Analytics
        ▼       ▼        ▼
       DB    WebSocket  Notification
               │
               ▼
            FRONTEND
```

---

# 41. What We Should Implement Now

Do NOT jump directly to Kafka, AI, distributed workers, etc.

For the current PoC, implement the Alert Engine in this order:

### Step 1 — Metadata loader

Load:

```text
devices.yml
sensors.yml
```

and build O(1) lookup dictionaries.

---

### Step 2 — Event parser

Read JSON sensor events.

---

### Step 3 — Lightweight event validation

Check:

```text
device_id
device_type
machine_id
plant_id
timestamp
reading
```

Only perform validation required to interpret the event.

---

### Step 4 — Rule engine

Evaluate:

```text
normal
warning
critical
emergency
```

based on sensor metadata.

---

### Step 5 — Sensor state manager

Track:

```text
last_seen
last_value
current_status
active_alert
```

---

### Step 6 — Missing sensor detection

Use:

```text
sampling_frequency_seconds
offline_grace_period_seconds
```

to detect:

```text
SENSOR_OFFLINE
```

---

### Step 7 — Alert lifecycle

Implement:

```text
TRIGGERED
ACTIVE
ACKNOWLEDGED
RECOVERED
```

---

### Step 8 — Alert persistence

Store alerts in:

```text
PostgreSQL
```

For the first PoC, SQLite can also be used.

---

### Step 9 — FastAPI backend

Expose APIs such as:

```text
GET /alerts
GET /alerts/active
GET /alerts/{alert_id}
POST /alerts/{alert_id}/acknowledge
```

---

### Step 10 — WebSocket

Use:

```text
/ws/alerts
```

to push newly generated alerts to the frontend.

---

### Step 11 — Metrics

Track:

```text
detection latency
delivery latency
active alerts
resolved alerts
sensor offline events
false alerts
```

---

### Step 12 — Later: Kafka

Once the local architecture works:

```text
JSON files
```

can be replaced by:

```text
Kafka
```

without rewriting the core rule engine.

---

# 42. Final Mental Model

The simplest way to think about the entire project is:

```text
                 SENSOR EVENT
                      │
          ┌───────────┴───────────┐
          │                       │
          ▼                       ▼
     "Is it dangerous?"      "Is it good data?"
          │                       │
          ▼                       ▼
     ALERT ENGINE             MEDALLION
          │                       │
          ▼                       ▼
    Immediate action        Historical truth
          │                       │
          ▼                       ▼
    Operator/Frontend       Analytics/BI
```

The two systems are **parallel but complementary**.

The Alert Engine answers:

> **"Does someone need to act right now?"**

The Medallion pipeline answers:

> **"What happened, was the data trustworthy, and what can we learn from it?"**

That separation is the key architectural decision for this project.

```

### One important takeaway for our implementation

Given your current project structure, I would **not introduce WebSockets, Kafka, or AI yet**. First build the Alert Engine as a clean Python service with:

`metadata → event parser → rule engine → state manager → alert manager → alert DB`

Then put **FastAPI/WebSocket on top of the Alert Manager** for frontend delivery.
```
