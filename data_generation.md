# IoT Sensor Simulator Review & Enhancement Proposal

## Purpose

The current `simulate_sensors.py` is already a strong foundation for a metadata-driven IoT data engineering project. It successfully demonstrates configurable sensor discovery, realistic telemetry generation, anomaly injection, and cloud-native data ingestion.

However, since the objective of this project is to emulate an **industrial-scale manufacturing IoT platform**, the simulator can be further enhanced to better represent how data is generated in real manufacturing environments.

This document outlines the proposed improvements before implementing the Bronze pipeline.

---

# Current Assessment


### Current Features

- Metadata-driven device discovery
- Gaussian noise generation
- Sensor drift simulation
- Random spike anomalies
- Flatline (stuck sensor) simulation
- Device offline simulation
- Late-arriving events
- GitHub Actions integration
- Hugging Face Object Storage upload
- Quality flags
- NDJSON batch generation

These provide an excellent starting point for building the downstream Medallion Architecture.

---

# Identified Limitation

Currently, **every sensor generates values independently**.

For example:

```text
Temperature
↓
118°C
```

Next tick:

```text
Temperature
↓
119°C
```

Likewise,

```text
Temperature
```

has no relationship with

```text
Pressure
Current
Vibration
```

This does not accurately represent a real manufacturing machine.

---

# Proposed Enhancement 1 — Machine State Based Simulation

Instead of generating sensor values independently, each machine should maintain an internal operating state.

```text
Machine
      │
      ▼
Machine State
      │
      ▼
Generate Sensor Readings
```

rather than

```text
Sensor
      │
      ▼
Random Reading
```

---

## Suggested Machine States

```text
RUNNING

IDLE

STARTUP

SHUTDOWN

MAINTENANCE

FAULT

EMERGENCY
```

Every machine always exists in exactly one state.

Individual sensors simply reflect that machine state.

---

## Example

### Normal Operation

```text
Temperature : 120°C

Pressure    : 5.3 bar

Current     : 18 A

Vibration   : 2.4 g
```

### Fault Condition

```text
Temperature : 195°C

Pressure    : 8.8 bar

Current     : 27 A

Vibration   : 6.5 g
```

### Emergency

```text
Temperature : 230°C

Pressure    : 10.8 bar

Current     : 33 A

Vibration   : 9.7 g
```

Multiple sensors now behave consistently because they belong to the same machine.

---

# Proposed Enhancement 2 — Correlated Failures

Instead of injecting independent random anomalies, generate realistic manufacturing failure scenarios.

Example

```text
Bearing Failure

↓

Vibration increases

↓

Current increases

↓

Temperature rises

↓

Pressure fluctuates

↓

Machine Shutdown
```

This creates realistic relationships between sensors and enables meaningful downstream analytics.

---

# Proposed Enhancement 3 — Event-Based Simulation

Replace purely random anomalies with fault events.

Suggested events:

```text
Bearing Failure

Motor Overload

Cooling Failure

Hydraulic Leak

Power Fluctuation

Sensor Drift

Sensor Failure

Smoke Incident

Emergency Stop

Maintenance Mode
```

Each event should affect multiple sensors simultaneously.

---

# Proposed Enhancement 4 — Progressive Fault Development

Instead of a random spike occurring in a single reading, faults should evolve gradually.

Example

```text
Tick 1
Temperature = 120°C

↓

Tick 2
125°C

↓

Tick 3
131°C

↓

Tick 4
138°C

↓

Tick 5
150°C

↓

Tick 6
170°C

↓

Tick 7
195°C

↓

Tick 8
220°C

↓

Emergency
```

This enables the Alert Engine to detect developing failures instead of isolated spikes.

---

# Proposed Enhancement 5 — Corrupted Data Generation

Corrupted data is different from anomalous data.

An anomaly is still a technically valid reading.

Corrupted data is structurally invalid.

Examples to simulate:

### Missing Required Field

```json
{
    "device_type": "temperature_process"
}
```

---

### Invalid Data Type

```json
{
    "reading": {
        "value": "ABC"
    }
}
```

---

### Incorrect Unit

```json
{
    "unit": "fahrenheit"
}
```

---

### Unknown Sensor Type

```json
{
    "device_type": "temperature_xyz"
}
```

---

### Future Timestamp

```text
2035-01-01T00:00:00Z
```

---

### Very Old Timestamp

```text
2012-01-01T00:00:00Z
```

---

### Duplicate Events

Generate identical records intentionally to validate Bronze deduplication.

---

### Unexpected Fields

```json
{
    "firmware_build": "2026.08.01"
}
```

Useful for testing schema evolution.

---

### Malformed JSON

Instead of

```json
{
    "device_id": "TEMP-001"
}
```

generate

```text
{"device_id":
```

to validate quarantine logic.

---

# Proposed Enhancement 6 — Device Lifecycle Simulation

Instead of simply toggling devices offline, simulate realistic lifecycle transitions.

```text
ONLINE

↓

OFFLINE

↓

ONLINE

↓

MAINTENANCE

↓

ONLINE
```

This provides richer operational scenarios.

---

# Proposed Enhancement 7 — Separate Data Quality from Operational Condition

Current implementation mixes operational anomalies and telemetry quality.

Suggested separation:

## Telemetry Quality

Represents the quality of incoming data.

```text
OK

LATE

NULL

CORRUPTED

DUPLICATE

OUT_OF_ORDER
```

---

## Operational Condition

Represents the actual machine condition.

```text
NORMAL

WARNING

FAULT

EMERGENCY

MAINTENANCE
```

A reading can therefore be:

```text
Telemetry Quality : OK

Operational Condition : EMERGENCY
```

which more accurately reflects industrial systems.

---

# Proposed Enhancement 8 — Metadata-Driven Simulation Configuration

Current simulation probabilities are hardcoded inside the Python script.

Move these values into a dedicated configuration file.

Example

`simulation_config.yml`

```yaml
anomaly_probability: 0.03

fault_probability: 0.01

duplicate_probability: 0.02

corrupt_probability: 0.01

schema_evolution_probability: 0.005

late_arrival_probability: 0.02

offline_probability: 0.005
```

This makes the simulator completely metadata-driven and simplifies experimentation.

---

# Recommended Implementation Roadmap

To minimize complexity and allow incremental validation, the simulator enhancements should be implemented in phases.

## Phase A — High Priority

- Introduce `simulation_config.yml`
- Remove hardcoded probabilities
- Add corrupted data generation
- Separate telemetry quality from operational condition

---

## Phase B

- Introduce machine operating states
- Correlate all sensors belonging to the same machine

---

## Phase C

- Introduce event-based failure scenarios
- Simulate progressive fault development over multiple ticks
- Generate realistic industrial incidents

---

# Expected Benefits

Implementing these enhancements will provide:

- More realistic manufacturing telemetry
- Better testing coverage for Bronze validation
- More meaningful Alert Engine behaviour
- Stronger Silver and Gold analytical datasets
- Higher-quality Power BI dashboards
- A project architecture that more closely resembles enterprise industrial IoT platforms

---

# Conclusion

The current simulator already demonstrates strong engineering practices and provides an excellent base for the project.

The proposed enhancements focus on improving **realism**, **maintainability**, and **testability**, ensuring the generated data better represents real manufacturing environments while providing comprehensive scenarios for validating the Medallion Architecture and Alert Engine.