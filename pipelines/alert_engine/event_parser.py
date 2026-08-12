from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from .models import SensorEvent


# ============================================================
# PARSE RESULT
# ============================================================


@dataclass(frozen=True)
class ParseResult:
    """
    Result of parsing one raw sensor record.

    A record can either produce:
        1. a valid SensorEvent
        2. a rejected record with an error reason

    The parser never raises an exception for normal bad input.
    """

    success: bool

    event: Optional[SensorEvent] = None

    error_code: Optional[str] = None
    error_message: Optional[str] = None

    raw_record: Optional[dict[str, Any]] = None


# ============================================================
# REQUIRED FIELDS
# ============================================================


REQUIRED_FIELDS = {
    "device_id",
    "device_type",
    "plant_id",
    "machine_id",
    "timestamp",
}


# ============================================================
# EVENT PARSER
# ============================================================


class EventParser:
    """
    Converts raw JSON-like dictionaries into SensorEvent objects.

    Responsibilities:
        - structural validation
        - required-field validation
        - timestamp parsing
        - reading extraction
        - numeric conversion
        - optional-field extraction
        - preserving unknown fields

    Does NOT:
        - evaluate alert thresholds
        - check whether a sensor is offline
        - create alerts
        - write to database
        - send WebSocket messages
    """

    def parse(self, record: dict[str, Any]) -> ParseResult:

        # ----------------------------------------------------
        # 1. Basic type validation
        # ----------------------------------------------------

        if not isinstance(record, dict):
            return self._reject(
                record,
                "INVALID_RECORD",
                "Sensor record must be a dictionary",
            )

        # ----------------------------------------------------
        # 2. Required fields
        # ----------------------------------------------------

        missing_fields = [
            field
            for field in REQUIRED_FIELDS
            if field not in record
            or record[field] is None
        ]

        if missing_fields:
            return self._reject(
                record,
                "MISSING_REQUIRED_FIELD",
                f"Missing required fields: {missing_fields}",
            )

        # ----------------------------------------------------
        # 3. Validate string identifiers
        # ----------------------------------------------------

        string_fields = (
            "device_id",
            "device_type",
            "plant_id",
            "machine_id",
        )

        for field in string_fields:
            if not isinstance(record[field], str) or not record[field].strip():
                return self._reject(
                    record,
                    "INVALID_FIELD_TYPE",
                    f"{field} must be a non-empty string",
                )

        # ----------------------------------------------------
        # 4. Parse timestamp
        # ----------------------------------------------------

        timestamp_result = self._parse_timestamp(
            record["timestamp"]
        )

        if timestamp_result is None:
            return self._reject(
                record,
                "INVALID_TIMESTAMP",
                f"Invalid timestamp: {record['timestamp']}",
            )

        timestamp = timestamp_result

        # ----------------------------------------------------
        # 5. Extract reading
        # ----------------------------------------------------

        reading = record.get("reading")

        reading_value: Optional[float] = None
        reading_unit: Optional[str] = None

        if reading is not None:

            if not isinstance(reading, dict):
                return self._reject(
                    record,
                    "INVALID_READING",
                    "reading must be an object",
                )

            reading_value_result = self._parse_numeric_value(
                reading.get("value")
            )

            if reading_value_result is not None:
                reading_value = reading_value_result

            reading_unit = reading.get("unit")

            if reading_unit is not None and not isinstance(
                reading_unit,
                str,
            ):
                return self._reject(
                    record,
                    "INVALID_READING_UNIT",
                    "reading.unit must be a string or null",
                )

        # ----------------------------------------------------
        # 6. Optional timestamps
        # ----------------------------------------------------

        ingestion_timestamp = self._parse_optional_timestamp(
            record.get("ingestion_timestamp")
        )

        if (
            record.get("ingestion_timestamp") is not None
            and ingestion_timestamp is None
        ):
            return self._reject(
                record,
                "INVALID_INGESTION_TIMESTAMP",
                "Invalid ingestion_timestamp",
            )

        # ----------------------------------------------------
        # 7. Extract optional health fields
        # ----------------------------------------------------

        error_code = self._optional_string(
            record.get("error_code")
        )

        calibration_status = self._optional_string(
            record.get("calibration_status")
        )

        sensor_health = self._optional_string(
            record.get("sensor_health")
        )

        firmware_version = self._optional_string(
            record.get("firmware_version")
        )

        quality_flag = self._optional_string(
            record.get("quality_flag")
        )

        # ----------------------------------------------------
        # 8. Preserve unknown fields
        # ----------------------------------------------------

        known_fields = {
            "device_id",
            "device_type",
            "plant_id",
            "machine_id",
            "timestamp",
            "reading",
            "firmware_version",
            "quality_flag",
            "ingestion_timestamp",
            "processing_timestamp",
            "source_file_name",
            "error_code",
            "calibration_status",
            "sensor_health",
        }

        metadata = {
            key: value
            for key, value in record.items()
            if key not in known_fields
        }

        # ----------------------------------------------------
        # 9. Construct SensorEvent
        # ----------------------------------------------------

        event = SensorEvent(
            device_id=record["device_id"],
            device_type=record["device_type"],
            plant_id=record["plant_id"],
            machine_id=record["machine_id"],
            timestamp=timestamp,
            reading_value=reading_value,
            reading_unit=reading_unit,
            quality_flag=quality_flag,
            firmware_version=firmware_version,
            ingestion_timestamp=ingestion_timestamp,
            error_code=error_code,
            calibration_status=calibration_status,
            sensor_health=sensor_health,
            metadata=metadata,
        )

        return ParseResult(
            success=True,
            event=event,
        )

    # ========================================================
    # TIMESTAMP
    # ========================================================

    @staticmethod
    def _parse_timestamp(
        value: Any,
    ) -> Optional[datetime]:

        if not isinstance(value, str):
            return None

        try:
            timestamp = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )

            # Normalize naive timestamps to UTC.
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(
                    tzinfo=timezone.utc
                )

            return timestamp.astimezone(timezone.utc)

        except (TypeError, ValueError):
            return None

    @classmethod
    def _parse_optional_timestamp(
        cls,
        value: Any,
    ) -> Optional[datetime]:

        if value is None:
            return None

        return cls._parse_timestamp(value)

    # ========================================================
    # NUMERIC VALUE
    # ========================================================

    @staticmethod
    def _parse_numeric_value(
        value: Any,
    ) -> Optional[float]:

        if value is None:
            return None

        if isinstance(value, bool):
            return None

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return None

        # Reject NaN and infinity.
        if numeric_value != numeric_value:
            return None

        if numeric_value in (
            float("inf"),
            float("-inf"),
        ):
            return None

        return numeric_value

    # ========================================================
    # OPTIONAL STRING
    # ========================================================

    @staticmethod
    def _optional_string(
        value: Any,
    ) -> Optional[str]:

        if value is None:
            return None

        if not isinstance(value, str):
            return None

        value = value.strip()

        return value if value else None

    # ========================================================
    # REJECTION
    # ========================================================

    @staticmethod
    def _reject(
        record: Any,
        error_code: str,
        error_message: str,
    ) -> ParseResult:

        return ParseResult(
            success=False,
            error_code=error_code,
            error_message=error_message,
            raw_record=(
                record
                if isinstance(record, dict)
                else None
            ),
        )