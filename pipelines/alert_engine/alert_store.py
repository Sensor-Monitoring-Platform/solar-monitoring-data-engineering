from __future__ import annotations

import sqlite3
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from .alert_manager import Alert, AlertStatus


class AlertStore:
    """
    Persistent storage abstraction for alerts.

    Current implementation:
        SQLite

    The rest of the Alert Engine should interact with this
    class rather than directly interacting with SQLite.
    """

    def __init__(self, database_path: str = "data/alerts.db"):

        self.database_path = Path(database_path)

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._initialize_database()

    # ========================================================
    # DATABASE INITIALIZATION
    # ========================================================

    def _get_connection(self):

        connection = sqlite3.connect(
            self.database_path,
            timeout=30,
        )

        connection.row_factory = sqlite3.Row

        return connection

    def _initialize_database(self):

        with self._get_connection() as connection:

            cursor = connection.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (

                    alert_id TEXT PRIMARY KEY,

                    device_id TEXT NOT NULL,
                    device_type TEXT NOT NULL,

                    plant_id TEXT NOT NULL,
                    machine_id TEXT NOT NULL,

                    rule_id TEXT NOT NULL,
                    rule_type TEXT NOT NULL,

                    severity TEXT NOT NULL,
                    status TEXT NOT NULL,

                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL,

                    current_value REAL,
                    peak_value REAL,

                    unit TEXT,

                    occurrence_count INTEGER NOT NULL,

                    acknowledged_at TEXT,
                    acknowledged_by TEXT,

                    resolved_at TEXT,

                    resolution_reason TEXT,

                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_events (

                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,

                    alert_id TEXT NOT NULL,

                    event_type TEXT NOT NULL,

                    event_timestamp TEXT NOT NULL,

                    value REAL,

                    severity TEXT,

                    status TEXT,

                    actor TEXT,

                    reason TEXT,

                    metadata TEXT,

                    FOREIGN KEY(alert_id)
                        REFERENCES alerts(alert_id)
                )
                """
            )

            # ------------------------------------------------
            # Useful indexes
            # ------------------------------------------------

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_alerts_device
                ON alerts(device_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_alerts_status
                ON alerts(status)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_alerts_machine
                ON alerts(machine_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_alert_events_alert
                ON alert_events(alert_id)
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_alert_events_timestamp
                ON alert_events(event_timestamp)
                """
            )

            connection.commit()

    # ========================================================
    # CREATE ALERT
    # ========================================================

    def create_alert(
        self,
        alert: Alert,
    ) -> None:

        now = datetime.utcnow().isoformat()

        with self._get_connection() as connection:

            connection.execute(
                """
                INSERT INTO alerts (

                    alert_id,

                    device_id,
                    device_type,

                    plant_id,
                    machine_id,

                    rule_id,
                    rule_type,

                    severity,
                    status,

                    first_seen,
                    last_seen,

                    current_value,
                    peak_value,

                    unit,

                    occurrence_count,

                    acknowledged_at,
                    acknowledged_by,

                    resolved_at,
                    resolution_reason,

                    created_at,
                    updated_at

                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?
                )
                """,
                (
                    alert.alert_id,

                    alert.device_id,
                    alert.device_type,

                    alert.plant_id,
                    alert.machine_id,

                    alert.rule_id,
                    alert.rule_type,

                    alert.severity.value,
                    alert.status.value,

                    alert.first_seen.isoformat(),
                    alert.last_seen.isoformat(),

                    alert.current_value,
                    alert.peak_value,

                    alert.unit,

                    alert.occurrence_count,

                    (
                        alert.acknowledged_at.isoformat()
                        if alert.acknowledged_at
                        else None
                    ),

                    alert.acknowledged_by,

                    (
                        alert.resolved_at.isoformat()
                        if alert.resolved_at
                        else None
                    ),

                    alert.resolution_reason,

                    now,
                    now,
                ),
            )

            self._insert_event(
                connection=connection,
                alert=alert,
                event_type="CREATED",
                event_timestamp=alert.first_seen,
            )

            connection.commit()

    # ========================================================
    # UPDATE ALERT
    # ========================================================

    def update_alert(
        self,
        alert: Alert,
    ) -> None:

        now = datetime.utcnow().isoformat()

        with self._get_connection() as connection:

            connection.execute(
                """
                UPDATE alerts

                SET
                    last_seen = ?,
                    current_value = ?,
                    peak_value = ?,
                    occurrence_count = ?,
                    status = ?,
                    updated_at = ?

                WHERE alert_id = ?
                """,
                (
                    alert.last_seen.isoformat(),

                    alert.current_value,
                    alert.peak_value,

                    alert.occurrence_count,

                    alert.status.value,

                    now,

                    alert.alert_id,
                ),
            )

            self._insert_event(
                connection=connection,
                alert=alert,
                event_type="VALUE_UPDATED",
                event_timestamp=alert.last_seen,
            )

            connection.commit()

    # ========================================================
    # ACKNOWLEDGE
    # ========================================================

    def acknowledge_alert(
        self,
        alert: Alert,
    ) -> None:

        now = datetime.utcnow().isoformat()

        with self._get_connection() as connection:

            connection.execute(
                """
                UPDATE alerts

                SET
                    status = ?,
                    acknowledged_at = ?,
                    acknowledged_by = ?,
                    updated_at = ?

                WHERE alert_id = ?
                """,
                (
                    alert.status.value,

                    (
                        alert.acknowledged_at.isoformat()
                        if alert.acknowledged_at
                        else None
                    ),

                    alert.acknowledged_by,

                    now,

                    alert.alert_id,
                ),
            )

            self._insert_event(
                connection=connection,
                alert=alert,
                event_type="ACKNOWLEDGED",
                event_timestamp=(
                    alert.acknowledged_at
                    or datetime.utcnow()
                ),
                actor=alert.acknowledged_by,
            )

            connection.commit()

    # ========================================================
    # RESOLVE
    # ========================================================

    def resolve_alert(
        self,
        alert: Alert,
    ) -> None:

        now = datetime.utcnow().isoformat()

        with self._get_connection() as connection:

            connection.execute(
                """
                UPDATE alerts

                SET
                    status = ?,
                    resolved_at = ?,
                    resolution_reason = ?,
                    updated_at = ?

                WHERE alert_id = ?
                """,
                (
                    alert.status.value,

                    (
                        alert.resolved_at.isoformat()
                        if alert.resolved_at
                        else None
                    ),

                    alert.resolution_reason,

                    now,

                    alert.alert_id,
                ),
            )

            self._insert_event(
                connection=connection,
                alert=alert,
                event_type="RESOLVED",
                event_timestamp=(
                    alert.resolved_at
                    or datetime.utcnow()
                ),
                reason=alert.resolution_reason,
            )

            connection.commit()

    # ========================================================
    # GET ALERT
    # ========================================================

    def get_alert(
        self,
        alert_id: str,
    ) -> Optional[dict]:

        with self._get_connection() as connection:

            row = connection.execute(
                """
                SELECT *
                FROM alerts
                WHERE alert_id = ?
                """,
                (alert_id,),
            ).fetchone()

        if row is None:
            return None

        return dict(row)

    # ========================================================
    # ACTIVE ALERTS
    # ========================================================

    def get_active_alerts(self) -> list[dict]:

        with self._get_connection() as connection:

            rows = connection.execute(
                """
                SELECT *
                FROM alerts

                WHERE status IN (
                    'OPEN',
                    'ACKNOWLEDGED'
                )

                ORDER BY first_seen ASC
                """
            ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    # ========================================================
    # ALERT HISTORY
    # ========================================================

    def get_alert_history(
        self,
        alert_id: str,
    ) -> list[dict]:

        with self._get_connection() as connection:

            rows = connection.execute(
                """
                SELECT *

                FROM alert_events

                WHERE alert_id = ?

                ORDER BY event_timestamp ASC
                """,
                (alert_id,),
            ).fetchall()

        return [
            dict(row)
            for row in rows
        ]

    # ========================================================
    # INTERNAL EVENT INSERT
    # ========================================================

    def _insert_event(
        self,
        connection,
        alert: Alert,
        event_type: str,
        event_timestamp: datetime,
        value: Optional[float] = None,
        actor: Optional[str] = None,
        reason: Optional[str] = None,
    ):

        if value is None:
            value = alert.current_value

        connection.execute(
            """
            INSERT INTO alert_events (

                alert_id,
                event_type,
                event_timestamp,

                value,
                severity,
                status,

                actor,
                reason

            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert.alert_id,

                event_type,

                event_timestamp.isoformat(),

                value,

                alert.severity.value,

                alert.status.value,

                actor,

                reason,
            ),
        )