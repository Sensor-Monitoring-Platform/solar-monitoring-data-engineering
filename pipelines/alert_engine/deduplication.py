from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional


# ============================================================
# DEDUPLICATION RESULT
# ============================================================


@dataclass(frozen=True)
class DeduplicationResult:
    """
    Result returned by the deduplication layer.
    """

    is_duplicate: bool

    fingerprint: str

    existing_alert_id: Optional[str]

    reason: str


# ============================================================
# ALERT DEDUPLICATOR
# ============================================================


class AlertDeduplicator:
    """
    Prevents repeated sensor events from creating duplicate
    alerts for the same underlying incident.

    The deduplicator maintains lightweight in-memory state.

    Permanent alert history belongs to AlertStore.
    """

    def __init__(
        self,
        cooldown_seconds: int = 60,
    ) -> None:

        self.cooldown = timedelta(
            seconds=cooldown_seconds
        )

        # ----------------------------------------------------
        # fingerprint -> alert information
        # ----------------------------------------------------

        self._active_alerts: Dict[
            str,
            AlertDeduplicationState,
        ] = {}

    # ========================================================
    # BUILD FINGERPRINT
    # ========================================================

    @staticmethod
    def build_fingerprint(
        device_id: str,
        rule_type: str,
        severity: str,
    ) -> str:

        return (
            f"{device_id}|"
            f"{rule_type}|"
            f"{severity}"
        )

    # ========================================================
    # CHECK EVENT
    # ========================================================

    def check(
        self,
        device_id: str,
        rule_type: str,
        severity: str,
        event_timestamp: datetime,
    ) -> DeduplicationResult:

        fingerprint = self.build_fingerprint(
            device_id=device_id,
            rule_type=rule_type,
            severity=severity,
        )

        existing = self._active_alerts.get(
            fingerprint
        )

        # ----------------------------------------------------
        # No previous alert
        # ----------------------------------------------------

        if existing is None:

            return DeduplicationResult(
                is_duplicate=False,
                fingerprint=fingerprint,
                existing_alert_id=None,
                reason="NO_EXISTING_ALERT",
            )

        # ----------------------------------------------------
        # Existing alert is already active
        # ----------------------------------------------------

        if existing.active:

            return DeduplicationResult(
                is_duplicate=True,
                fingerprint=fingerprint,
                existing_alert_id=existing.alert_id,
                reason="ACTIVE_ALERT",
            )

        # ----------------------------------------------------
        # Existing alert is inside cooldown period
        # ----------------------------------------------------

        if (
            event_timestamp - existing.last_seen
            < self.cooldown
        ):

            return DeduplicationResult(
                is_duplicate=True,
                fingerprint=fingerprint,
                existing_alert_id=existing.alert_id,
                reason="COOLDOWN",
            )

        # ----------------------------------------------------
        # Cooldown expired
        # ----------------------------------------------------

        return DeduplicationResult(
            is_duplicate=False,
            fingerprint=fingerprint,
            existing_alert_id=None,
            reason="COOLDOWN_EXPIRED",
        )

    # ========================================================
    # REGISTER ALERT
    # ========================================================

    def register(
        self,
        fingerprint: str,
        alert_id: str,
        event_timestamp: datetime,
    ) -> None:

        self._active_alerts[
            fingerprint
        ] = AlertDeduplicationState(

            alert_id=alert_id,

            first_seen=event_timestamp,

            last_seen=event_timestamp,

            active=True,
        )

    # ========================================================
    # UPDATE EXISTING ALERT
    # ========================================================

    def update(
        self,
        fingerprint: str,
        event_timestamp: datetime,
    ) -> None:

        existing = self._active_alerts.get(
            fingerprint
        )

        if existing is None:
            return

        existing.last_seen = event_timestamp

    # ========================================================
    # RESOLVE ALERT
    # ========================================================

    def resolve(
        self,
        fingerprint: str,
        event_timestamp: datetime,
    ) -> None:

        existing = self._active_alerts.get(
            fingerprint
        )

        if existing is None:
            return

        existing.active = False

        existing.last_seen = event_timestamp

    # ========================================================
    # REMOVE
    # ========================================================

    def remove(
        self,
        fingerprint: str,
    ) -> None:

        self._active_alerts.pop(
            fingerprint,
            None,
        )

    # ========================================================
    # GET ACTIVE ALERT
    # ========================================================

    def get_active_alert(
        self,
        fingerprint: str,
    ) -> Optional[str]:

        existing = self._active_alerts.get(
            fingerprint
        )

        if existing is None:
            return None

        if not existing.active:
            return None

        return existing.alert_id

    # ========================================================
    # CLEANUP
    # ========================================================

    def cleanup(
        self,
        now: datetime,
    ) -> None:

        expired = []

        for fingerprint, state in self._active_alerts.items():

            if state.active:
                continue

            if now - state.last_seen > self.cooldown:

                expired.append(fingerprint)

        for fingerprint in expired:

            self._active_alerts.pop(
                fingerprint,
                None,
            )


# ============================================================
# INTERNAL STATE
# ============================================================


@dataclass
class AlertDeduplicationState:

    alert_id: str

    first_seen: datetime

    last_seen: datetime

    active: bool = True