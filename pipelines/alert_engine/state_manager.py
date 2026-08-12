from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional

from .models import SensorEvent


# ============================================================
# SENSOR STATES
# ============================================================

ONLINE = "ONLINE"
SUSPECTED_OFFLINE = "SUSPECTED_OFFLINE"
OFFLINE = "OFFLINE"
RECOVERED = "RECOVERED"


# ============================================================
# SENSOR STATE
# ============================================================


@dataclass
class SensorState:
    """
    Runtime state maintained for one physical sensor.

    This state is NOT the permanent alert history.

    It is an in-memory operational state used by the alert
    engine while processing events.
    """

    device_id: str

    # --------------------------------------------------------
    # Event timing
    # --------------------------------------------------------

    last_event_timestamp: Optional[datetime] = None

    last_received_at: Optional[datetime] = None

    # --------------------------------------------------------
    # Last known sensor reading
    # --------------------------------------------------------

    last_value: Optional[float] = None

    last_unit: Optional[str] = None

    # --------------------------------------------------------
    # Sensor status
    # --------------------------------------------------------

    status: str = ONLINE

    state_changed_at: Optional[datetime] = None

    # --------------------------------------------------------
    # Data quality
    # --------------------------------------------------------

    consecutive_invalid_events: int = 0

    total_events: int = 0

    total_invalid_events: int = 0

    # --------------------------------------------------------
    # Recovery tracking
    # --------------------------------------------------------

    recovery_count: int = 0


# ============================================================
# STATE UPDATE RESULT
# ============================================================


@dataclass(frozen=True)
class StateUpdateResult:
    """
    Describes what changed after processing an event.
    """

    previous_status: str

    current_status: str

    status_changed: bool

    became_online: bool

    became_offline: bool

    recovered: bool

    out_of_order: bool


# ============================================================
# STATE MANAGER
# ============================================================


class StateManager:
    """
    Maintains operational state for all sensors.

    Important:
        State is maintained per device_id.

    Example:

        TEMP-GGN01-L01-MC01-001
        VIB-GGN01-L01-MC01-001
        PRESS-GGN01-L02-MC05-001

    each gets an independent SensorState.
    """

    def __init__(self) -> None:

        self._states: Dict[str, SensorState] = {}

    # ========================================================
    # GET STATE
    # ========================================================

    def get_state(
        self,
        device_id: str,
    ) -> SensorState:

        if device_id not in self._states:

            self._states[device_id] = SensorState(
                device_id=device_id,
            )

        return self._states[device_id]

    # ========================================================
    # PROCESS EVENT
    # ========================================================

    def process_event(
        self,
        event: SensorEvent,
        received_at: Optional[datetime] = None,
    ) -> StateUpdateResult:

        state = self.get_state(event.device_id)

        if received_at is None:
            received_at = event.timestamp

        previous_status = state.status

        # ----------------------------------------------------
        # Detect out-of-order event
        # ----------------------------------------------------

        out_of_order = False

        if (
            state.last_event_timestamp is not None
            and event.timestamp < state.last_event_timestamp
        ):
            out_of_order = True

        # ----------------------------------------------------
        # Update counters
        # ----------------------------------------------------

        state.total_events += 1

        state.last_received_at = received_at

        # ----------------------------------------------------
        # Update invalid-event counter
        # ----------------------------------------------------

        if event.reading_value is None:
            state.consecutive_invalid_events += 1
            state.total_invalid_events += 1

        else:
            state.consecutive_invalid_events = 0

        # ----------------------------------------------------
        # Update latest reading
        # ----------------------------------------------------

        if not out_of_order:

            state.last_event_timestamp = event.timestamp

            state.last_value = event.reading_value

            state.last_unit = event.reading_unit

        # ----------------------------------------------------
        # Determine recovery
        # ----------------------------------------------------

        recovered = (
            previous_status
            in {
                SUSPECTED_OFFLINE,
                OFFLINE,
            }
            and event.timestamp is not None
        )

        if recovered:

            state.recovery_count += 1

            self._change_state(
                state,
                RECOVERED,
                event.timestamp,
            )

        elif previous_status == RECOVERED:

            self._change_state(
                state,
                ONLINE,
                event.timestamp,
            )

        elif previous_status != ONLINE:

            self._change_state(
                state,
                ONLINE,
                event.timestamp,
            )

        # ----------------------------------------------------
        # Determine status transition
        # ----------------------------------------------------

        current_status = state.status

        status_changed = (
            previous_status != current_status
        )

        became_online = (
            current_status == ONLINE
            and previous_status != ONLINE
        )

        became_offline = (
            current_status
            in {
                SUSPECTED_OFFLINE,
                OFFLINE,
            }
            and previous_status
            not in {
                SUSPECTED_OFFLINE,
                OFFLINE,
            }
        )

        return StateUpdateResult(
            previous_status=previous_status,
            current_status=current_status,
            status_changed=status_changed,
            became_online=became_online,
            became_offline=became_offline,
            recovered=recovered,
            out_of_order=out_of_order,
        )

    # ========================================================
    # CHECK OFFLINE SENSORS
    # ========================================================

    def check_offline_sensors(
        self,
        now: datetime,
        grace_periods: Dict[str, int],
    ) -> list[SensorState]:
        """
        Checks all known sensors and updates their status
        according to their configured offline grace period.

        Parameters
        ----------
        now:
            Current processing time.

        grace_periods:
            Mapping:

                device_id -> grace period in seconds

        Returns
        -------
        list[SensorState]
            Sensors that changed into an offline-related state.
        """

        changed_states: list[SensorState] = []

        for device_id, state in self._states.items():

            if state.last_event_timestamp is None:
                continue

            grace_seconds = grace_periods.get(
                device_id
            )

            if grace_seconds is None:
                continue

            elapsed = (
                now - state.last_event_timestamp
            ).total_seconds()

            # ------------------------------------------------
            # Already offline
            # ------------------------------------------------

            if state.status == OFFLINE:
                continue

            # ------------------------------------------------
            # Grace period exceeded
            # ------------------------------------------------

            if elapsed > grace_seconds:

                if state.status == ONLINE:

                    self._change_state(
                        state,
                        SUSPECTED_OFFLINE,
                        now,
                    )

                    changed_states.append(state)

                elif state.status == SUSPECTED_OFFLINE:

                    self._change_state(
                        state,
                        OFFLINE,
                        now,
                    )

                    changed_states.append(state)

        return changed_states

    # ========================================================
    # FORCE OFFLINE CHECK FOR ONE SENSOR
    # ========================================================

    def check_sensor_offline(
        self,
        device_id: str,
        now: datetime,
        grace_period_seconds: int,
    ) -> Optional[SensorState]:

        state = self.get_state(device_id)

        if state.last_event_timestamp is None:
            return None

        elapsed = (
            now - state.last_event_timestamp
        ).total_seconds()

        if elapsed <= grace_period_seconds:
            return None

        if state.status == ONLINE:

            self._change_state(
                state,
                SUSPECTED_OFFLINE,
                now,
            )

            return state

        if state.status == SUSPECTED_OFFLINE:

            self._change_state(
                state,
                OFFLINE,
                now,
            )

            return state

        return None

    # ========================================================
    # STATE TRANSITION
    # ========================================================

    @staticmethod
    def _change_state(
        state: SensorState,
        new_status: str,
        timestamp: datetime,
    ) -> None:

        if state.status == new_status:
            return

        state.status = new_status

        state.state_changed_at = timestamp

    # ========================================================
    # SNAPSHOT
    # ========================================================

    def snapshot(self) -> Dict[str, SensorState]:
        """
        Returns the current state of all sensors.

        Useful for:
            - debugging
            - monitoring
            - metrics
            - API endpoints
        """

        return dict(self._states)

    # ========================================================
    # RESET
    # ========================================================

    def reset_sensor(
        self,
        device_id: str,
    ) -> None:

        self._states.pop(
            device_id,
            None,
        )

    # ========================================================
    # CLEAR ALL
    # ========================================================

    def clear(self) -> None:

        self._states.clear()