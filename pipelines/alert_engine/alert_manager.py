class AlertManager:

    def __init__(self, deduplicator):
        self.deduplicator = deduplicator

        # ----------------------------------------------------
        # Temporary active-alert registry.
        #
        # AlertStore will eventually become the persistent
        # source of truth.
        # ----------------------------------------------------

        self._alerts: dict[str, Alert] = {}

    # ========================================================
    # CREATE ALERT
    # ========================================================

    def create_alert(
        self,
        *,
        event,
        rule_result,
    ) -> Alert:

        alert_id = self._generate_alert_id()

        alert = Alert(
            alert_id=alert_id,

            device_id=event.device_id,
            device_type=event.device_type,

            plant_id=event.plant_id,
            machine_id=event.machine_id,

            rule_id=rule_result.rule_id,
            rule_type=rule_result.rule_type,

            severity=AlertSeverity(
                rule_result.severity
            ),

            status=AlertStatus.OPEN,

            first_seen=event.timestamp,
            last_seen=event.timestamp,

            current_value=event.value,
            peak_value=event.value,

            unit=event.unit,

            occurrence_count=1,
        )

        self._alerts[alert_id] = alert

        fingerprint = self.deduplicator.build_fingerprint(
            device_id=event.device_id,
            rule_type=rule_result.rule_id,
            severity=rule_result.severity,
        )

        self.deduplicator.register(
            fingerprint=fingerprint,
            alert_id=alert_id,
            event_timestamp=event.timestamp,
        )

        return alert

    # ========================================================
    # UPDATE EXISTING ALERT
    # ========================================================

    def update_alert(
        self,
        alert: Alert,
        *,
        event,
    ) -> Alert:

        alert.last_seen = event.timestamp

        alert.current_value = event.value

        alert.occurrence_count += 1

        # ----------------------------------------------------
        # Update peak value
        # ----------------------------------------------------

        if (
            event.value is not None
            and (
                alert.peak_value is None
                or event.value > alert.peak_value
            )
        ):
            alert.peak_value = event.value

        fingerprint = self.deduplicator.build_fingerprint(
            device_id=alert.device_id,
            rule_type=alert.rule_id,
            severity=alert.severity.value,
        )

        self.deduplicator.update(
            fingerprint=fingerprint,
            event_timestamp=event.timestamp,
        )

        return alert

    # ========================================================
    # ACKNOWLEDGE
    # ========================================================

    def acknowledge(
        self,
        alert_id: str,
        *,
        acknowledged_by: str,
        timestamp: datetime,
    ) -> Alert:

        alert = self._get_alert(alert_id)

        if alert.status == AlertStatus.RESOLVED:
            raise ValueError(
                "Cannot acknowledge a resolved alert."
            )

        alert.status = AlertStatus.ACKNOWLEDGED

        alert.acknowledged_at = timestamp

        alert.acknowledged_by = acknowledged_by

        return alert

    # ========================================================
    # RESOLVE
    # ========================================================

    def resolve(
        self,
        alert_id: str,
        *,
        timestamp: datetime,
        reason: str,
    ) -> Alert:

        alert = self._get_alert(alert_id)

        if alert.status == AlertStatus.RESOLVED:
            return alert

        alert.status = AlertStatus.RESOLVED

        alert.resolved_at = timestamp

        alert.resolution_reason = reason

        fingerprint = self.deduplicator.build_fingerprint(
            device_id=alert.device_id,
            rule_type=alert.rule_id,
            severity=alert.severity.value,
        )

        self.deduplicator.resolve(
            fingerprint=fingerprint,
            event_timestamp=timestamp,
        )

        return alert

    # ========================================================
    # GET ALERT
    # ========================================================

    def get_alert(
        self,
        alert_id: str,
    ) -> Optional[Alert]:

        return self._alerts.get(alert_id)

    # ========================================================
    # ACTIVE ALERTS
    # ========================================================

    def get_active_alerts(self) -> list[Alert]:

        return [
            alert
            for alert in self._alerts.values()
            if alert.status != AlertStatus.RESOLVED
        ]

    # ========================================================
    # INTERNAL
    # ========================================================

    def _get_alert(
        self,
        alert_id: str,
    ) -> Alert:

        alert = self._alerts.get(alert_id)

        if alert is None:
            raise KeyError(
                f"Alert not found: {alert_id}"
            )

        return alert

    @staticmethod
    def _generate_alert_id() -> str:

        return f"ALT-{uuid.uuid4().hex[:12].upper()}"