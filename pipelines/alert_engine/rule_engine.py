from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .models import (
    AlertRule,
    AlertSeverity,
    EvaluationStatus,
    RuleEvaluation,
    RuleOperator,
    SensorEvent,
)


# ============================================================
# OPERATOR FUNCTIONS
# ============================================================

_OPERATOR_FUNCTIONS: dict[
    RuleOperator,
    Callable[[float, float], bool],
] = {
    RuleOperator.GREATER_THAN: lambda value, threshold: value > threshold,
    RuleOperator.GREATER_THAN_OR_EQUAL: lambda value, threshold: value >= threshold,
    RuleOperator.LESS_THAN: lambda value, threshold: value < threshold,
    RuleOperator.LESS_THAN_OR_EQUAL: lambda value, threshold: value <= threshold,
    RuleOperator.EQUAL: lambda value, threshold: value == threshold,
    RuleOperator.NOT_EQUAL: lambda value, threshold: value != threshold,
}


# ============================================================
# RULE REGISTRY
# ============================================================


class RuleRegistry:
    """
    Stores alert rules indexed by device/sensor type.

    Example:

        temperature_process
            -> warning high rule
            -> warning low rule
            -> critical high rule
            -> critical low rule

    This avoids scanning every rule for every incoming event.
    """

    def __init__(self, rules: list[AlertRule]):
        self._rules_by_device_type: dict[str, tuple[AlertRule, ...]] = {}

        for rule in rules:
            if not rule.enabled:
                continue

            self._rules_by_device_type.setdefault(
                rule.device_type,
                [],
            ).append(rule)

        # Convert lists to tuples after construction so the
        # registry becomes effectively immutable.
        self._rules_by_device_type = {
            device_type: tuple(rules)
            for device_type, rules in self._rules_by_device_type.items()
        }

    def get_rules(self, device_type: str) -> tuple[AlertRule, ...]:
        """
        Return all enabled rules for a sensor type.
        """
        return self._rules_by_device_type.get(device_type, ())

    def has_rules(self, device_type: str) -> bool:
        return device_type in self._rules_by_device_type


# ============================================================
# RULE ENGINE
# ============================================================


class RuleEngine:
    """
    Stateless rule evaluation engine.

    It receives:
        SensorEvent

    and produces:
        RuleEvaluation

    It does NOT:
        - write to a database
        - send WebSocket messages
        - maintain alert state
        - determine whether an alert is duplicated
        - determine whether a sensor is offline

    Those responsibilities belong to other components.
    """

    def __init__(self, rule_registry: RuleRegistry):
        self.registry = rule_registry

    def evaluate(self, event: SensorEvent) -> list[RuleEvaluation]:
        """
        Evaluate a sensor event against all applicable rules.

        Returns every triggered rule.

        A single event can theoretically trigger multiple rules.
        """

        rules = self.registry.get_rules(event.device_type)

        if not rules:
            return [
                RuleEvaluation(
                    status=EvaluationStatus.NORMAL,
                    rule_id=None,
                    severity=None,
                    triggered=False,
                    value=event.reading_value,
                    threshold=None,
                    reason="No alert rules configured for sensor type",
                )
            ]

        if event.reading_value is None:
            return [
                RuleEvaluation(
                    status=EvaluationStatus.INVALID,
                    rule_id=None,
                    severity=None,
                    triggered=False,
                    value=None,
                    threshold=None,
                    reason="Sensor event contains no numeric reading",
                )
            ]

        evaluations: list[RuleEvaluation] = []

        for rule in rules:
            evaluation = self._evaluate_rule(event, rule)

            if evaluation.triggered:
                evaluations.append(evaluation)

        # Nothing triggered.
        if not evaluations:
            return [
                RuleEvaluation(
                    status=EvaluationStatus.NORMAL,
                    rule_id=None,
                    severity=None,
                    triggered=False,
                    value=event.reading_value,
                    threshold=None,
                    reason="No configured threshold exceeded",
                )
            ]

        return evaluations

    @staticmethod
    def _evaluate_rule(
        event: SensorEvent,
        rule: AlertRule,
    ) -> RuleEvaluation:

        value = event.reading_value

        if value is None:
            return RuleEvaluation(
                status=EvaluationStatus.INVALID,
                rule_id=rule.rule_id,
                severity=None,
                triggered=False,
                value=None,
                threshold=rule.threshold,
                reason="Missing sensor value",
            )

        operator_function = _OPERATOR_FUNCTIONS.get(rule.operator)

        if operator_function is None:
            return RuleEvaluation(
                status=EvaluationStatus.INVALID,
                rule_id=rule.rule_id,
                severity=None,
                triggered=False,
                value=value,
                threshold=rule.threshold,
                reason=f"Unsupported operator: {rule.operator}",
            )

        triggered = operator_function(
            value,
            rule.threshold,
        )

        if not triggered:
            return RuleEvaluation(
                status=EvaluationStatus.NORMAL,
                rule_id=rule.rule_id,
                severity=None,
                triggered=False,
                value=value,
                threshold=rule.threshold,
                reason="Threshold not exceeded",
            )

        status = self._severity_to_status(rule.severity)

        message = rule.message_template.format(
            device_id=event.device_id,
            device_type=event.device_type,
            plant_id=event.plant_id,
            machine_id=event.machine_id,
            value=value,
            threshold=rule.threshold,
            unit=event.reading_unit or "",
        )

        return RuleEvaluation(
            status=status,
            rule_id=rule.rule_id,
            severity=rule.severity,
            triggered=True,
            value=value,
            threshold=rule.threshold,
            message=message,
            reason=(
                f"Rule triggered: {value} "
                f"{rule.operator.value} {rule.threshold}"
            ),
        )

    @staticmethod
    def _severity_to_status(
        severity: AlertSeverity,
    ) -> EvaluationStatus:

        mapping = {
            AlertSeverity.INFO: EvaluationStatus.NORMAL,
            AlertSeverity.WARNING: EvaluationStatus.WARNING,
            AlertSeverity.CRITICAL: EvaluationStatus.CRITICAL,
            AlertSeverity.EMERGENCY: EvaluationStatus.EMERGENCY,
        }

        return mapping[severity]


# ============================================================
# RULE BUILDER
# ============================================================


@dataclass(frozen=True)
class SensorTypeConfig:
    """
    Internal representation of one sensors.yml entry.
    """

    device_type: str

    warning_low: float | None
    warning_high: float | None

    critical_low: float | None
    critical_high: float | None

    is_safety_critical: bool

    unit: str | None


def build_rules_from_sensor_metadata(
    sensor_types: dict,
) -> list[AlertRule]:
    """
    Convert sensors.yml configuration into executable AlertRule objects.

    This is the bridge between configuration and execution.

    Example:

        sensors.yml

        temperature_process:
            warning_threshold:
                high: 190
            critical_threshold:
                high: 210

    becomes:

        TEMP_TEMPERATURE_PROCESS_HIGH_WARNING
        TEMP_TEMPERATURE_PROCESS_HIGH_CRITICAL
    """

    rules: list[AlertRule] = []

    for device_type, config in sensor_types.items():

        warning = config.get("warning_threshold")
        critical = config.get("critical_threshold")

        if warning is None and critical is None:
            # Example:
            # access_control
            #
            # It does not currently have numeric threshold rules.
            continue

        unit = config.get("unit")

        safety_critical = bool(
            config.get("is_safety_critical", False)
        )

        # ----------------------------------------------------
        # WARNING LOW
        # ----------------------------------------------------

        if warning and warning.get("low") is not None:
            low = warning["low"]

            # -1 means "not applicable" in smoke_alarm.
            if low >= 0:
                rules.append(
                    AlertRule(
                        rule_id=f"{device_type.upper()}_LOW_WARNING",
                        device_type=device_type,
                        operator=RuleOperator.LESS_THAN,
                        threshold=float(low),
                        severity=AlertSeverity.WARNING,
                        alert_type="THRESHOLD_LOW",
                        message_template=(
                            "{device_type} sensor {device_id} "
                            "on machine {machine_id} is below "
                            "warning threshold: {value} {unit} "
                            "(threshold={threshold} {unit})"
                        ),
                        cooldown_seconds=60,
                        safety_critical=safety_critical,
                    )
                )

        # ----------------------------------------------------
        # WARNING HIGH
        # ----------------------------------------------------

        if warning and warning.get("high") is not None:
            high = warning["high"]

            if high >= 0:
                rules.append(
                    AlertRule(
                        rule_id=f"{device_type.upper()}_HIGH_WARNING",
                        device_type=device_type,
                        operator=RuleOperator.GREATER_THAN,
                        threshold=float(high),
                        severity=AlertSeverity.WARNING,
                        alert_type="THRESHOLD_HIGH",
                        message_template=(
                            "{device_type} sensor {device_id} "
                            "on machine {machine_id} exceeded "
                            "warning threshold: {value} {unit} "
                            "(threshold={threshold} {unit})"
                        ),
                        cooldown_seconds=60,
                        safety_critical=safety_critical,
                    )
                )

        # ----------------------------------------------------
        # CRITICAL LOW
        # ----------------------------------------------------

        if critical and critical.get("low") is not None:
            low = critical["low"]

            if low >= 0:
                rules.append(
                    AlertRule(
                        rule_id=f"{device_type.upper()}_LOW_CRITICAL",
                        device_type=device_type,
                        operator=RuleOperator.LESS_THAN,
                        threshold=float(low),
                        severity=AlertSeverity.CRITICAL,
                        alert_type="THRESHOLD_LOW_CRITICAL",
                        message_template=(
                            "{device_type} sensor {device_id} "
                            "on machine {machine_id} is below "
                            "critical threshold: {value} {unit} "
                            "(threshold={threshold} {unit})"
                        ),
                        cooldown_seconds=30,
                        safety_critical=safety_critical,
                    )
                )

        # ----------------------------------------------------
        # CRITICAL HIGH
        # ----------------------------------------------------

        if critical and critical.get("high") is not None:
            high = critical["high"]

            if high >= 0:
                rules.append(
                    AlertRule(
                        rule_id=f"{device_type.upper()}_HIGH_CRITICAL",
                        device_type=device_type,
                        operator=RuleOperator.GREATER_THAN,
                        threshold=float(high),
                        severity=AlertSeverity.CRITICAL,
                        alert_type="THRESHOLD_HIGH_CRITICAL",
                        message_template=(
                            "{device_type} sensor {device_id} "
                            "on machine {machine_id} exceeded "
                            "critical threshold: {value} {unit} "
                            "(threshold={threshold} {unit})"
                        ),
                        cooldown_seconds=30,
                        safety_critical=safety_critical,
                    )
                )

    return rules