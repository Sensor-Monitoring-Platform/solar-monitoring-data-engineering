"""
Alert Engine - Main Entry Point

Current input:
    JSON / JSONL sensor files

Future input:
    Kafka / Event Hub / MQTT / other streaming source

Responsibilities:
    1. Read sensor events
    2. Convert them into the AlertEvent contract
    3. Pass events to the alert engine
    4. Persist generated alerts
    5. Publish real-time alert events
    6. Periodically check for offline sensors
    7. Track processing metrics

IMPORTANT:
    Business rules should NOT live in this file.

    main.py is an orchestration layer only.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------

try:
    from .alert_manager import AlertManager
    from .alert_store import AlertStore
    from .deduplication import DeduplicationManager
    from .offline_detector import OfflineDetector
    from .rule_engine import RuleEngine
    from .websocket_manager import WebSocketManager
except ImportError:
    # Allows:
    #
    #     python main.py
    #
    # when executed directly from the alert_engine directory.
    from alert_manager import AlertManager
    from alert_store import AlertStore
    from deduplication import DeduplicationManager
    from offline_detector import OfflineDetector
    from rule_engine import RuleEngine
    from websocket_manager import WebSocketManager


# ===========================================================================
# Logging
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("alert_engine")


# ===========================================================================
# Configuration
# ===========================================================================

DEFAULT_INPUT_PATH = "../../data/raw"

DEFAULT_RULE_CONFIG = "../../config/alert_rules.yaml"

DEFAULT_OFFLINE_CHECK_INTERVAL_SECONDS = 5


# ===========================================================================
# Alert Engine
# ===========================================================================


class AlertEngine:
    """
    Coordinates the complete alert-processing pipeline.

    This class intentionally contains orchestration logic only.

    It does NOT decide whether:
        temperature > threshold
        vibration > threshold
        smoke > threshold

    Those decisions belong to RuleEngine.

    It also does NOT directly implement:
        deduplication
        persistence
        websocket communication
        offline detection

    Each responsibility belongs to its own component.
    """

    def __init__(
        self,
        rule_engine: RuleEngine,
        offline_detector: OfflineDetector,
        deduplication_manager: DeduplicationManager,
        alert_manager: AlertManager,
        alert_store: AlertStore,
        websocket_manager: WebSocketManager,
    ):
        self.rule_engine = rule_engine
        self.offline_detector = offline_detector
        self.deduplication_manager = deduplication_manager
        self.alert_manager = alert_manager
        self.alert_store = alert_store
        self.websocket_manager = websocket_manager

        self.running = True

        # Metrics
        self.events_processed = 0
        self.alerts_generated = 0
        self.alerts_suppressed = 0
        self.processing_errors = 0

        self.started_at = time.time()

    # -----------------------------------------------------------------------
    # Event processing
    # -----------------------------------------------------------------------

    def process_event(self, event: Dict[str, Any]) -> None:
        """
        Process one sensor event.

        Pipeline:

            sensor event
                ↓
            offline detector
                ↓
            rule evaluation
                ↓
            deduplication
                ↓
            alert manager
                ↓
            persistence
                ↓
            websocket notification
        """

        start_time = time.perf_counter()

        try:
            self.events_processed += 1

            device_id = event.get("device_id")

            if not device_id:
                logger.warning(
                    "Ignoring event without device_id: %s",
                    event,
                )
                return

            # ---------------------------------------------------------------
            # 1. Register that the sensor is alive
            # ---------------------------------------------------------------

            self.offline_detector.record_event(event)

            # ---------------------------------------------------------------
            # 2. Evaluate alert rules
            # ---------------------------------------------------------------

            evaluation_results = self.rule_engine.evaluate(event)

            if evaluation_results is None:
                return

            # Rule engine may return:
            #
            #   AlertEvaluation
            #
            # or:
            #
            #   list[AlertEvaluation]
            #
            # Normalize to a list.

            if not isinstance(evaluation_results, list):
                evaluation_results = [evaluation_results]

            # ---------------------------------------------------------------
            # 3. Process each rule result
            # ---------------------------------------------------------------

            for evaluation in evaluation_results:

                if evaluation is None:
                    continue

                # -----------------------------------------------------------
                # No alert triggered
                # -----------------------------------------------------------

                if not evaluation.triggered:
                    continue

                # -----------------------------------------------------------
                # 4. Deduplication / cooldown
                # -----------------------------------------------------------

                should_emit = self.deduplication_manager.should_emit(
                    evaluation
                )

                if not should_emit:
                    self.alerts_suppressed += 1

                    logger.debug(
                        "Alert suppressed by deduplication: "
                        "device=%s rule=%s",
                        device_id,
                        evaluation.rule_id,
                    )

                    continue

                # -----------------------------------------------------------
                # 5. Create/update alert lifecycle
                # -----------------------------------------------------------

                alert = self.alert_manager.process_evaluation(
                    evaluation
                )

                if alert is None:
                    continue

                self.alerts_generated += 1

                # -----------------------------------------------------------
                # 6. Persist alert
                # -----------------------------------------------------------

                self.alert_store.save_alert(alert)

                # -----------------------------------------------------------
                # 7. Notify frontend / subscribers
                # -----------------------------------------------------------

                self.websocket_manager.publish_alert(alert)

                logger.warning(
                    "ALERT | severity=%s | device=%s | rule=%s",
                    getattr(alert, "severity", "UNKNOWN"),
                    device_id,
                    evaluation.rule_id,
                )

        except Exception:
            self.processing_errors += 1

            logger.exception(
                "Error while processing sensor event: %s",
                event,
            )

        finally:
            elapsed_ms = (
                time.perf_counter() - start_time
            ) * 1000

            logger.debug(
                "Event processed in %.3f ms",
                elapsed_ms,
            )

    # -----------------------------------------------------------------------
    # Offline detection
    # -----------------------------------------------------------------------

    def check_offline_sensors(self) -> None:
        """
        Check whether any registered sensor has exceeded its
        offline grace period.

        Offline detection is intentionally separate from value-based
        rule evaluation.

        Example:

            vibration sensor
            sampling interval = 10 sec
            offline grace = 60 sec

        If no event arrives for > 60 seconds:

            SENSOR_OFFLINE

        may be generated.
        """

        try:
            offline_events = (
                self.offline_detector.check_offline_sensors()
            )

            if offline_events is None:
                return

            if not isinstance(offline_events, list):
                offline_events = [offline_events]

            for evaluation in offline_events:

                if evaluation is None:
                    continue

                if not evaluation.triggered:
                    continue

                # Deduplication also applies to offline alerts.
                if not self.deduplication_manager.should_emit(
                    evaluation
                ):
                    self.alerts_suppressed += 1
                    continue

                alert = self.alert_manager.process_evaluation(
                    evaluation
                )

                if alert is None:
                    continue

                self.alerts_generated += 1

                self.alert_store.save_alert(alert)

                self.websocket_manager.publish_alert(alert)

                logger.warning(
                    "OFFLINE ALERT | device=%s",
                    evaluation.device_id,
                )

        except Exception:
            self.processing_errors += 1

            logger.exception(
                "Error during offline sensor check"
            )

    # -----------------------------------------------------------------------
    # Shutdown
    # -----------------------------------------------------------------------

    def shutdown(self) -> None:
        """
        Gracefully shut down the alert engine.
        """

        if not self.running:
            return

        logger.info("Shutting down Alert Engine...")

        self.running = False

        # Allow components to clean up resources if implemented.

        try:
            close = getattr(
                self.websocket_manager,
                "close",
                None,
            )

            if close:
                close()

        except Exception:
            logger.exception(
                "Error while closing WebSocket manager"
            )

        try:
            close = getattr(
                self.alert_store,
                "close",
                None,
            )

            if close:
                close()

        except Exception:
            logger.exception(
                "Error while closing alert store"
            )

        self.print_metrics()

        logger.info("Alert Engine stopped.")

    # -----------------------------------------------------------------------
    # Metrics
    # -----------------------------------------------------------------------

    def print_metrics(self) -> None:
        elapsed = max(
            time.time() - self.started_at,
            0.001,
        )

        events_per_second = (
            self.events_processed / elapsed
        )

        logger.info(
            "================================================"
        )
        logger.info(
            "Alert Engine Metrics"
        )
        logger.info(
            "================================================"
        )
        logger.info(
            "Events processed : %s",
            self.events_processed,
        )
        logger.info(
            "Alerts generated : %s",
            self.alerts_generated,
        )
        logger.info(
            "Alerts suppressed: %s",
            self.alerts_suppressed,
        )
        logger.info(
            "Processing errors : %s",
            self.processing_errors,
        )
        logger.info(
            "Events/sec        : %.2f",
            events_per_second,
        )
        logger.info(
            "================================================"
        )


# ===========================================================================
# JSON Reader
# ===========================================================================


def read_json_file(path: Path) -> Iterable[Dict[str, Any]]:
    """
    Read JSON or JSONL sensor events.

    Supported formats:

    1. JSON Lines:

        {...}
        {...}
        {...}

    2. JSON array:

        [
            {...},
            {...}
        ]

    This makes the reader tolerant of the current generated data format.
    """

    try:
        with path.open(
            "r",
            encoding="utf-8",
        ) as file:

            content = file.read().strip()

        if not content:
            return

        # ---------------------------------------------------------------
        # JSON array
        # ---------------------------------------------------------------

        if content.startswith("["):

            data = json.loads(content)

            if not isinstance(data, list):
                logger.warning(
                    "Expected JSON array: %s",
                    path,
                )
                return

            for event in data:

                if isinstance(event, dict):
                    yield event

            return

        # ---------------------------------------------------------------
        # JSON Lines / multiple JSON objects
        # ---------------------------------------------------------------

        for line_number, line in enumerate(
            content.splitlines(),
            start=1,
        ):

            line = line.strip()

            if not line:
                continue

            try:

                event = json.loads(line)

                if isinstance(event, dict):
                    yield event
                else:
                    logger.warning(
                        "Skipping non-object JSON at %s:%s",
                        path,
                        line_number,
                    )

            except json.JSONDecodeError:

                logger.exception(
                    "Invalid JSON at %s:%s",
                    path,
                    line_number,
                )

    except OSError:

        logger.exception(
            "Unable to read input file: %s",
            path,
        )


def discover_json_files(
    input_path: Path,
) -> list[Path]:
    """
    Discover JSON files recursively.

    Example:

        data/raw/
          year=2026/
            month=08/
              day=04/
                hour=05/
                  batch_x.json
    """

    if input_path.is_file():
        return [input_path]

    if not input_path.exists():

        raise FileNotFoundError(
            f"Input path does not exist: {input_path}"
        )

    files = sorted(
        input_path.rglob("*.json")
    )

    logger.info(
        "Discovered %d JSON files",
        len(files),
    )

    return files


# ===========================================================================
# Component Factory
# ===========================================================================


def create_alert_engine(
    rule_config_path: Optional[str] = None,
) -> AlertEngine:
    """
    Build the alert engine and all of its dependencies.

    Keeping construction here makes main() clean and also makes
    unit testing easier.
    """

    # -----------------------------------------------------------------------
    # Rule engine
    # -----------------------------------------------------------------------

    rule_engine = RuleEngine(
        rule_config_path=rule_config_path
    )

    # -----------------------------------------------------------------------
    # Offline detector
    # -----------------------------------------------------------------------

    offline_detector = OfflineDetector(
        rule_engine=rule_engine
    )

    # -----------------------------------------------------------------------
    # Deduplication
    # -----------------------------------------------------------------------

    deduplication_manager = DeduplicationManager()

    # -----------------------------------------------------------------------
    # Alert lifecycle
    # -----------------------------------------------------------------------

    alert_manager = AlertManager()

    # -----------------------------------------------------------------------
    # Persistence
    # -----------------------------------------------------------------------

    alert_store = AlertStore()

    # -----------------------------------------------------------------------
    # Real-time frontend communication
    # -----------------------------------------------------------------------

    websocket_manager = WebSocketManager()

    # -----------------------------------------------------------------------
    # Complete engine
    # -----------------------------------------------------------------------

    return AlertEngine(
        rule_engine=rule_engine,
        offline_detector=offline_detector,
        deduplication_manager=deduplication_manager,
        alert_manager=alert_manager,
        alert_store=alert_store,
        websocket_manager=websocket_manager,
    )


# ===========================================================================
# Signal Handling
# ===========================================================================


def install_signal_handlers(
    engine: AlertEngine,
) -> None:
    """
    Handle Ctrl+C / termination gracefully.
    """

    def handle_signal(
        signum: int,
        frame: Any,
    ) -> None:

        logger.info(
            "Shutdown signal received: %s",
            signum,
        )

        engine.shutdown()

        sys.exit(0)

    signal.signal(
        signal.SIGINT,
        handle_signal,
    )

    signal.signal(
        signal.SIGTERM,
        handle_signal,
    )


# ===========================================================================
# Main
# ===========================================================================


def main() -> None:

    parser = argparse.ArgumentParser(
        description="Solar Manufacturing IoT Alert Engine"
    )

    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_PATH,
        help="JSON file or directory containing sensor data",
    )

    parser.add_argument(
        "--rules",
        default=DEFAULT_RULE_CONFIG,
        help="Path to alert rule configuration",
    )

    parser.add_argument(
        "--offline-check-interval",
        type=int,
        default=DEFAULT_OFFLINE_CHECK_INTERVAL_SECONDS,
        help="Seconds between offline sensor checks",
    )

    parser.add_argument(
        "--follow",
        action="store_true",
        help=(
            "Keep monitoring the input directory for "
            "new JSON files"
        ),
    )

    args = parser.parse_args()

    input_path = Path(args.input)

    logger.info(
        "================================================"
    )
    logger.info(
        "Solar Manufacturing IoT Alert Engine"
    )
    logger.info(
        "================================================"
    )

    logger.info(
        "Input : %s",
        input_path,
    )

    logger.info(
        "Rules : %s",
        args.rules,
    )

    # -----------------------------------------------------------------------
    # Create engine
    # -----------------------------------------------------------------------

    engine = create_alert_engine(
        rule_config_path=args.rules
    )

    install_signal_handlers(engine)

    # -----------------------------------------------------------------------
    # Track processed files
    # -----------------------------------------------------------------------

    processed_files: set[Path] = set()

    last_offline_check = time.monotonic()

    # -----------------------------------------------------------------------
    # Processing loop
    # -----------------------------------------------------------------------

    try:

        while engine.running:

            files = discover_json_files(
                input_path
            )

            new_files = [
                path
                for path in files
                if path not in processed_files
            ]

            # ---------------------------------------------------------------
            # Process new files
            # ---------------------------------------------------------------

            for path in new_files:

                logger.info(
                    "Processing file: %s",
                    path,
                )

                event_count = 0

                for event in read_json_file(path):

                    engine.process_event(event)

                    event_count += 1

                processed_files.add(path)

                logger.info(
                    "Finished %s | events=%d",
                    path,
                    event_count,
                )

            # ---------------------------------------------------------------
            # Offline sensor monitoring
            # ---------------------------------------------------------------

            now = time.monotonic()

            if (
                now - last_offline_check
                >= args.offline_check_interval
            ):

                engine.check_offline_sensors()

                last_offline_check = now

            # ---------------------------------------------------------------
            # One-shot mode
            # ---------------------------------------------------------------

            if not args.follow:

                break

            # ---------------------------------------------------------------
            # Follow mode
            # ---------------------------------------------------------------

            time.sleep(1)

    except KeyboardInterrupt:

        logger.info(
            "Keyboard interrupt received."
        )

    finally:

        engine.shutdown()


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    main()