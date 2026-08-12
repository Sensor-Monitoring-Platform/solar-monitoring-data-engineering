from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import WebSocket


logger = logging.getLogger(__name__)


class WebSocketManager:
    """
    Manages active WebSocket connections and broadcasts
    alert events to connected frontend clients.

    Important:
        This class is responsible ONLY for event delivery.

        It does not:
        - evaluate sensor rules
        - create alerts
        - resolve alerts
        - persist alerts
        - perform deduplication
    """

    def __init__(self) -> None:

        self._connections: set[WebSocket] = set()

        self._lock = asyncio.Lock()

    # ==========================================================
    # CONNECTION MANAGEMENT
    # ==========================================================

    async def connect(
        self,
        websocket: WebSocket,
    ) -> None:

        await websocket.accept()

        async with self._lock:
            self._connections.add(websocket)

        logger.info(
            "WebSocket client connected. "
            "Active clients=%d",
            len(self._connections),
        )

    async def disconnect(
        self,
        websocket: WebSocket,
    ) -> None:

        async with self._lock:
            self._connections.discard(websocket)

        logger.info(
            "WebSocket client disconnected. "
            "Active clients=%d",
            len(self._connections),
        )

    # ==========================================================
    # BROADCAST
    # ==========================================================

    async def broadcast(
        self,
        event: dict[str, Any],
    ) -> None:

        if not self._connections:
            return

        message = json.dumps(
            event,
            default=str,
        )

        async with self._lock:
            connections = list(self._connections)

        results = await asyncio.gather(
            *[
                self._send(
                    websocket,
                    message,
                )
                for websocket in connections
            ],
            return_exceptions=True,
        )

        for websocket, result in zip(
            connections,
            results,
        ):

            if isinstance(
                result,
                Exception,
            ):

                logger.warning(
                    "Removing failed WebSocket connection: %s",
                    result,
                )

                await self.disconnect(websocket)

    # ==========================================================
    # SEND TO SINGLE CLIENT
    # ==========================================================

    async def _send(
        self,
        websocket: WebSocket,
        message: str,
    ) -> None:

        await websocket.send_text(message)

    # ==========================================================
    # CONNECTION COUNT
    # ==========================================================

    @property
    def connection_count(self) -> int:

        return len(self._connections)