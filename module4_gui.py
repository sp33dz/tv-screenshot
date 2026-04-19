"""
module4_gui.py — Headless Server Edition
TradingView Auto Screenshot System

This is the Linux/Cloud replacement for the Windows GUI module.
On a headless server there is no system tray, no tkinter window, and no
Windows notifications.  This module provides a compatible API stub so
module6_integration can import and wire it without changes.

What's provided:
    - GUIController (start/stop/show_notification — all logging-based)
    - save_config / load_config helpers
    - No pystray, no tkinter, no plyer, no winreg
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from module1_core import CoreEngine
    from module3_storage import StorageManager

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
CONFIG_PATH = Path("config.json")
APP_NAME = "TradingView AutoShot (Linux/Headless)"

DEFAULT_CONFIG: dict = {
    "screenshot_folder": "./screenshots",
    "global_limit_gb": 1.0,
    "per_symbol_limit_mb": 150,
    "delay_after_close_sec": 5,
    "markets": {"CRYPTO": True, "US": True},
    "timezone": "America/New_York",
    "notification_popup": False,
    "notification_duration_sec": 0,
    "autostart": False,
    "screenshot_mode": "playwright",
    "playwright_headless": True,
}


# ─────────────────────────────────────────────
# Config helpers
# ─────────────────────────────────────────────

def load_config() -> dict:
    """Load config.json; return DEFAULT_CONFIG if missing/corrupt."""
    try:
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            for k, v in DEFAULT_CONFIG.items():
                data.setdefault(k, v)
            # Normalise markets field (list → dict)
            if isinstance(data.get("markets"), list):
                mlist = data["markets"]
                data["markets"] = {
                    "CRYPTO": "CRYPTO" in mlist,
                    "US": "US" in mlist,
                }
            elif not isinstance(data.get("markets"), dict):
                data["markets"] = {"CRYPTO": True, "US": True}
            return data
    except Exception as exc:
        logger.warning("Config load failed (%s), using defaults.", exc)
    return dict(DEFAULT_CONFIG)


def save_config(cfg: dict) -> None:
    """Persist config dict to config.json."""
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        logger.info("Config saved → %s", CONFIG_PATH)
    except Exception as exc:
        logger.error("Config save failed: %s", exc)
        raise


# ─────────────────────────────────────────────
# Notification (log-based on server)
# ─────────────────────────────────────────────

def popup_notification(
    title: str,
    message: str,
    duration_sec: int = 0,
) -> None:
    """
    On headless Linux: log the notification instead of showing a popup.
    On Windows: use plyer if available; otherwise log.
    """
    if sys.platform == "win32":
        try:
            from plyer import notification as plyer_notification  # type: ignore
            plyer_notification.notify(
                title=title,
                message=message,
                app_name=APP_NAME,
                timeout=duration_sec or 4,
            )
            return
        except Exception:
            pass
    logger.info("NOTIFY | %s | %s", title, message)


# ─────────────────────────────────────────────
# GUIController — headless stub
# ─────────────────────────────────────────────

class GUIController:
    """
    Headless GUIController for Linux/Cloud servers.

    On Windows this class managed the system tray, settings window,
    and notifications.  On Linux it:
      - starts CoreEngine in the background
      - logs all notification events
      - keeps the main thread alive with a blocking loop
      - provides the same public API as the Windows version

    API:
        gc = GUIController(core_engine, storage_manager)
        gc.start()                        # blocking — runs until Ctrl+C
        gc.show_notification(title, msg)  # logs to console
        gc.set_autostart(bool)            # no-op on Linux
    """

    def __init__(
        self,
        core_engine: Optional[object] = None,
        storage_manager: Optional[object] = None,
    ) -> None:
        self._engine = core_engine
        self._storage = storage_manager
        self._stop_event = threading.Event()
        logger.info("GUIController initialised (headless Linux mode)")

    def show_notification(self, title: str, message: str) -> None:
        """Show notification — logged on server, system popup on Windows."""
        popup_notification(title, message)

    def set_autostart(self, enabled: bool) -> None:
        """
        Set process to auto-start on boot.
        On Linux this is handled via crontab / systemd — see SETUP.md.
        On Windows uses Task Scheduler via module6_integration.
        """
        if sys.platform != "win32":
            logger.info(
                "set_autostart(%s): on Linux use crontab or systemd — see SETUP.md",
                enabled,
            )
        else:
            logger.info("set_autostart(%s): use Task Scheduler on Windows", enabled)

    def start(self) -> None:
        """
        Start CoreEngine (background threads) and block until stopped.

        Handles graceful shutdown on SIGINT (Ctrl+C) and SIGTERM.
        """
        import signal

        def _shutdown(signum: int, frame: object) -> None:
            logger.info("Signal %d received — shutting down gracefully...", signum)
            self._stop_event.set()

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        if self._engine is not None:
            try:
                self._engine.start()  # type: ignore[union-attr]
                logger.info("CoreEngine started — waiting for candle closes")
            except Exception as exc:
                logger.error("CoreEngine start failed: %s", exc)
                return

        logger.info(
            "%s running. Press Ctrl+C or send SIGTERM to stop.", APP_NAME
        )

        # Keep main thread alive
        while not self._stop_event.is_set():
            time.sleep(1)

        # Graceful stop
        if self._engine is not None:
            try:
                self._engine.stop()  # type: ignore[union-attr]
            except Exception as exc:
                logger.warning("CoreEngine stop error: %s", exc)

        logger.info("%s stopped.", APP_NAME)

    def stop(self) -> None:
        """Request graceful shutdown."""
        self._stop_event.set()
