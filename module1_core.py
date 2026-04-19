"""
module1_core.py
TradingView Auto Screenshot System — Core Engine
Linux / Cloud Edition (Oracle Cloud, Raspberry Pi, any Linux server)

Changes from original Linux version:
- Added: on_drive_sync callback  — fired after each successful screenshot
                                    DriveManager.sync_file() hook
- Added: on_telegram callback    — fired after Drive sync
                                    TelegramSender.send_screenshot() hook
- Added: drive_name / drive_label passed through ScreenshotResult
- CoreEngine._on_candle_close()  — now calls Drive + Telegram after capture
- PlaywrightEngine.capture_one() — on_save receives full ScreenshotResult
                                    then fires on_drive_sync + on_telegram
- TV_SESSION_JSON env var support — injects Playwright session cookies/storage
  before screenshot so private indicators are visible

Playwright mode is the ONLY screenshot mode on Linux.
Set "screenshot_mode": "playwright" in config.json (already default).

Requirements:
    pip install playwright pillow pytz requests
    playwright install chromium
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────

def setup_logger(log_folder: str = "./logs") -> logging.Logger:
    """Setup logger with file and console handlers."""
    Path(log_folder).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_folder) / "core.log"

    logger = logging.getLogger("tv_screenshot.core")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger


logger = setup_logger()


# ─────────────────────────────────────────────
# CONFIG LOADER
# ─────────────────────────────────────────────

def load_config(config_path: str = "./config.json") -> dict:
    """Load config.json. Returns defaults if file not found."""
    defaults: dict = {
        "global_limit_gb": 1,
        "per_symbol_limit_mb": 150,
        "delay_after_close_sec": 5,
        "retry_count": 3,
        "retry_delay_sec": 2,
        "screenshot_folder": "./screenshots",
        "log_folder": "./logs",
        "notification_popup": False,
        "health_check_interval_min": 60,
        "watchdog_interval_sec": 30,
        "freeze_detection": True,
        "disk_warning_gb": 0.2,
        "screenshot_mode": "playwright",
        "playwright_wait_sec": 15,
        "playwright_headless": True,
        "stocks": [],
        # Drive / Telegram (used by callbacks injected from module6)
        "drives": [],
        "telegram_bot_token": "",
        "telegram_chat_id": "",
    }
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
            defaults.update(loaded)
            if sys.platform != "win32":
                defaults["playwright_headless"] = loaded.get("playwright_headless", True)
            logger.debug("Config loaded from %s", config_path)
    except FileNotFoundError:
        logger.warning("Config file not found at %s, using defaults", config_path)
    except json.JSONDecodeError as exc:
        logger.error("Config JSON parse error: %s, using defaults", exc)
    return defaults


# ─────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────

@dataclass
class ScreenshotResult:
    """Result of a screenshot attempt."""
    success: bool
    symbol: str
    market: str
    filepath: Optional[str] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    is_duplicate: bool = False
    tag: str = ""
    # Drive info — populated by on_drive_sync callback
    drive_name: str = ""
    drive_label: str = ""
    drive_remote_path: str = ""
    drive_sync_ok: bool = False
    # Telegram — populated by on_telegram callback
    telegram_sent: bool = False


# ─────────────────────────────────────────────
# DPI AWARENESS (no-op on Linux)
# ─────────────────────────────────────────────

def enable_dpi_awareness() -> None:
    """No-op on Linux. DPI awareness is Windows-only."""
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.shcore.SetProcessDpiAwareness(2)
            logger.debug("DPI awareness set: per-monitor (Windows)")
        except Exception as exc:
            logger.warning("Could not set DPI awareness: %s", exc)
    else:
        logger.debug("DPI awareness: skipped (Linux/non-Windows platform)")


# ─────────────────────────────────────────────
# FILE PATH BUILDER
# ─────────────────────────────────────────────

def _build_filepath_simple(
    base_folder: str,
    symbol: str,
    market: str,
    dt: datetime,
    tag: str = "",
) -> Path:
    """
    Build screenshot file path.
    Structure: base_folder/MARKET/SYMBOL/YYYY-MM/SYMBOL_YYYY-MM-DD_HH-MM[_TAG].png
    """
    month_folder = dt.strftime("%Y-%m")
    time_str = dt.strftime("%Y-%m-%d_%H-%M")
    tag_str = f"_{tag}" if tag else ""
    filename = f"{symbol}_{time_str}{tag_str}.png"
    folder = Path(base_folder) / market / symbol / month_folder
    folder.mkdir(parents=True, exist_ok=True)
    return folder / filename


# Keep alias for compatibility
build_filepath = _build_filepath_simple


# ─────────────────────────────────────────────
# IMAGE HASH (FREEZE DETECTION)
# ─────────────────────────────────────────────

_last_image_hash: Dict[str, str] = {}


def compute_image_hash(image_bytes: bytes) -> str:
    """Compute MD5 hash of image bytes for freeze detection."""
    return hashlib.md5(image_bytes).hexdigest()


def is_frozen_frame(symbol: str, image_bytes: bytes) -> bool:
    """
    Check if the screenshot is identical to the previous capture.
    Returns True if frozen (no change detected).
    """
    current_hash = compute_image_hash(image_bytes)
    last_hash = _last_image_hash.get(symbol)

    if last_hash and last_hash == current_hash:
        logger.warning("Freeze detected for %s — image unchanged", symbol)
        return True

    _last_image_hash[symbol] = current_hash
    return False


# ─────────────────────────────────────────────
# TV SESSION INJECTION (GitHub Secret: TV_SESSION_JSON)
# ─────────────────────────────────────────────

def _inject_tv_session(context: object, session_json: str) -> bool:
    """
    Inject TradingView session (cookies + localStorage) into Playwright context.

    session_json: JSON string from TV_SESSION_JSON GitHub Secret.
    Expected format:
        {
          "cookies": [...],           ← list of cookie dicts
          "origins": [                ← localStorage per origin
            {
              "origin": "https://www.tradingview.com",
              "localStorage": [{"name": "...", "value": "..."}]
            }
          ]
        }

    Returns True on success, False if session_json is empty/invalid.
    """
    if not session_json or not session_json.strip():
        logger.debug("TV_SESSION_JSON not set — skipping session injection")
        return False

    try:
        data = json.loads(session_json)
    except json.JSONDecodeError as exc:
        logger.warning("TV_SESSION_JSON is not valid JSON: %s", exc)
        return False

    try:
        # Add cookies
        cookies = data.get("cookies", [])
        if cookies:
            context.add_cookies(cookies)  # type: ignore[union-attr]
            logger.info("TV session: injected %d cookies", len(cookies))

        # Add localStorage/sessionStorage
        origins = data.get("origins", [])
        if origins:
            context.add_init_script(  # type: ignore[union-attr]
                _build_storage_init_script(origins)
            )
            logger.info("TV session: injected localStorage for %d origin(s)", len(origins))

        return True

    except Exception as exc:
        logger.warning("TV session injection failed: %s", exc)
        return False


def _build_storage_init_script(origins: List[dict]) -> str:
    """
    Build a JS init script that restores localStorage entries.

    Runs before each page load so TradingView sees the session data.
    """
    # Encode origins as JSON so we can embed safely
    origins_json = json.dumps(origins, ensure_ascii=False)
    script = f"""
(function() {{
  var origins = {origins_json};
  origins.forEach(function(o) {{
    if (window.location.origin !== o.origin) return;
    var items = o.localStorage || [];
    items.forEach(function(item) {{
      try {{ localStorage.setItem(item.name, item.value); }} catch(e) {{}}
    }});
  }});
}})();
"""
    return script


# ─────────────────────────────────────────────
# PLAYWRIGHT ENGINE (URL-based screenshot)
# ─────────────────────────────────────────────

class PlaywrightEngine:
    """
    Screenshot engine: opens Chromium via Playwright, loads each stock URL,
    waits for chart to load, takes screenshot, closes browser.

    Works on Linux headless servers (Oracle Cloud, Raspberry Pi, VPS).

    NEW in this version:
        on_drive_sync  — Callable[[ScreenshotResult], None]
                         fired after save, before Telegram
        on_telegram    — Callable[[ScreenshotResult], None]
                         fired after Drive sync

    Config keys:
        stocks               — list of {symbol, url, market}
        screenshot_folder    — root output folder
        playwright_wait_sec  — max seconds to wait for chart (default 15)
        playwright_headless  — True on server (default True on Linux)
        retry_count          — retries per stock (default 3)
        retry_delay_sec      — seconds between retries (default 2)
    """

    _CHART_READY_SELECTOR: str = 'div[class*="chart-container"]'
    _SPINNER_SELECTOR: str = 'div[class*="spinner"]'

    def __init__(self, config: dict) -> None:
        self.config = config
        self.stocks: List[dict] = config.get("stocks", [])
        self.base_folder: str = config.get("screenshot_folder", "./screenshots")
        self.wait_sec: int = int(config.get("playwright_wait_sec", 15))
        self.headless: bool = bool(config.get("playwright_headless", True))

        # Callbacks — set externally by module6_integration
        self._on_notify: Optional[Callable[[str, str], None]] = None
        self._on_drive_sync: Optional[Callable[[ScreenshotResult], None]] = None
        self._on_telegram: Optional[Callable[[ScreenshotResult], None]] = None

        # TV session from env (GitHub Secret)
        self._tv_session_json: str = os.environ.get("TV_SESSION_JSON", "")

        # Stocks JSON override from env (GitHub Secret: STOCKS_JSON)
        stocks_json_env = os.environ.get("STOCKS_JSON", "")
        if stocks_json_env:
            try:
                env_stocks = json.loads(stocks_json_env)
                if isinstance(env_stocks, list) and env_stocks:
                    self.stocks = env_stocks
                    logger.info(
                        "STOCKS_JSON env var loaded: %d stocks", len(self.stocks)
                    )
            except json.JSONDecodeError as exc:
                logger.warning("STOCKS_JSON env var is invalid JSON: %s", exc)

    def set_notify_callback(self, cb: Optional[Callable[[str, str], None]]) -> None:
        """Attach notification callback (title, message)."""
        self._on_notify = cb

    def set_drive_sync_callback(
        self, cb: Optional[Callable[[ScreenshotResult], None]]
    ) -> None:
        """
        Attach Drive sync callback.
        Fired after each successful screenshot save.
        Callback receives ScreenshotResult and should populate
        result.drive_name, result.drive_label, result.drive_sync_ok.
        """
        self._on_drive_sync = cb

    def set_telegram_callback(
        self, cb: Optional[Callable[[ScreenshotResult], None]]
    ) -> None:
        """
        Attach Telegram send callback.
        Fired after Drive sync callback.
        Callback receives ScreenshotResult (with drive_name/label populated).
        """
        self._on_telegram = cb

    def _notify(self, title: str, msg: str) -> None:
        """Fire notification callback safely."""
        if self._on_notify:
            try:
                self._on_notify(title, msg)
            except Exception:
                pass

    def _fire_drive_sync(self, result: ScreenshotResult) -> None:
        """Fire Drive sync callback safely."""
        if self._on_drive_sync and result.success and result.filepath:
            try:
                self._on_drive_sync(result)
            except Exception as exc:
                logger.warning("on_drive_sync callback error: %s", exc)

    def _fire_telegram(self, result: ScreenshotResult) -> None:
        """Fire Telegram callback safely."""
        if self._on_telegram and result.success and result.filepath:
            try:
                self._on_telegram(result)
            except Exception as exc:
                logger.warning("on_telegram callback error: %s", exc)

    def _get_profile_dir(self, profile: str) -> str:
        """
        Return a profile directory for Playwright browser context.
        On Linux: uses ~/.pw_tv_profiles/<profile_name>
        On Windows: uses %TEMP%/pw_tv_profiles/<profile_name>
        """
        safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", profile)
        if sys.platform == "win32":
            import tempfile
            base = os.path.join(tempfile.gettempdir(), "pw_tv_profiles", safe_name)
        else:
            base = os.path.join(str(Path.home()), ".pw_tv_profiles", safe_name)
        os.makedirs(base, exist_ok=True)
        return base

    def _remove_chrome_lock(self, profile_dir: str) -> None:
        """Remove Chrome SingletonLock files to avoid launch errors."""
        lock_files = [
            os.path.join(profile_dir, "SingletonLock"),
            os.path.join(profile_dir, "SingletonSocket"),
            os.path.join(profile_dir, "SingletonCookie"),
        ]
        for lock_path in lock_files:
            try:
                if os.path.exists(lock_path):
                    os.remove(lock_path)
                    logger.debug("Removed lock file: %s", lock_path)
            except OSError as exc:
                logger.warning("Could not remove lock file %s: %s", lock_path, exc)

    def _wait_for_chart(self, page: object, symbol: str) -> bool:
        """
        Wait for TradingView chart and indicators to finish loading.

        Steps:
          1. networkidle — all requests finished
          2. chart-container selector present in DOM
          3. spinner hidden (if any)
          4. 3-second buffer for indicator rendering
        """
        try:
            from playwright.sync_api import TimeoutError as PWTimeout  # type: ignore
        except ImportError:
            return False

        timeout_ms = self.wait_sec * 1000

        try:
            logger.info("  [%s] Waiting networkidle...", symbol)
            page.wait_for_load_state("networkidle", timeout=timeout_ms)  # type: ignore

            logger.info("  [%s] Waiting chart-container...", symbol)
            page.wait_for_selector(  # type: ignore
                self._CHART_READY_SELECTOR, timeout=timeout_ms
            )

            try:
                page.wait_for_selector(  # type: ignore
                    self._SPINNER_SELECTOR, state="hidden", timeout=5000
                )
            except PWTimeout:
                pass  # no spinner visible is fine

            logger.info("  [%s] Waiting 3s for indicators...", symbol)
            page.wait_for_timeout(3000)  # type: ignore
            return True

        except PWTimeout as exc:
            logger.warning("  [%s] Chart wait timeout: %s", symbol, exc)
            return False
        except Exception as exc:
            logger.warning("  [%s] Chart wait error: %s", symbol, exc)
            return False

    def capture_one(
        self,
        page: object,
        stock: dict,
        on_save: Optional[Callable[[ScreenshotResult], None]] = None,
        tag: str = "",
    ) -> ScreenshotResult:
        """
        Navigate to one stock URL, wait for chart, screenshot, save.
        After save: fire on_drive_sync → on_telegram callbacks.

        Args:
            page:    Playwright Page object
            stock:   Dict {symbol, url, market}
            on_save: Legacy storage callback(ScreenshotResult) — still supported
            tag:     Optional event tag (e.g. "NY_OPEN")
        """
        symbol: str = stock.get("symbol", "UNKNOWN").upper()
        url: str = stock.get("url", "")
        market: str = stock.get("market", "US").upper()
        retry_count: int = int(self.config.get("retry_count", 3))
        retry_delay: float = float(self.config.get("retry_delay_sec", 2))
        freeze_detection: bool = bool(self.config.get("freeze_detection", True))

        for attempt in range(1, retry_count + 1):
            try:
                logger.info(
                    "[%s] Opening URL (attempt %d/%d): %s",
                    symbol, attempt, retry_count, url,
                )
                page.goto(url, timeout=30_000, wait_until="domcontentloaded")  # type: ignore

                ready = self._wait_for_chart(page, symbol)
                if not ready:
                    logger.warning(
                        "[%s] Chart may not be fully loaded — proceeding anyway",
                        symbol,
                    )

                now = datetime.now()
                filepath = _build_filepath_simple(
                    self.base_folder, symbol, market, now, tag
                )

                # Screenshot to bytes first for freeze detection
                screenshot_bytes: bytes = page.screenshot(full_page=False)  # type: ignore

                # Freeze detection
                if freeze_detection and is_frozen_frame(symbol, screenshot_bytes):
                    self._notify(
                        f"Freeze? {symbol}",
                        "TradingView chart may not have updated",
                    )

                # Save to file
                with open(str(filepath), "wb") as fh:
                    fh.write(screenshot_bytes)

                logger.info("[%s] Saved: %s", symbol, filepath)

                result = ScreenshotResult(
                    success=True,
                    symbol=symbol,
                    market=market,
                    filepath=str(filepath),
                    timestamp=now,
                    tag=tag,
                )

                # ── 1. Legacy on_save (StorageManager) ──────────────
                if on_save:
                    try:
                        on_save(result)
                    except Exception as exc:
                        logger.warning("on_save callback error [%s]: %s", symbol, exc)

                # ── 2. Drive sync ─────────────────────────────────────
                self._fire_drive_sync(result)

                # ── 3. Telegram ───────────────────────────────────────
                self._fire_telegram(result)

                self._notify(
                    f"OK {symbol}",
                    f"Screenshot saved — {now.strftime('%H:%M')}",
                )
                return result

            except Exception as exc:
                logger.warning("[%s] Attempt %d failed: %s", symbol, attempt, exc)
                if attempt < retry_count:
                    time.sleep(retry_delay)

        logger.error("[%s] All %d attempts failed", symbol, retry_count)
        self._notify(f"FAIL {symbol}", "Screenshot failed after retries")
        return ScreenshotResult(
            success=False,
            symbol=symbol,
            market=market,
            error=f"Failed after {retry_count} attempts",
        )

    def run_all(
        self,
        on_save: Optional[Callable[[ScreenshotResult], None]] = None,
        tag: str = "",
    ) -> List[ScreenshotResult]:
        """
        Main entry: open Chromium → loop all stocks → screenshot → close.

        Groups stocks by chrome_profile to minimise browser launches.
        Injects TV_SESSION_JSON before first navigation.

        Args:
            on_save: Legacy storage callback fired per screenshot
            tag:     Event tag applied to all screenshots this run
        """
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
            from collections import defaultdict
        except ImportError:
            logger.error(
                "Playwright not installed. "
                "Run: pip install playwright && playwright install chromium"
            )
            return []

        if not self.stocks:
            logger.warning("PlaywrightEngine: no stocks in config.json")
            return []

        results: List[ScreenshotResult] = []
        logger.info(
            "=== PlaywrightEngine start — %d stocks (headless=%s) ===",
            len(self.stocks), self.headless,
        )

        # Group stocks by profile
        profile_groups: Dict[str, List[dict]] = defaultdict(list)
        for stock in self.stocks:
            profile = stock.get("chrome_profile", "Default")
            profile_groups[profile].append(stock)

        # Chromium launch args — optimised for headless server
        launch_args: List[str] = [
            "--window-size=1920,1080",
            "--disable-notifications",
            "--disable-popup-blocking",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-default-apps",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox",
        ]

        with sync_playwright() as pw:
            for profile, stock_list in profile_groups.items():
                logger.info(
                    "Opening browser profile='%s' (%d stocks)",
                    profile, len(stock_list),
                )

                profile_dir = self._get_profile_dir(profile)
                self._remove_chrome_lock(profile_dir)

                launch_kwargs: dict = {
                    "user_data_dir": profile_dir,
                    "headless": self.headless,
                    "args": launch_args,
                    "viewport": {"width": 1920, "height": 1080},
                    "ignore_default_args": ["--enable-automation"],
                }
                if sys.platform == "win32":
                    launch_kwargs["channel"] = "chrome"

                for attempt_browser in range(1, 3):
                    try:
                        context = pw.chromium.launch_persistent_context(
                            **launch_kwargs
                        )

                        # Inject TV session before any navigation
                        if self._tv_session_json:
                            _inject_tv_session(context, self._tv_session_json)

                        page = context.new_page()

                        for stock in stock_list:
                            result = self.capture_one(
                                page, stock, on_save=on_save, tag=tag
                            )
                            results.append(result)

                        context.close()
                        logger.info("Browser closed — profile='%s'", profile)
                        break  # success

                    except Exception as exc:
                        logger.error(
                            "Browser error profile='%s' attempt %d: %s",
                            profile, attempt_browser, exc,
                        )
                        self._remove_chrome_lock(profile_dir)
                        if attempt_browser == 2:
                            for stock in stock_list:
                                results.append(ScreenshotResult(
                                    success=False,
                                    symbol=stock.get("symbol", "UNKNOWN"),
                                    market=stock.get("market", "US"),
                                    error=f"Browser launch failed: {exc}",
                                ))

        ok = sum(1 for r in results if r.success)
        logger.info("=== PlaywrightEngine done: %d/%d OK ===", ok, len(results))
        return results


# ─────────────────────────────────────────────
# HOURLY SCHEDULER
# ─────────────────────────────────────────────

class HourlyScheduler:
    """
    Fires callback at each 1H candle close (top of the hour + delay_sec).
    Runs in a background daemon thread.
    """

    def __init__(
        self,
        callback: Callable[[datetime], None],
        delay_sec: int = 5,
    ) -> None:
        self.callback = callback
        self.delay_sec = delay_sec
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def _seconds_until_next_hour(self) -> float:
        """Seconds until the top of the next hour."""
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(
            minute=0, second=0, microsecond=0
        )
        return (next_hour - now).total_seconds()

    def _run(self) -> None:
        """Scheduler loop — sleeps until next candle close then fires callback."""
        logger.info("HourlyScheduler started")
        while not self._stop_event.is_set():
            wait = self._seconds_until_next_hour() + self.delay_sec
            logger.info(
                "Next candle close in %.0f seconds (%.1f min)",
                wait, wait / 60,
            )

            deadline = time.monotonic() + wait
            while time.monotonic() < deadline:
                if self._stop_event.is_set():
                    return
                time.sleep(min(10, deadline - time.monotonic()))

            candle_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            logger.info(
                "Candle close: %s — firing callback",
                candle_time.strftime("%Y-%m-%d %H:%M"),
            )
            try:
                self.callback(candle_time)
            except Exception as exc:
                logger.error("Scheduler callback error: %s", exc)

    def start(self) -> None:
        """Start scheduler in background thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="HourlyScheduler"
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop scheduler."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=15)
        logger.info("HourlyScheduler stopped")


# ─────────────────────────────────────────────
# WATCHDOG
# ─────────────────────────────────────────────

class Watchdog:
    """
    Monitors that captures are still happening.
    Fires alert if no capture within max_silence_sec (default 2h).
    """

    def __init__(
        self,
        check_interval_sec: int = 30,
        max_silence_sec: int = 7200,
        on_alert: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.check_interval_sec = check_interval_sec
        self.max_silence_sec = max_silence_sec
        self._on_alert = on_alert
        self._last_capture_time: datetime = datetime.now()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def report_capture(self) -> None:
        """Call after every successful capture to reset watchdog timer."""
        self._last_capture_time = datetime.now()

    def _run(self) -> None:
        """Watchdog loop."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self.check_interval_sec)
            if self._stop_event.is_set():
                continue
            elapsed = (datetime.now() - self._last_capture_time).total_seconds()
            if elapsed > self.max_silence_sec:
                msg = (
                    f"Watchdog: No capture for {elapsed / 3600:.1f}h"
                    " — system may be stuck"
                )
                logger.warning(msg)
                if self._on_alert:
                    self._on_alert(msg)

    def start(self) -> None:
        """Start watchdog in background thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="Watchdog"
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop watchdog."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Watchdog stopped")


# ─────────────────────────────────────────────
# HEALTH CHECKER
# ─────────────────────────────────────────────

class HealthChecker:
    """Logs health status to file every interval_min minutes."""

    def __init__(
        self,
        interval_min: int = 60,
        log_folder: str = "./logs",
    ) -> None:
        self.interval_min = interval_min
        self.log_folder = log_folder
        self._capture_count: int = 0
        self._fail_count: int = 0
        self._drive_sync_ok: int = 0
        self._telegram_sent: int = 0
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._start_time = datetime.now()

    def record_success(self) -> None:
        """Record a successful capture."""
        self._capture_count += 1

    def record_failure(self) -> None:
        """Record a failed capture."""
        self._fail_count += 1

    def record_drive_sync(self, ok: bool) -> None:
        """Record a Drive sync result."""
        if ok:
            self._drive_sync_ok += 1

    def record_telegram(self, sent: bool) -> None:
        """Record a Telegram send result."""
        if sent:
            self._telegram_sent += 1

    def _write_health_log(self) -> None:
        """Write health JSON to health.log."""
        Path(self.log_folder).mkdir(parents=True, exist_ok=True)
        health_file = Path(self.log_folder) / "health.log"
        uptime = datetime.now() - self._start_time
        status = {
            "timestamp": datetime.now().isoformat(),
            "uptime_hours": round(uptime.total_seconds() / 3600, 2),
            "total_captures": self._capture_count,
            "total_failures": self._fail_count,
            "drive_syncs_ok": self._drive_sync_ok,
            "telegram_sent": self._telegram_sent,
            "status": (
                "OK"
                if self._fail_count < max(self._capture_count * 0.1, 1)
                else "DEGRADED"
            ),
        }
        with open(health_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(status) + "\n")
        logger.info(
            "Health: captures=%d failures=%d drive_ok=%d tg_sent=%d uptime=%.1fh",
            self._capture_count,
            self._fail_count,
            self._drive_sync_ok,
            self._telegram_sent,
            uptime.total_seconds() / 3600,
        )

    def _run(self) -> None:
        """Health check loop."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self.interval_min * 60)
            if not self._stop_event.is_set():
                self._write_health_log()

    def start(self) -> None:
        """Start health checker in background thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="HealthChecker"
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop health checker."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)


# ─────────────────────────────────────────────
# CORE ENGINE
# ─────────────────────────────────────────────

class CoreEngine:
    """
    Main Core Engine — orchestrates all Module 1 components.

    Callbacks injected by module6_integration:
        on_notify        Callable[[str, str], None]     — GUI/log notification
        on_save          Callable[[ScreenshotResult], None] — StorageManager
        on_market_check  Callable[[str, datetime], bool]   — module2 market check
        on_drive_sync    Callable[[ScreenshotResult], None] — module7 Drive upload
        on_telegram      Callable[[ScreenshotResult], None] — Telegram send

    Usage:
        engine = CoreEngine()
        engine.start()          # blocks in foreground via scheduler
        engine.stop()           # graceful shutdown
        engine.test_screenshot() # one-shot manual run
    """

    def __init__(self, config_path: str = "./config.json") -> None:
        self.config = load_config(config_path)
        enable_dpi_awareness()

        self.scheduler = HourlyScheduler(
            callback=self._on_candle_close,
            delay_sec=self.config.get("delay_after_close_sec", 5),
        )
        self.watchdog = Watchdog(
            check_interval_sec=self.config.get("watchdog_interval_sec", 30),
        )
        self.health_checker = HealthChecker(
            interval_min=self.config.get("health_check_interval_min", 60),
            log_folder=self.config.get("log_folder", "./logs"),
        )

        # ── Callbacks (set by module6_integration) ────────────────────
        self.on_notify: Optional[Callable[[str, str], None]] = None
        self.on_save: Optional[Callable[[ScreenshotResult], None]] = None
        self.on_market_check: Optional[Callable[[str, datetime], bool]] = None
        self.on_drive_sync: Optional[Callable[[ScreenshotResult], None]] = None
        self.on_telegram: Optional[Callable[[ScreenshotResult], None]] = None

        logger.info("CoreEngine initialized (platform: %s)", sys.platform)

    def _build_pw_engine(self, config_override: Optional[dict] = None) -> PlaywrightEngine:
        """Create a PlaywrightEngine with all current callbacks wired.
        
        Parameters
        ----------
        config_override : dict, optional
            If provided, use this config instead of self.config (e.g. to pass
            a filtered stocks list for a specific candle-close run).
        """
        pw_engine = PlaywrightEngine(config_override if config_override is not None else self.config)
        pw_engine.set_notify_callback(self.on_notify)
        pw_engine.set_drive_sync_callback(self.on_drive_sync)
        pw_engine.set_telegram_callback(self.on_telegram)
        return pw_engine

    def _on_candle_close(self, candle_time: datetime) -> None:
        """Called by HourlyScheduler at each 1H candle close."""
        logger.info("Candle close: %s", candle_time.strftime("%Y-%m-%d %H:%M"))

        # Market check per stock — skip US if market closed
        active_stocks: List[dict] = []
        for stock in self.config.get("stocks", []):
            market = stock.get("market", "CRYPTO").upper()
            if market != "CRYPTO" and self.on_market_check:
                if not self.on_market_check(market, candle_time):
                    logger.info(
                        "Skipping %s — market closed (%s)",
                        stock.get("symbol", "?"), market,
                    )
                    continue
            active_stocks.append(stock)

        if not active_stocks:
            logger.info("No active stocks at %s — skipping run",
                        candle_time.strftime("%H:%M"))
            return

        # Temporarily override stocks in config for this run
        run_config = dict(self.config)
        run_config["stocks"] = active_stocks

        pw_engine = self._build_pw_engine(run_config)

        results = pw_engine.run_all(on_save=self.on_save)

        for result in results:
            if result.success:
                self.health_checker.record_success()
                self.health_checker.record_drive_sync(result.drive_sync_ok)
                self.health_checker.record_telegram(result.telegram_sent)
                self.watchdog.report_capture()
            else:
                self.health_checker.record_failure()

    def start(self) -> None:
        """Start all background services."""
        logger.info("CoreEngine starting...")
        self.health_checker.start()
        self.watchdog.start()
        self.scheduler.start()
        logger.info("CoreEngine running — waiting for next candle close")

    def stop(self) -> None:
        """Stop all background services."""
        logger.info("CoreEngine stopping...")
        self.scheduler.stop()
        self.watchdog.stop()
        self.health_checker.stop()
        logger.info("CoreEngine stopped")

    def test_screenshot(self) -> List[ScreenshotResult]:
        """
        Manually trigger screenshot of all stocks immediately.
        Used for CLI testing (python main.py --screenshot) and smoke tests.
        All callbacks (Drive, Telegram) are fired normally.
        """
        logger.info("Test screenshot triggered")
        pw_engine = self._build_pw_engine()
        return pw_engine.run_all(on_save=self.on_save)


# ─────────────────────────────────────────────
# ENTRY POINT (standalone test)
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  TradingView Auto Screenshot — Module 1 Core Engine (Linux)")
    print("=" * 60)

    engine = CoreEngine()

    print("\n[TEST] Running test screenshot for all configured stocks...")
    results = engine.test_screenshot()
    for r in results:
        if r.success:
            print(f"  OK  {r.symbol} -> {r.filepath}")
            if r.drive_sync_ok:
                print(f"       Drive: {r.drive_name} [{r.drive_label}] -> {r.drive_remote_path}")
            if r.telegram_sent:
                print("       Telegram: sent")
        else:
            print(f"  ERR {r.symbol} -> {r.error}")

    print("\nModule 1 test complete.")
