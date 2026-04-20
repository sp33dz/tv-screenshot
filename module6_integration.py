"""
module6_integration.py — Integration, Wiring & Self-Test
TradingView Auto Screenshot System — Linux / Cloud Edition

Changes from Windows version:
    - Removed: Windows Task Scheduler registration (schtasks.exe)
    - Added:   Linux crontab / systemd service file helpers
    - Removed: "diagnose_and_fix.bat" references
    - Added:   Linux setup instructions in error messages
    - Kept:    bootstrap(), run(), run_self_test(), print_health_summary()
    - Kept:    all module wiring logic (unchanged)
    - Module4 (GUI) is now the headless stub — no system tray

Changes from previous version (Account A task):
    - AppContext: added drive_manager, telegram_sender fields
    - bootstrap(): DriveManager.setup_rclone_from_env() called first
    - bootstrap(): DriveManager(config) instantiated
    - bootstrap(): TelegramSender.from_config(config) instantiated (None-safe)
    - bootstrap(): inject DriveManager + TelegramSender into CoreEngine
    - bootstrap(): register on_drive_sync callback (assign + sync + sidecar)
    - bootstrap(): register on_telegram callback (send_screenshot_to_telegram)
    - _import_modules(): added module7_drive + module_telegram
    - run_self_test(): added _test_module7, _test_module_telegram

Public API (unchanged signatures — main.py calls these):
    bootstrap(config_path)   -> AppContext
    run(config_path)                         # bootstrap + gui.start() (blocking)
    run_self_test()          -> bool          # smoke tests
    register_crontab()       -> bool          # add cron job on Linux
    unregister_crontab()     -> bool          # remove cron job
    write_systemd_service()  -> bool          # write .service file
    print_health_summary()                   # CLI health report
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional PIL
# ---------------------------------------------------------------------------

try:
    from PIL import Image  # type: ignore
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_CONFIG_PATH: str = "./config.json"
APP_NAME: str = "TVScreenshot"
CRON_MARKER: str = "# TVScreenshot-autostart"


# ---------------------------------------------------------------------------
# AppContext
# ---------------------------------------------------------------------------

@dataclass
class AppContext:
    """Holds references to every live module instance."""
    core_engine: Any
    storage_manager: Any
    gallery_manager: Any
    drive_manager: Any          # DriveManager | None
    telegram_sender: Any        # TelegramSender | None
    # Compatibility / legacy fields
    market_module: Any = None
    gui_controller: Any = None
    config: dict = field(default_factory=dict)
    config_path: str = DEFAULT_CONFIG_PATH


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_config(config_path: str) -> dict:
    """Load config.json with fallback defaults."""
    defaults: dict = {
        "global_limit_gb": 1,
        "per_symbol_limit_mb": 150,
        "delay_after_close_sec": 5,
        "retry_count": 3,
        "retry_delay_sec": 2,
        "markets": {"CRYPTO": True, "US": True},
        "screenshot_folder": "./screenshots",
        "gallery_folder": "./gallery",
        "log_folder": "./logs",
        "notification_popup": False,
        "notification_duration_sec": 0,
        "health_check_interval_min": 60,
        "watchdog_interval_sec": 30,
        "freeze_detection": True,
        "disk_warning_gb": 0.2,
        "auto_start_windows": False,
        "timezone": "America/New_York",
        "screenshot_mode": "playwright",
        "playwright_wait_sec": 15,
        "playwright_headless": True,
        "stocks": [],
        "drives": [],
        "telegram": {"enabled": False},
        "github_pages_url": "",
    }
    p = Path(config_path)
    if p.exists():
        try:
            user_cfg: dict = json.loads(p.read_text(encoding="utf-8"))
            defaults.update(user_cfg)
            logger.info("Config loaded from %s", p)
        except Exception as exc:
            logger.warning("Failed to parse %s: %s — using defaults", p, exc)
    else:
        logger.info("Config file not found at %s — using defaults", p)
    return defaults


def _setup_logging(config: dict) -> None:
    """Reconfigure root logger using log_folder from config."""
    log_folder = Path(config.get("log_folder", "./logs"))
    log_folder.mkdir(parents=True, exist_ok=True)
    log_file = log_folder / "app.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    logger.info("Logging initialised -> %s", log_file)


# ---------------------------------------------------------------------------
# Module imports
# ---------------------------------------------------------------------------

def _import_modules() -> dict:
    """Import all application modules. Missing modules -> None + warning."""
    modules: dict = {}

    _pip_hint: dict = {
        "PIL":         "Pillow",
        "pytz":        "pytz",
        "requests":    "requests",
        "imagehash":   "imagehash",
        "playwright":  "playwright",
        "cv2":         "opencv-python",
        "telegram":    "python-telegram-bot>=20.0",
    }

    def _try_import(name: str, path: str) -> None:
        import importlib.util
        import traceback as _tb

        try:
            spec = importlib.util.spec_from_file_location(name, path)
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                sys.modules[name] = mod
                try:
                    spec.loader.exec_module(mod)  # type: ignore[union-attr]
                except Exception:
                    sys.modules.pop(name, None)
                    raise
                modules[name] = mod
                logger.debug("Imported %s from %s", name, path)
            else:
                logger.warning("Cannot find spec for %s at %s", name, path)
                modules[name] = None
        except ModuleNotFoundError as exc:
            missing_pkg = exc.name or ""
            pip_name = _pip_hint.get(missing_pkg, missing_pkg)
            logger.error(
                "IMPORT FAILED: %s\n"
                "  Missing package : %s\n"
                "  Fix             : pip install %s",
                name, missing_pkg, pip_name,
            )
            modules[name] = None
        except Exception as exc:
            logger.error(
                "IMPORT FAILED: %s\n  Error: %s\n  %s",
                name, exc, _tb.format_exc(),
            )
            modules[name] = None

    base = Path(__file__).parent
    _try_import("module1_core",    str(base / "module1_core.py"))
    _try_import("module2_market",  str(base / "module2_market.py"))
    _try_import("module3_storage", str(base / "module3_storage.py"))
    _try_import("module4_gui",     str(base / "module4_gui.py"))
    _try_import("module5_gallery", str(base / "module5_gallery.py"))
    _try_import("module7_drive",   str(base / "module7_drive.py"))
    _try_import("module_telegram", str(base / "module_telegram.py"))

    return modules


# ---------------------------------------------------------------------------
# Gallery rebuild helper
# ---------------------------------------------------------------------------

_gallery_rebuild_lock = threading.Lock()
_gallery_rebuild_pending = threading.Event()


def _rebuild_gallery_safe(gallery_manager: object) -> None:
    """Rebuild gallery with debounce — swallow errors."""
    _gallery_rebuild_pending.set()
    if not _gallery_rebuild_lock.acquire(blocking=False):
        return
    try:
        while _gallery_rebuild_pending.is_set():
            _gallery_rebuild_pending.clear()
            try:
                gallery_manager.build()  # type: ignore[union-attr]
            except Exception as exc:
                logger.warning("Gallery rebuild failed: %s", exc)
    finally:
        _gallery_rebuild_lock.release()


# ---------------------------------------------------------------------------
# on_save callback builder
# ---------------------------------------------------------------------------

def _make_on_save(storage_manager: object, gallery_manager: object):
    """Build on_save callback wired into CoreEngine."""
    def on_save(result: object) -> None:
        try:
            img_attr = getattr(result, "image", None)
            fp = getattr(result, "filepath", None)

            # If no pre-loaded image, open from filepath — use context manager to
            # avoid leaving file handles open (memory/fd leak on long-running server)
            if img_attr is None and fp and PIL_AVAILABLE:
                try:
                    with Image.open(fp) as _img:
                        _img.load()  # force decode while file is open
                        img_attr = _img.copy()  # detach from file handle
                except Exception:
                    pass

            if img_attr is not None and storage_manager is not None:
                storage_manager.save_screenshot(  # type: ignore[union-attr]
                    image=img_attr,
                    symbol=result.symbol,       # type: ignore[attr-defined]
                    market=result.market,       # type: ignore[attr-defined]
                    dt=result.timestamp,        # type: ignore[attr-defined]
                    tag=result.tag,             # type: ignore[attr-defined]
                )

            if gallery_manager is not None:
                threading.Thread(
                    target=_rebuild_gallery_safe,
                    args=(gallery_manager,),
                    daemon=True,
                ).start()

        except Exception as exc:
            logger.error("on_save callback error: %s", exc, exc_info=True)

    return on_save


# ---------------------------------------------------------------------------
# on_drive_sync callback builder
# ---------------------------------------------------------------------------

def _make_on_drive_sync(drive_manager: object, write_sidecar_fn: Any):
    """
    Build on_drive_sync callback.

    Flow:
        1. assign_drive(symbol, market)
        2. sync_file(filepath, assignment, "screenshots")
        3. write_drive_sidecar(filepath, drive.name)
        4. populate result.drive_name / drive_label / drive_remote_path / drive_sync_ok
    """
    def on_drive_sync(result: object) -> None:
        symbol: str = getattr(result, "symbol", "")
        market: str = getattr(result, "market", "")
        filepath: Optional[str] = getattr(result, "filepath", None)

        if not filepath:
            logger.warning("on_drive_sync: no filepath in result for %s", symbol)
            return

        try:
            # 1. Assign drive
            assignment = drive_manager.assign_drive(symbol, market)  # type: ignore[union-attr]
            if assignment is None:
                logger.warning(
                    "on_drive_sync: no drive assigned for %s (%s) — skipping",
                    symbol, market,
                )
                return

            drive_name: str = assignment.drive.name
            drive_label: str = assignment.drive.label

            # 2. Sync file to Google Drive
            sync_result = drive_manager.sync_file(  # type: ignore[union-attr]
                filepath, assignment, "screenshots"
            )

            if sync_result.success:
                logger.info(
                    "Drive sync OK: %s -> %s [%s] remote=%s (%.2fs)",
                    symbol, drive_name, drive_label,
                    sync_result.remote_path, sync_result.duration_sec,
                )
            else:
                logger.warning(
                    "Drive sync FAILED: %s -> %s: %s",
                    symbol, drive_name, sync_result.error,
                )

            # 3. Write .drive sidecar for gallery filter
            if write_sidecar_fn is not None:
                try:
                    write_sidecar_fn(filepath, drive_name)
                except Exception as exc_sc:
                    logger.warning(
                        "write_drive_sidecar error for %s: %s", filepath, exc_sc
                    )

            # 4. Populate result fields (consumed by on_telegram + health checker)
            result.drive_name = drive_name                          # type: ignore[attr-defined]
            result.drive_label = drive_label                        # type: ignore[attr-defined]
            result.drive_remote_path = sync_result.remote_path     # type: ignore[attr-defined]
            result.drive_sync_ok = sync_result.success              # type: ignore[attr-defined]
            result.drive_public_url = getattr(sync_result, "drive_public_url", "")  # type: ignore[attr-defined]

            # 5. Write .driveurl sidecar so gallery can embed Drive image URL
            pub_url = getattr(sync_result, "drive_public_url", "")
            if pub_url and filepath:
                try:
                    from pathlib import Path as _Path
                    sidecar = _Path(filepath).with_suffix(".driveurl")
                    sidecar.write_text(pub_url, encoding="utf-8")
                    logger.debug("driveurl sidecar written: %s", sidecar)
                except Exception as exc_url:
                    logger.warning("driveurl sidecar write error: %s", exc_url)

        except Exception as exc:
            logger.error(
                "on_drive_sync error for %s: %s", symbol, exc, exc_info=True
            )

    return on_drive_sync


# ---------------------------------------------------------------------------
# on_telegram callback builder
# ---------------------------------------------------------------------------

def _make_on_telegram(telegram_sender: object, send_fn: Any):
    """
    Build on_telegram callback.

    Reads result.drive_name / drive_label already populated by on_drive_sync.
    Uses send_screenshot_to_telegram() wrapper (None-safe — sender may be None).
    """
    def on_telegram(result: object) -> None:
        symbol: str = getattr(result, "symbol", "")
        market: str = getattr(result, "market", "")
        filepath: Optional[str] = getattr(result, "filepath", None)
        dt: datetime = getattr(result, "timestamp", datetime.now())
        drive_name: str = getattr(result, "drive_name", "")
        drive_label: str = getattr(result, "drive_label", "")
        tag: str = getattr(result, "tag", "")

        if not filepath:
            logger.warning("on_telegram: no filepath in result for %s", symbol)
            return

        try:
            sent: bool = send_fn(
                telegram_sender,
                Path(filepath),
                symbol,
                market,
                dt,
                drive_name,
                drive_label,
                tag,
            )
            result.telegram_sent = sent  # type: ignore[attr-defined]
            if sent:
                logger.info("Telegram sent: %s (%s)", symbol, market)
            else:
                logger.warning("Telegram returned False for %s", symbol)

        except Exception as exc:
            logger.error(
                "on_telegram error for %s: %s", symbol, exc, exc_info=True
            )

    return on_telegram


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

def bootstrap(config_path: str = DEFAULT_CONFIG_PATH) -> AppContext:
    """
    Instantiate and wire all application modules.

    Dependency order (per spec):
        1. DriveManager.setup_rclone_from_env()
        2. DriveManager(config)
        3. TelegramSender.from_config(config)   <- None if no token
        4. inject into CoreEngine
        5. register on_drive_sync callback
        6. register on_telegram callback
        7. return AppContext
    """
    logger.info("=" * 60)
    logger.info("Bootstrapping TradingView Auto Screenshot System (Linux)")
    logger.info("=" * 60)

    config = _load_config(config_path)
    _setup_logging(config)

    mods = _import_modules()

    m1 = mods.get("module1_core")
    m2 = mods.get("module2_market")
    m3 = mods.get("module3_storage")
    m4 = mods.get("module4_gui")
    m5 = mods.get("module5_gallery")
    m7 = mods.get("module7_drive")
    mt = mods.get("module_telegram")

    # Critical modules — fail fast
    missing = [
        name for name, mod in [
            ("module1_core", m1),
            ("module2_market", m2),
            ("module3_storage", m3),
        ]
        if mod is None
    ]
    if missing:
        logger.critical(
            "STARTUP FAILED — modules could not be imported: %s\n"
            "Fix: pip install playwright pillow pytz requests imagehash\n"
            "     playwright install chromium",
            missing,
        )
        raise RuntimeError(f"Critical modules failed to load: {missing}")

    # ── StorageManager ────────────────────────────────────────────────────
    storage: object = m3.StorageManager(config=config)  # type: ignore[union-attr]
    logger.info("StorageManager ready")

    # ── GalleryManager (optional) ─────────────────────────────────────────
    gallery: object = None
    if m5 is not None:
        try:
            gallery = m5.GalleryManager(config=config)  # type: ignore[union-attr]
            logger.info("GalleryManager ready")
        except Exception as exc:
            logger.warning("GalleryManager init failed (non-critical): %s", exc)

    # ── Step 1+2: DriveManager ────────────────────────────────────────────
    drive_manager: object = None
    if m7 is not None and config.get("drives"):
        try:
            # Step 1: Write rclone.conf from RCLONE_CONF env var
            rclone_written = m7.DriveManager.setup_rclone_from_env()  # type: ignore[union-attr]
            if rclone_written:
                logger.info("rclone.conf written from RCLONE_CONF env var")
            else:
                logger.warning(
                    "RCLONE_CONF env var not set or empty — "
                    "rclone.conf not written. Drive sync may fail."
                )

            # Step 2: Instantiate DriveManager
            drive_manager = m7.DriveManager(config=config)  # type: ignore[union-attr]
            logger.info(
                "DriveManager ready — %d drive(s) configured",
                len(config.get("drives", [])),
            )
        except Exception as exc:
            logger.warning(
                "DriveManager init failed (non-critical): %s — drive sync disabled",
                exc,
            )
            drive_manager = None
    else:
        logger.info(
            "DriveManager skipped — module7=%s, drives[] present=%s",
            "ok" if m7 is not None else "not imported",
            bool(config.get("drives")),
        )

    # ── Step 3: TelegramSender ────────────────────────────────────────────
    telegram_sender: object = None
    if mt is not None:
        tg_enabled = config.get("telegram", {}).get("enabled", False)
        if tg_enabled:
            try:
                # from_config() returns None if token/chat_id missing
                telegram_sender = mt.TelegramSender.from_config(config)  # type: ignore[union-attr]
                if telegram_sender is not None:
                    verified = telegram_sender.verify_connection()
                    if verified:
                        logger.info("TelegramSender ready — connection verified")
                    else:
                        logger.warning(
                            "TelegramSender: verify_connection() failed "
                            "(check TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)"
                        )
                else:
                    logger.info(
                        "TelegramSender: from_config() returned None "
                        "— TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing"
                    )
            except Exception as exc:
                logger.warning(
                    "TelegramSender init failed (non-critical): %s", exc
                )
                telegram_sender = None
        else:
            logger.info("TelegramSender: disabled in config (telegram.enabled=false)")
    else:
        logger.info(
            "TelegramSender: module_telegram not imported — Telegram disabled"
        )

    # ── CoreEngine ────────────────────────────────────────────────────────
    engine: object = m1.CoreEngine(config_path=config_path)  # type: ignore[union-attr]
    logger.info("CoreEngine ready")

    stocks = config.get("stocks", [])
    logger.info(
        "Screenshot mode: PLAYWRIGHT (headless=%s) — %d stocks: %s",
        config.get("playwright_headless", True),
        len(stocks),
        [s.get("symbol") for s in stocks],
    )

    # ── Wire: market check ────────────────────────────────────────────────
    engine.on_market_check = m2.is_market_open  # type: ignore[union-attr]
    logger.info("Wired: CoreEngine.on_market_check -> module2_market.is_market_open")

    # ── Wire: on_save (StorageManager + gallery rebuild) ──────────────────
    engine.on_save = _make_on_save(storage, gallery)  # type: ignore[union-attr]
    logger.info("Wired: CoreEngine.on_save -> StorageManager + GalleryManager")

    # ── Step 5: Wire on_drive_sync ────────────────────────────────────────
    if drive_manager is not None:
        write_sidecar_fn = None
        if m5 is not None:
            write_sidecar_fn = getattr(m5, "write_drive_sidecar", None)

        engine.on_drive_sync = _make_on_drive_sync(  # type: ignore[union-attr]
            drive_manager, write_sidecar_fn
        )
        logger.info(
            "Wired: CoreEngine.on_drive_sync -> DriveManager.sync_file"
            " + write_drive_sidecar (sidecar_fn=%s)",
            "ok" if write_sidecar_fn is not None else "not found",
        )
    else:
        engine.on_drive_sync = None  # type: ignore[union-attr]
        logger.info("CoreEngine.on_drive_sync = None (Drive disabled)")

    # ── Step 6: Wire on_telegram ──────────────────────────────────────────
    if mt is not None:
        send_fn = getattr(mt, "send_screenshot_to_telegram", None)
        if send_fn is not None:
            engine.on_telegram = _make_on_telegram(  # type: ignore[union-attr]
                telegram_sender, send_fn
            )
            logger.info(
                "Wired: CoreEngine.on_telegram -> send_screenshot_to_telegram"
                " (sender=%s)",
                "active" if telegram_sender is not None else "None/disabled",
            )
        else:
            engine.on_telegram = None  # type: ignore[union-attr]
            logger.warning(
                "module_telegram imported but send_screenshot_to_telegram not found"
            )
    else:
        engine.on_telegram = None  # type: ignore[union-attr]
        logger.info(
            "CoreEngine.on_telegram = None (module_telegram not imported)"
        )

    # ── GUIController (headless stub on Linux) ────────────────────────────
    if m4 is not None:
        gui: object = m4.GUIController(  # type: ignore[union-attr]
            core_engine=engine,
            storage_manager=storage,
        )
    else:
        # Fallback: minimal inline stub if module4 failed to import
        class _MinimalGUI:
            def show_notification(self, t: str, m: str) -> None:
                logger.info("NOTIFY | %s | %s", t, m)

            def start(self) -> None:
                engine.start()  # type: ignore[union-attr]
                import signal
                stop = threading.Event()
                signal.signal(signal.SIGINT,  lambda s, f: stop.set())
                signal.signal(signal.SIGTERM, lambda s, f: stop.set())
                logger.info("Running (fallback headless mode) — Ctrl+C to stop")
                while not stop.is_set():
                    time.sleep(1)
                engine.stop()  # type: ignore[union-attr]

            def stop(self) -> None:
                pass

            def set_autostart(self, _: bool) -> None:
                pass

        gui = _MinimalGUI()

    logger.info("GUIController ready (headless)")

    # ── Wire: notifications ───────────────────────────────────────────────
    engine.on_notify = gui.show_notification  # type: ignore[union-attr]
    logger.info("Wired: CoreEngine.on_notify -> GUIController.show_notification")

    logger.info("Bootstrap complete — all modules wired")

    # ── Step 7: Return AppContext ──────────────────────────────────────────
    return AppContext(
        core_engine=engine,
        storage_manager=storage,
        gallery_manager=gallery,
        drive_manager=drive_manager,
        telegram_sender=telegram_sender,
        market_module=m2,
        gui_controller=gui,
        config=config,
        config_path=config_path,
    )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def run(config_path: str = DEFAULT_CONFIG_PATH) -> None:
    """Full application entry: bootstrap -> gui.start() (blocking)."""
    try:
        ctx = bootstrap(config_path)
        logger.info("Starting — application running (headless)")
        ctx.gui_controller.start()  # type: ignore[union-attr]
    except KeyboardInterrupt:
        logger.info("Interrupted by user — shutting down")
    except Exception as exc:
        logger.critical("Fatal error in run(): %s", exc, exc_info=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Linux: crontab helpers
# ---------------------------------------------------------------------------

def register_crontab(config_path: str = DEFAULT_CONFIG_PATH) -> bool:
    """
    Add a crontab entry to run main.py at system startup (@reboot).

    The entry is tagged with CRON_MARKER so it can be found and removed later.
    Existing entries with the same marker are replaced.

    Returns True on success.
    """
    if sys.platform == "win32":
        logger.warning("register_crontab: use register_task_scheduler on Windows")
        return False

    python = sys.executable
    main_py = str(Path(__file__).parent / "main.py")
    log_file = str(Path(__file__).parent / "logs" / "cron.log")
    config_abs = str(Path(config_path).resolve())

    new_entry = (
        f"@reboot {python} {main_py} --config {config_abs} "
        f">> {log_file} 2>&1  {CRON_MARKER}\n"
    )

    try:
        result = subprocess.run(
            ["crontab", "-l"],
            capture_output=True, text=True
        )
        existing = result.stdout if result.returncode == 0 else ""

        lines = [
            line for line in existing.splitlines(keepends=True)
            if CRON_MARKER not in line
        ]
        lines.append(new_entry)
        new_crontab = "".join(lines)

        proc = subprocess.run(
            ["crontab", "-"],
            input=new_crontab, text=True, capture_output=True
        )
        if proc.returncode == 0:
            logger.info("Crontab @reboot entry added: %s", new_entry.strip())
            return True
        else:
            logger.error("crontab write failed: %s", proc.stderr)
            return False

    except FileNotFoundError:
        logger.error("crontab not found — install cron: sudo apt install cron")
        return False
    except Exception as exc:
        logger.error("register_crontab error: %s", exc)
        return False


def unregister_crontab() -> bool:
    """Remove TVScreenshot crontab entry. Returns True on success."""
    if sys.platform == "win32":
        logger.warning("unregister_crontab: not applicable on Windows")
        return False

    try:
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True
        )
        if result.returncode != 0:
            logger.info("No crontab found — nothing to remove")
            return True

        lines = [
            line for line in result.stdout.splitlines(keepends=True)
            if CRON_MARKER not in line
        ]
        new_crontab = "".join(lines)
        proc = subprocess.run(
            ["crontab", "-"], input=new_crontab, text=True, capture_output=True
        )
        if proc.returncode == 0:
            logger.info("Crontab entry removed")
            return True
        else:
            logger.error("crontab write failed: %s", proc.stderr)
            return False

    except Exception as exc:
        logger.error("unregister_crontab error: %s", exc)
        return False


def write_systemd_service(config_path: str = DEFAULT_CONFIG_PATH) -> bool:
    """
    Write a systemd user service file for auto-start.

    File: ~/.config/systemd/user/tvscreenshot.service
    After writing:
        systemctl --user enable tvscreenshot
        systemctl --user start tvscreenshot

    Returns True on success.
    """
    if sys.platform == "win32":
        logger.warning("write_systemd_service: not applicable on Windows")
        return False

    python = sys.executable
    main_py = str(Path(__file__).parent.resolve() / "main.py")
    working_dir = str(Path(__file__).parent.resolve())
    config_abs = str(Path(config_path).resolve())
    log_file = str(Path(__file__).parent.resolve() / "logs" / "systemd.log")

    service_content = (
        "[Unit]\n"
        "Description=TradingView Auto Screenshot System\n"
        "After=network.target network-online.target\n"
        "Wants=network-online.target\n"
        "\n"
        "[Service]\n"
        "Type=simple\n"
        f"WorkingDirectory={working_dir}\n"
        f"ExecStart={python} {main_py} --config {config_abs} --no-detach\n"
        "Restart=on-failure\n"
        "RestartSec=30\n"
        f"StandardOutput=append:{log_file}\n"
        f"StandardError=append:{log_file}\n"
        "\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    )

    service_dir = Path.home() / ".config" / "systemd" / "user"
    service_file = service_dir / "tvscreenshot.service"

    try:
        service_dir.mkdir(parents=True, exist_ok=True)
        service_file.write_text(service_content, encoding="utf-8")
        logger.info("systemd service written: %s", service_file)
        logger.info("Enable with: systemctl --user enable tvscreenshot")
        logger.info("Start  with: systemctl --user start tvscreenshot")
        logger.info("Logs   with: journalctl --user -u tvscreenshot -f")
        return True
    except Exception as exc:
        logger.error("write_systemd_service error: %s", exc)
        return False


# Keep Windows stubs for API compatibility
def register_task_scheduler(*args, **kwargs) -> bool:
    """Windows Task Scheduler — not available on Linux. Use register_crontab()."""
    logger.warning(
        "register_task_scheduler is Windows-only. "
        "Use register_crontab() or write_systemd_service() on Linux."
    )
    return False


def unregister_task_scheduler(*args, **kwargs) -> bool:
    """Windows Task Scheduler — not available on Linux. Use unregister_crontab()."""
    logger.warning(
        "unregister_task_scheduler is Windows-only. "
        "Use unregister_crontab() on Linux."
    )
    return False


# ---------------------------------------------------------------------------
# Self-test helpers
# ---------------------------------------------------------------------------

class _SelfTestResult:
    def __init__(self) -> None:
        self._ok: list = []
        self._failures: list = []

    def ok(self, label: str) -> None:
        self._ok.append(label)
        print(f"  [PASS] {label}")

    def fail(self, label: str, reason: str) -> None:
        self._failures.append(f"{label}: {reason}")
        print(f"  [FAIL] {label}: {reason}")

    @property
    def all_passed(self) -> bool:
        return len(self._failures) == 0

    def summary(self) -> str:
        return f"{len(self._ok)} passed, {len(self._failures)} failed"


def _test_module2(r: _SelfTestResult, mods: dict) -> None:
    print("\n--- module2_market ---")
    m2 = mods.get("module2_market")
    if m2 is None:
        r.fail("module2_market", "import failed")
        return
    try:
        import pytz  # type: ignore
        tz = pytz.utc
        dt = datetime.now(tz=tz)
        result = m2.is_market_open("BTCUSDT", "CRYPTO", dt)
        if result is True:
            r.ok("is_market_open CRYPTO always True")
        else:
            r.fail("is_market_open CRYPTO", f"returned {result}")
    except Exception as exc:
        r.fail("module2_market", str(exc))


def _test_module3(r: _SelfTestResult, mods: dict) -> None:
    print("\n--- module3_storage ---")
    m3 = mods.get("module3_storage")
    if m3 is None:
        r.fail("module3_storage", "import failed")
        return
    try:
        with tempfile.TemporaryDirectory() as tmp:
            m3.StorageManager(config={
                "screenshot_folder": tmp,
                "global_limit_gb": 1,
                "per_symbol_limit_mb": 150,
                "disk_warning_gb": 0.1,
            })
            r.ok("StorageManager instantiated")
    except Exception as exc:
        r.fail("StorageManager init", str(exc))


def _test_module5(r: _SelfTestResult, mods: dict) -> None:
    print("\n--- module5_gallery ---")
    m5 = mods.get("module5_gallery")
    if m5 is None:
        r.ok("module5_gallery: skipped (not imported)")
        return
    try:
        with tempfile.TemporaryDirectory() as tmp:
            gm = m5.GalleryManager(config={
                "screenshot_folder": tmp,
                "gallery_folder": tmp,
            })
            gm.build()
            r.ok("GalleryManager.build() OK")
    except Exception as exc:
        r.fail("GalleryManager.build", str(exc))


def _test_module7(r: _SelfTestResult, mods: dict) -> None:
    print("\n--- module7_drive ---")
    m7 = mods.get("module7_drive")
    if m7 is None:
        r.ok("module7_drive: skipped (not imported)")
        return
    try:
        test_cfg = {
            "drives": [
                {
                    "name": "Drive1",
                    "rclone_remote": "gdrive1:",
                    "limit_gb": 9.5,
                    "label": "CRYPTO",
                },
                {
                    "name": "Drive2",
                    "rclone_remote": "gdrive2:",
                    "limit_gb": 9.5,
                    "label": "US-A",
                },
            ]
        }
        dm = m7.DriveManager(config=test_cfg)
        # CRYPTO must always go to Drive1 (index 0)
        asgn_crypto = dm.assign_drive("BTCUSD", "CRYPTO")
        if asgn_crypto is not None and asgn_crypto.drive.name == "Drive1":
            r.ok("DriveManager.assign_drive CRYPTO -> Drive1")
        else:
            r.fail(
                "DriveManager.assign_drive CRYPTO",
                f"got {asgn_crypto.drive.name if asgn_crypto else None}",
            )
        # US must NOT go to Drive1
        asgn_us = dm.assign_drive("AAPL", "US")
        if asgn_us is not None and asgn_us.drive.name != "Drive1":
            r.ok("DriveManager.assign_drive US -> non-Drive1")
        else:
            r.fail(
                "DriveManager.assign_drive US",
                f"got {asgn_us.drive.name if asgn_us else None}",
            )
    except Exception as exc:
        r.fail("module7_drive", str(exc))


def _test_module_telegram(r: _SelfTestResult, mods: dict) -> None:
    print("\n--- module_telegram ---")
    mt = mods.get("module_telegram")
    if mt is None:
        r.ok("module_telegram: skipped (not imported)")
        return
    try:
        # from_config with empty config should return None (no exception)
        sender = mt.TelegramSender.from_config({})
        if sender is None:
            r.ok("TelegramSender.from_config(empty) -> None (correct)")
        else:
            r.fail(
                "TelegramSender.from_config(empty)",
                "should return None without token",
            )
    except Exception as exc:
        r.fail("TelegramSender.from_config(empty)", str(exc))

    try:
        # send_screenshot_to_telegram with None sender must return True (no-op)
        result = mt.send_screenshot_to_telegram(
            None,
            Path("/tmp/test.png"),
            "AAPL",
            "US",
            datetime.now(),
            "Drive2",
            "US-A",
            "",
        )
        if result is True:
            r.ok("send_screenshot_to_telegram(None sender) -> True (no-op)")
        else:
            r.fail(
                "send_screenshot_to_telegram(None)",
                f"returned {result}, expected True",
            )
    except Exception as exc:
        r.fail("send_screenshot_to_telegram(None)", str(exc))


def _test_config_loading(r: _SelfTestResult) -> None:
    print("\n--- config loading ---")
    with tempfile.TemporaryDirectory() as tmp:
        cfg_path = Path(tmp) / "config.json"
        cfg_path.write_text(json.dumps({"global_limit_gb": 2}), encoding="utf-8")
        try:
            cfg = _load_config(str(cfg_path))
            if cfg.get("global_limit_gb") == 2:
                r.ok("_load_config reads user value")
            else:
                r.fail(
                    "_load_config",
                    f"global_limit_gb={cfg.get('global_limit_gb')}",
                )
        except Exception as exc:
            r.fail("_load_config", str(exc))

    try:
        cfg_default = _load_config("/nonexistent/config.json")
        if "screenshot_folder" in cfg_default:
            r.ok("_load_config fallback defaults")
        else:
            r.fail("_load_config defaults", "missing screenshot_folder")
    except Exception as exc:
        r.fail("_load_config defaults", str(exc))


def _test_bootstrap(r: _SelfTestResult) -> None:
    print("\n--- bootstrap ---")
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        cfg_path = Path(tmp) / "config.json"
        cfg_path.write_text(
            json.dumps({
                "screenshot_folder": str(Path(tmp) / "screenshots"),
                "gallery_folder":    str(Path(tmp) / "gallery"),
                "log_folder":        str(Path(tmp) / "logs"),
                "auto_start_windows": False,
                "playwright_headless": True,
                "drives": [],
                "telegram": {"enabled": False},
            }),
            encoding="utf-8",
        )
        try:
            ctx = bootstrap(str(cfg_path))
            r.ok("bootstrap() returned AppContext")

            assert ctx.core_engine is not None, "core_engine is None"
            assert ctx.storage_manager is not None, "storage_manager is None"
            assert ctx.market_module is not None, "market_module is None"
            assert ctx.gui_controller is not None, "gui_controller is None"
            r.ok("AppContext has all required components")

            if callable(getattr(ctx.core_engine, "on_notify", None)):
                r.ok("CoreEngine.on_notify wired")
            else:
                r.fail("CoreEngine.on_notify", "not callable")

            if callable(getattr(ctx.core_engine, "on_save", None)):
                r.ok("CoreEngine.on_save wired")
            else:
                r.fail("CoreEngine.on_save", "not callable")

            if callable(getattr(ctx.core_engine, "on_market_check", None)):
                r.ok("CoreEngine.on_market_check wired")
            else:
                r.fail("CoreEngine.on_market_check", "not callable")

            # When drives=[] and telegram disabled, these should be None
            if ctx.drive_manager is None:
                r.ok("drive_manager=None when drives=[] (correct)")
            else:
                r.fail("drive_manager", "should be None when drives=[]")

            if ctx.telegram_sender is None:
                r.ok("telegram_sender=None when disabled (correct)")
            else:
                r.fail("telegram_sender", "should be None when telegram.enabled=false")

            # on_drive_sync and on_telegram should be None when disabled
            if getattr(ctx.core_engine, "on_drive_sync", "MISSING") is None:
                r.ok("CoreEngine.on_drive_sync=None when Drive disabled (correct)")
            if getattr(ctx.core_engine, "on_telegram", "MISSING") is None:
                r.ok("CoreEngine.on_telegram=None when Telegram disabled (correct)")

        except Exception as exc:
            r.fail("bootstrap()", str(exc))
        finally:
            root_logger = logging.getLogger()
            for h in list(root_logger.handlers):
                if isinstance(h, logging.FileHandler) and tmp in getattr(
                    h, "baseFilename", ""
                ):
                    h.close()
                    root_logger.removeHandler(h)


def run_self_test() -> bool:
    """Run all self-tests. Returns True if all pass."""
    print("=" * 60)
    print("TradingView Auto Screenshot System — Self Test (Linux)")
    print(f"Python {sys.version.split()[0]} | {sys.platform}")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    r = _SelfTestResult()
    mods = _import_modules()

    _test_config_loading(r)
    _test_module2(r, mods)
    _test_module3(r, mods)
    _test_module5(r, mods)
    _test_module7(r, mods)
    _test_module_telegram(r, mods)
    _test_bootstrap(r)

    print("\n" + "=" * 60)
    print(r.summary())
    if r._failures:
        print("\nFailures:")
        for f in r._failures:
            print(f"  FAIL {f}")
    if r.all_passed:
        print("All tests passed")
    else:
        print("Some tests failed — see above")
    print("=" * 60)
    return r.all_passed


# ---------------------------------------------------------------------------
# Health summary
# ---------------------------------------------------------------------------

def print_health_summary(config_path: str = DEFAULT_CONFIG_PATH) -> None:
    """Print a quick health summary to stdout."""
    config = _load_config(config_path)
    shots_folder = Path(config.get("screenshot_folder", "./screenshots"))

    print("\nHealth Summary")
    print("-" * 40)
    print(f"Config      : {Path(config_path).resolve()}")
    print(f"Screenshots : {shots_folder.resolve()}")
    print(f"Platform    : {sys.platform}")

    if shots_folder.exists():
        total_bytes = sum(
            f.stat().st_size for f in shots_folder.rglob("*.png") if f.is_file()
        )
        total_mb = total_bytes / (1024 ** 2)
        total_files = sum(1 for _ in shots_folder.rglob("*.png"))
        print(f"Storage     : {total_mb:.1f} MB across {total_files} PNG files")
    else:
        print("Storage     : folder not found")

    mods = _import_modules()
    m2 = mods.get("module2_market")
    if m2 is not None:
        try:
            summary = m2.get_market_status_summary()
            for market, status in summary.items():
                if market == "timestamp_utc":
                    continue
                if isinstance(status, dict):
                    open_str = "OPEN" if status.get("open") else "CLOSED"
                    shot_str = "screenshot=YES" if status.get("screenshot") else "screenshot=NO"
                    tag_str = f" tag={status['tag']}" if status.get("tag") else ""
                    print(f"Market      : {market} — {open_str}, {shot_str}{tag_str}")
                else:
                    print(f"Market      : {market} — {status}")
        except Exception as exc:
            print(f"Market      : error ({exc})")
    else:
        print("Market      : module2 not available")

    drives = config.get("drives", [])
    if drives:
        print(f"Drives      : {len(drives)} configured")
        for d in drives:
            print(
                f"             {d.get('name')} [{d.get('label', '')}]"
                f" rclone={d.get('rclone_remote')} limit={d.get('limit_gb')}GB"
            )
    else:
        print("Drives      : none configured")

    tg = config.get("telegram", {})
    print(f"Telegram    : {'enabled' if tg.get('enabled') else 'disabled'}")
    pages_url = config.get("github_pages_url", "")
    if pages_url:
        print(f"Gallery URL : {pages_url}")

    print(
        f"Timestamp   : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    print("-" * 40)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        prog="module6_integration",
        description="TradingView Auto Screenshot — Integration Module (Linux)",
    )
    parser.add_argument(
        "command", nargs="?", default="run",
        choices=["run", "test", "health", "register", "unregister", "systemd"],
        help=(
            "run=start app, test=smoke tests, health=show status, "
            "register=add crontab @reboot, unregister=remove crontab, "
            "systemd=write systemd service file"
        ),
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH)

    args = parser.parse_args()

    if args.command == "test":
        sys.exit(0 if run_self_test() else 1)
    elif args.command == "health":
        print_health_summary(args.config)
    elif args.command == "register":
        sys.exit(0 if register_crontab(args.config) else 1)
    elif args.command == "unregister":
        sys.exit(0 if unregister_crontab() else 1)
    elif args.command == "systemd":
        sys.exit(0 if write_systemd_service(args.config) else 1)
    else:
        run(args.config)


if __name__ == "__main__":
    _cli()
