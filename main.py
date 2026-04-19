"""
main.py — TradingView Auto Screenshot System
Entry Point — Linux / Cloud Edition

Usage:
    python main.py                    # Start (blocking, logs to console + file)
    python main.py --test             # Smoke test and exit
    python main.py --health           # Print health summary and exit
    python main.py --register         # Add @reboot crontab entry
    python main.py --unregister       # Remove crontab entry
    python main.py --systemd          # Write systemd service file
    python main.py --screenshot       # Take one manual screenshot now and exit

The application:
    1. Reads config.json
    2. Starts CoreEngine — waits for 1H candle close (top of every hour + 5s)
    3. Screenshots all configured TradingView URLs via Playwright headless
    4. Saves to screenshots/MARKET/SYMBOL/YYYY-MM/
    5. Rebuilds HTML gallery after each capture
    6. Runs until Ctrl+C or SIGTERM
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logger = logging.getLogger(__name__)

_HERE     = Path(__file__).parent
_LOG_FILE = _HERE / "app.log"


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(log_path: Path, debug: bool = False) -> None:
    """Route all logs to rotating file + stdout."""
    import logging.handlers

    root = logging.getLogger()
    root.setLevel(logging.DEBUG if debug else logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="TVScreenshot",
        description="TradingView Auto Screenshot System (Linux/Cloud Edition)",
    )
    parser.add_argument(
        "--config", default="./config.json", metavar="PATH",
        help="Path to config.json (default: ./config.json)",
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Run self-test and exit",
    )
    parser.add_argument(
        "--health", action="store_true",
        help="Print health summary and exit",
    )
    parser.add_argument(
        "--register", action="store_true",
        help="Add @reboot crontab entry for auto-start",
    )
    parser.add_argument(
        "--unregister", action="store_true",
        help="Remove crontab auto-start entry",
    )
    parser.add_argument(
        "--systemd", action="store_true",
        help="Write ~/.config/systemd/user/tvscreenshot.service",
    )
    parser.add_argument(
        "--screenshot", action="store_true",
        help="Take one manual screenshot of all stocks now and exit",
    )
    parser.add_argument(
        "--no-detach", action="store_true",
        help="(Compatibility flag — no-op on Linux, always runs in foreground)",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Enable DEBUG logging",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    _setup_logging(_LOG_FILE, debug=args.debug)

    logger.info("=" * 60)
    logger.info("TradingView Auto Screenshot System (Linux/Cloud)")
    logger.info("Python     : %s", sys.version.split()[0])
    logger.info("Platform   : %s", sys.platform)
    logger.info("Config     : %s", Path(args.config).resolve())
    logger.info("Log file   : %s", _LOG_FILE)

    try:
        import module6_integration as m6
    except ImportError as exc:
        logger.error("Cannot import module6_integration: %s", exc)
        sys.exit(1)

    if args.test:
        sys.exit(0 if m6.run_self_test() else 1)

    elif args.health:
        m6.print_health_summary(args.config)
        sys.exit(0)

    elif args.register:
        sys.exit(0 if m6.register_crontab(args.config) else 1)

    elif args.unregister:
        sys.exit(0 if m6.unregister_crontab() else 1)

    elif args.systemd:
        sys.exit(0 if m6.write_systemd_service(args.config) else 1)

    elif args.screenshot:
        # One-shot manual screenshot
        logger.info("Manual screenshot triggered")
        try:
            ctx = m6.bootstrap(args.config)
            results = ctx.core_engine.test_screenshot()  # type: ignore[union-attr]
            ok = sum(1 for r in results if r.success)
            logger.info("Manual screenshot done: %d/%d OK", ok, len(results))
            for r in results:
                status = "OK" if r.success else "ERR"
                logger.info("  %s  %s -> %s", status, r.symbol, r.filepath or r.error)
            sys.exit(0 if ok == len(results) else 1)
        except Exception as exc:
            logger.error("Manual screenshot failed: %s", exc)
            sys.exit(1)

    else:
        m6.run(args.config)


if __name__ == "__main__":
    main()
