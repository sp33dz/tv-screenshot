"""
module2_market.py — Market Hours & Event Tag Logic
TradingView Auto Screenshot System

Provides:
- is_market_open(symbol, market, dt) → bool
- is_screenshot_time(symbol, market, dt) → bool   ← NEW
- get_event_tag(symbol, market, dt) → str
- MarketCalendar class (NYSE holidays, DST-aware)

Markets supported:
- CRYPTO: 24/7, always open
- US:     NYSE 09:30–16:00 ET, Mon–Fri, exc. holidays

Screenshot window (US):
- 07:30–18:00 ET on trading days
  (2 hours pre-market + market hours + 2 hours post-market)

Special event tags:
- NY_OPEN       : 09:30 ET (± 1 min)
- NY_CLOSE      : 16:00 ET (± 1 min)
- PRE_MARKET    : 07:30 ET (± 1 min)   ← NEW
- POST_MARKET   : 18:00 ET (± 1 min)   ← NEW
- CRYPTO_FUNDING: 00:00, 08:00, 16:00 UTC (± 1 min)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import pytz
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TZ_NEW_YORK: pytz.BaseTzInfo = pytz.timezone("America/New_York")
TZ_UTC: pytz.BaseTzInfo = pytz.utc

NYSE_OPEN_LOCAL: Tuple[int, int] = (9, 30)   # HH, MM in ET
NYSE_CLOSE_LOCAL: Tuple[int, int] = (16, 0)  # HH, MM in ET

# Screenshot window for US stocks (pre-market + market + post-market)
US_SCREENSHOT_START: Tuple[int, int] = (7, 30)   # 2h before NYSE open
US_SCREENSHOT_END: Tuple[int, int] = (18, 0)     # 2h after NYSE close

CRYPTO_FUNDING_HOURS_UTC: Tuple[int, ...] = (0, 8, 16)  # UTC hours

# Tolerance window (minutes) for event-tag matching
EVENT_TAG_TOLERANCE_MIN: int = 1

# Cache TTL for NYSE holiday fetch (seconds)
HOLIDAY_CACHE_TTL_SEC: int = 86_400  # 24 h

# NYSE holiday API (Nasdaq market-calendar endpoint — free, no key)
HOLIDAY_API_URL = (
    "https://api.nasdaq.com/api/calendar/holidays?assetclass=stocks&fromdate={year}-01-01&todate={year}-12-31"
)

# Fallback hardcoded NYSE holidays 2025 & 2026
# Dates as (YYYY, M, D)
_HARDCODED_HOLIDAYS: Dict[int, List[Tuple[int, int, int]]] = {
    2025: [
        (2025, 1, 1),   # New Year's Day
        (2025, 1, 20),  # MLK Day
        (2025, 2, 17),  # Presidents' Day
        (2025, 4, 18),  # Good Friday
        (2025, 5, 26),  # Memorial Day
        (2025, 6, 19),  # Juneteenth
        (2025, 7, 4),   # Independence Day
        (2025, 9, 1),   # Labor Day
        (2025, 11, 27), # Thanksgiving
        (2025, 12, 25), # Christmas
    ],
    2026: [
        (2026, 1, 1),   # New Year's Day
        (2026, 1, 19),  # MLK Day
        (2026, 2, 16),  # Presidents' Day
        (2026, 4, 3),   # Good Friday
        (2026, 5, 25),  # Memorial Day
        (2026, 6, 19),  # Juneteenth
        (2026, 7, 3),   # Independence Day (observed)
        (2026, 9, 7),   # Labor Day
        (2026, 11, 26), # Thanksgiving
        (2026, 12, 25), # Christmas
    ],
}


# ---------------------------------------------------------------------------
# MarketCalendar — NYSE holiday management
# ---------------------------------------------------------------------------

@dataclass
class MarketCalendar:
    """
    Manages NYSE trading-day calendar with auto-fetching holidays.

    Attributes:
        _holidays: Set of holiday dates per year.
        _cache_ts: Unix timestamp of last successful API fetch per year.
    """

    _holidays: Dict[int, Set[date]] = field(default_factory=dict)
    _cache_ts: Dict[int, float] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_holiday(self, d: date) -> bool:
        """Return True if *d* is a NYSE holiday."""
        self._ensure_loaded(d.year)
        return d in self._holidays.get(d.year, set())

    def is_trading_day(self, d: date) -> bool:
        """Return True if *d* is a NYSE trading day (Mon–Fri, not holiday)."""
        if d.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        return not self.is_holiday(d)

    def get_holidays(self, year: int) -> Set[date]:
        """Return set of NYSE holidays for *year*."""
        self._ensure_loaded(year)
        return self._holidays.get(year, set()).copy()

    # ------------------------------------------------------------------
    # Internal loading
    # ------------------------------------------------------------------

    def _ensure_loaded(self, year: int) -> None:
        """Load holidays for *year* if not cached or cache is stale."""
        ts = self._cache_ts.get(year, 0.0)
        if time.time() - ts < HOLIDAY_CACHE_TTL_SEC and year in self._holidays:
            return  # cache still fresh

        holidays = self._fetch_from_api(year)
        if holidays:
            self._holidays[year] = holidays
            self._cache_ts[year] = time.time()
            logger.info("NYSE holidays loaded from API for %d (%d dates)", year, len(holidays))
        else:
            holidays = self._load_hardcoded(year)
            self._holidays[year] = holidays
            self._cache_ts[year] = time.time()
            logger.warning(
                "NYSE API unavailable — using hardcoded holidays for %d (%d dates)",
                year,
                len(holidays),
            )

    def _fetch_from_api(self, year: int) -> Optional[Set[date]]:
        """
        Attempt to fetch NYSE holidays from Nasdaq API.

        Returns set of dates on success, None on failure.
        """
        url = HOLIDAY_API_URL.format(year=year)
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            data = resp.json()
            # Nasdaq calendar response structure:
            # data["data"]["rows"] → list of {"date": "YYYY-MM-DD", ...}
            rows = data.get("data", {}).get("rows", [])
            if not rows:
                return None
            holidays: Set[date] = set()
            for row in rows:
                raw_date = row.get("date", "")
                try:
                    holidays.add(date.fromisoformat(raw_date))
                except ValueError:
                    logger.debug("Skipping unparseable holiday date: %s", raw_date)
            return holidays if holidays else None
        except Exception as exc:  # noqa: BLE001
            logger.debug("NYSE holiday API fetch failed for %d: %s", year, exc)
            return None

    def _load_hardcoded(self, year: int) -> Set[date]:
        """Return hardcoded holiday set for *year*."""
        raw = _HARDCODED_HOLIDAYS.get(year, [])
        loaded = {date(y, m, d) for y, m, d in raw}
        if not loaded:
            logger.warning("No hardcoded holidays available for %d — calendar may be inaccurate", year)
        return loaded


# ---------------------------------------------------------------------------
# Module-level singleton calendar
# ---------------------------------------------------------------------------

_calendar = MarketCalendar()


# ---------------------------------------------------------------------------
# Core public functions
# ---------------------------------------------------------------------------

def is_market_open(
    symbol: str,
    market: str,
    dt: Optional[datetime] = None,
) -> bool:
    """
    Check whether the market for *symbol* is open at *dt*.

    Parameters
    ----------
    symbol : str
        Trading symbol (e.g. "BTCUSDT", "AAPL").
    market : str
        "CRYPTO" or "US".
    dt : datetime, optional
        Timezone-aware datetime to check.  Defaults to utcnow (UTC).

    Returns
    -------
    bool
        True if market is open.
    """
    if dt is None:
        dt = datetime.now(tz=TZ_UTC)
    elif dt.tzinfo is None:
        dt = TZ_UTC.localize(dt)
        logger.debug("is_market_open: naive datetime assumed UTC")

    market_upper = market.upper()

    if market_upper == "CRYPTO":
        return _is_crypto_open()

    if market_upper == "US":
        return _is_us_market_open(dt)

    logger.warning("Unknown market '%s' for symbol '%s' — defaulting to open", market, symbol)
    return True


def is_screenshot_time(
    symbol: str,
    market: str,
    dt: Optional[datetime] = None,
) -> bool:
    """
    Check whether a screenshot should be taken for *symbol* at *dt*.

    Rules:
    - CRYPTO : always True (24H)
    - US     : True on trading days within 07:30–18:00 ET
               (2h pre-market + regular hours + 2h post-market)

    Parameters
    ----------
    symbol : str
        Trading symbol.
    market : str
        "CRYPTO" or "US".
    dt : datetime, optional
        Timezone-aware datetime.  Defaults to now (UTC).

    Returns
    -------
    bool
    """
    if dt is None:
        dt = datetime.now(tz=TZ_UTC)
    elif dt.tzinfo is None:
        dt = TZ_UTC.localize(dt)
        logger.debug("is_screenshot_time: naive datetime assumed UTC")

    market_upper = market.upper()

    if market_upper == "CRYPTO":
        return True  # 24H, no restriction

    if market_upper == "US":
        return _is_us_screenshot_time(dt)

    logger.warning(
        "Unknown market '%s' for symbol '%s' — defaulting to screenshot", market, symbol
    )
    return True


def get_event_tag(
    symbol: str,
    market: str,
    dt: Optional[datetime] = None,
) -> str:
    """
    Return a special event tag string if *dt* coincides with a key event.

    Parameters
    ----------
    symbol : str
        Trading symbol.
    market : str
        "CRYPTO" or "US".
    dt : datetime, optional
        Timezone-aware datetime.  Defaults to utcnow.

    Returns
    -------
    str
        Event tag such as "NY_OPEN", "NY_CLOSE", "CRYPTO_FUNDING",
        or "" if no special event.
    """
    if dt is None:
        dt = datetime.now(tz=TZ_UTC)
    elif dt.tzinfo is None:
        dt = TZ_UTC.localize(dt)

    market_upper = market.upper()

    if market_upper == "CRYPTO":
        return _get_crypto_event_tag(dt)

    if market_upper == "US":
        return _get_us_event_tag(dt)

    return ""


# ---------------------------------------------------------------------------
# Internal: CRYPTO
# ---------------------------------------------------------------------------

def _is_crypto_open() -> bool:
    """Crypto is always open — 24/7."""
    return True


def _get_crypto_event_tag(dt: datetime) -> str:
    """
    Return "CRYPTO_FUNDING" if *dt* is within EVENT_TAG_TOLERANCE_MIN of
    a funding-rate settlement (00:00, 08:00, 16:00 UTC).
    """
    dt_utc = dt.astimezone(TZ_UTC)
    minute_of_day = dt_utc.hour * 60 + dt_utc.minute

    for funding_hour in CRYPTO_FUNDING_HOURS_UTC:
        funding_minute = funding_hour * 60
        if abs(minute_of_day - funding_minute) <= EVENT_TAG_TOLERANCE_MIN:
            logger.debug(
                "CRYPTO_FUNDING tag at %s (UTC %02d:00)",
                dt_utc.isoformat(),
                funding_hour,
            )
            return "CRYPTO_FUNDING"

    return ""


# ---------------------------------------------------------------------------
# Internal: US (NYSE)
# ---------------------------------------------------------------------------

def _is_us_market_open(dt: datetime) -> bool:
    """
    Return True if NYSE is open at *dt*.

    Rules:
    1. Must be a weekday (Mon–Fri).
    2. Must not be a NYSE holiday.
    3. Must be between 09:30 and 16:00 ET (DST-aware via pytz).
    """
    dt_et = dt.astimezone(TZ_NEW_YORK)
    local_date = dt_et.date()

    if local_date.weekday() >= 5:
        return False

    if _calendar.is_holiday(local_date):
        logger.debug("US market closed — NYSE holiday: %s", local_date)
        return False

    open_time = dt_et.replace(
        hour=NYSE_OPEN_LOCAL[0], minute=NYSE_OPEN_LOCAL[1], second=0, microsecond=0
    )
    close_time = dt_et.replace(
        hour=NYSE_CLOSE_LOCAL[0], minute=NYSE_CLOSE_LOCAL[1], second=0, microsecond=0
    )

    return open_time <= dt_et < close_time


def _is_us_screenshot_time(dt: datetime) -> bool:
    """
    Return True if US screenshot should run at *dt*.

    Window: 07:30–18:00 ET on NYSE trading days (weekday, not holiday).
    Inclusive of both endpoints so cron at exactly :30 or :00 is captured.
    """
    dt_et = dt.astimezone(TZ_NEW_YORK)
    local_date = dt_et.date()

    # Must be a weekday
    if local_date.weekday() >= 5:
        return False

    # Must not be a NYSE holiday
    if _calendar.is_holiday(local_date):
        logger.debug("US screenshot skipped — NYSE holiday: %s", local_date)
        return False

    # Build window boundaries for today
    start_time = dt_et.replace(
        hour=US_SCREENSHOT_START[0], minute=US_SCREENSHOT_START[1],
        second=0, microsecond=0,
    )
    end_time = dt_et.replace(
        hour=US_SCREENSHOT_END[0], minute=US_SCREENSHOT_END[1],
        second=0, microsecond=0,
    )

    in_window = start_time <= dt_et <= end_time
    if not in_window:
        logger.debug(
            "US screenshot skipped — outside window: %s ET (window %02d:%02d–%02d:%02d)",
            dt_et.strftime("%H:%M"),
            US_SCREENSHOT_START[0], US_SCREENSHOT_START[1],
            US_SCREENSHOT_END[0], US_SCREENSHOT_END[1],
        )
    return in_window


def _get_us_event_tag(dt: datetime) -> str:
    """
    Return event tag if *dt* is within EVENT_TAG_TOLERANCE_MIN of a key US time.

    Tags (ET):
    - PRE_MARKET  : 07:30
    - NY_OPEN     : 09:30
    - NY_CLOSE    : 16:00
    - POST_MARKET : 18:00
    """
    dt_et = dt.astimezone(TZ_NEW_YORK)

    def _boundary(hour: int, minute: int) -> datetime:
        return dt_et.replace(hour=hour, minute=minute, second=0, microsecond=0)

    checkpoints: List[Tuple[datetime, str]] = [
        (_boundary(US_SCREENSHOT_START[0], US_SCREENSHOT_START[1]), "PRE_MARKET"),
        (_boundary(NYSE_OPEN_LOCAL[0],     NYSE_OPEN_LOCAL[1]),     "NY_OPEN"),
        (_boundary(NYSE_CLOSE_LOCAL[0],    NYSE_CLOSE_LOCAL[1]),    "NY_CLOSE"),
        (_boundary(US_SCREENSHOT_END[0],   US_SCREENSHOT_END[1]),   "POST_MARKET"),
    ]

    for boundary_dt, tag in checkpoints:
        diff_min = abs((dt_et - boundary_dt).total_seconds()) / 60.0
        if diff_min <= EVENT_TAG_TOLERANCE_MIN:
            logger.debug("%s tag at %s ET", tag, dt_et.strftime("%H:%M"))
            return tag

    return ""


# ---------------------------------------------------------------------------
# Convenience helpers (used by CoreEngine integration)
# ---------------------------------------------------------------------------

def get_market_status_summary(dt: Optional[datetime] = None) -> Dict[str, object]:
    """
    Return a status dict for all supported markets at *dt*.

    Returns
    -------
    dict with keys: "CRYPTO", "US", "timestamp_utc"
    """
    if dt is None:
        dt = datetime.now(tz=TZ_UTC)
    elif dt.tzinfo is None:
        dt = TZ_UTC.localize(dt)

    return {
        "timestamp_utc": dt.isoformat(),
        "CRYPTO": {
            "open": is_market_open("", "CRYPTO", dt),
            "screenshot": is_screenshot_time("", "CRYPTO", dt),
            "tag": get_event_tag("", "CRYPTO", dt),
        },
        "US": {
            "open": is_market_open("", "US", dt),
            "screenshot": is_screenshot_time("", "US", dt),
            "tag": get_event_tag("", "US", dt),
        },
    }


def next_market_open(market: str, from_dt: Optional[datetime] = None) -> Optional[datetime]:
    """
    Return the next open datetime for *market* after *from_dt*.

    Returns None for CRYPTO (always open).
    Returns datetime in UTC for US market.
    """
    if market.upper() == "CRYPTO":
        return None  # Always open

    if from_dt is None:
        from_dt = datetime.now(tz=TZ_UTC)
    elif from_dt.tzinfo is None:
        from_dt = TZ_UTC.localize(from_dt)

    from_dt_et = from_dt.astimezone(TZ_NEW_YORK)

    # Walk forward day by day until we find a trading day with an open time in the future
    for day_offset in range(14):  # max 14 days look-ahead
        candidate_date = (from_dt_et + timedelta(days=day_offset)).date()

        if not _calendar.is_trading_day(candidate_date):
            continue

        open_dt_et = TZ_NEW_YORK.localize(
            datetime(
                candidate_date.year,
                candidate_date.month,
                candidate_date.day,
                NYSE_OPEN_LOCAL[0],
                NYSE_OPEN_LOCAL[1],
            )
        )

        if open_dt_et > from_dt_et:
            return open_dt_et.astimezone(TZ_UTC)

    logger.error("next_market_open: could not find next open within 14 days")
    return None


# ---------------------------------------------------------------------------
# Module self-test (run with: python module2_market.py)
# ---------------------------------------------------------------------------

def _run_self_test() -> None:
    """Comprehensive smoke-test for all functions including new screenshot window."""
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    print("=" * 60)
    print("module2_market.py — Self Test")
    print("=" * 60)

    passed = 0
    failed = 0

    def check(label: str, result: bool, expected: bool) -> None:
        nonlocal passed, failed
        status = "PASS" if result == expected else "FAIL"
        if result == expected:
            passed += 1
        else:
            failed += 1
        print(f"  [{status}] {label}")
        if result != expected:
            print(f"         expected={expected}, got={result}")

    # ── CRYPTO ──────────────────────────────────────────────────────
    print("\n--- CRYPTO ---")
    dt_utc = datetime.now(tz=TZ_UTC)
    check("Crypto is_market_open always True",
          is_market_open("BTCUSDT", "CRYPTO", dt_utc), True)
    check("Crypto is_screenshot_time always True",
          is_screenshot_time("BTCUSDT", "CRYPTO", dt_utc), True)

    dt_funding_0 = TZ_UTC.localize(datetime(2026, 4, 14, 0, 0, 0))
    dt_funding_8 = TZ_UTC.localize(datetime(2026, 4, 14, 8, 0, 0))
    dt_funding_16 = TZ_UTC.localize(datetime(2026, 4, 14, 16, 0, 0))
    check("CRYPTO_FUNDING tag at 00:00 UTC",
          get_event_tag("BTCUSDT", "CRYPTO", dt_funding_0), "CRYPTO_FUNDING")
    check("CRYPTO_FUNDING tag at 08:00 UTC",
          get_event_tag("BTCUSDT", "CRYPTO", dt_funding_8), "CRYPTO_FUNDING")
    check("CRYPTO_FUNDING tag at 16:00 UTC",
          get_event_tag("BTCUSDT", "CRYPTO", dt_funding_16), "CRYPTO_FUNDING")
    check("No tag at 12:00 UTC",
          get_event_tag("BTCUSDT", "CRYPTO",
                        TZ_UTC.localize(datetime(2026, 4, 14, 12, 0, 0))), "")

    # ── US — is_market_open (original window 09:30–16:00) ──────────
    print("\n--- US is_market_open (09:30–16:00 ET) ---")
    dt_sat = TZ_NEW_YORK.localize(datetime(2026, 4, 11, 10, 0, 0))
    dt_open = TZ_NEW_YORK.localize(datetime(2026, 4, 14, 10, 0, 0))
    dt_pre930 = TZ_NEW_YORK.localize(datetime(2026, 4, 14, 9, 0, 0))
    dt_post16 = TZ_NEW_YORK.localize(datetime(2026, 4, 14, 16, 5, 0))
    dt_xmas = TZ_NEW_YORK.localize(datetime(2026, 12, 25, 10, 0, 0))

    check("US closed on Saturday",
          is_market_open("AAPL", "US", dt_sat), False)
    check("US open Tuesday 10:00 ET",
          is_market_open("AAPL", "US", dt_open), True)
    check("US closed before 09:30 ET",
          is_market_open("AAPL", "US", dt_pre930), False)
    check("US closed after 16:00 ET",
          is_market_open("AAPL", "US", dt_post16), False)
    check("US closed Christmas 2026",
          is_market_open("AAPL", "US", dt_xmas), False)

    # ── US — is_screenshot_time (new window 07:30–18:00 ET) ────────
    print("\n--- US is_screenshot_time (07:30–18:00 ET) ---")
    t = lambda h, m: TZ_NEW_YORK.localize(datetime(2026, 4, 14, h, m, 0))
    sat_t = lambda h, m: TZ_NEW_YORK.localize(datetime(2026, 4, 11, h, m, 0))

    check("US screenshot at 07:30 ET (window start)",
          is_screenshot_time("AAPL", "US", t(7, 30)), True)
    check("US screenshot at 07:29 ET (before window)",
          is_screenshot_time("AAPL", "US", t(7, 29)), False)
    check("US screenshot at 06:00 ET (before window)",
          is_screenshot_time("AAPL", "US", t(6, 0)), False)
    check("US screenshot at 09:30 ET (NYSE open)",
          is_screenshot_time("AAPL", "US", t(9, 30)), True)
    check("US screenshot at 12:00 ET (midday)",
          is_screenshot_time("AAPL", "US", t(12, 0)), True)
    check("US screenshot at 16:00 ET (NYSE close)",
          is_screenshot_time("AAPL", "US", t(16, 0)), True)
    check("US screenshot at 17:00 ET (post-market)",
          is_screenshot_time("AAPL", "US", t(17, 0)), True)
    check("US screenshot at 18:00 ET (window end inclusive)",
          is_screenshot_time("AAPL", "US", t(18, 0)), True)
    check("US screenshot at 18:01 ET (after window)",
          is_screenshot_time("AAPL", "US", t(18, 1)), False)
    check("US screenshot at 20:00 ET (after window)",
          is_screenshot_time("AAPL", "US", t(20, 0)), False)
    check("US NO screenshot on Saturday 10:00 ET",
          is_screenshot_time("AAPL", "US", sat_t(10, 0)), False)
    check("US NO screenshot Christmas 2026",
          is_screenshot_time("AAPL", "US",
                             TZ_NEW_YORK.localize(datetime(2026, 12, 25, 10, 0, 0))), False)

    # ── Event tags ──────────────────────────────────────────────────
    print("\n--- US event tags ---")
    check("PRE_MARKET tag at 07:30 ET",
          get_event_tag("AAPL", "US", t(7, 30)), "PRE_MARKET")
    check("NY_OPEN tag at 09:30 ET",
          get_event_tag("AAPL", "US", t(9, 30)), "NY_OPEN")
    check("NY_CLOSE tag at 16:00 ET",
          get_event_tag("AAPL", "US", t(16, 0)), "NY_CLOSE")
    check("POST_MARKET tag at 18:00 ET",
          get_event_tag("AAPL", "US", t(18, 0)), "POST_MARKET")
    check("No tag at 12:00 ET",
          get_event_tag("AAPL", "US", t(12, 0)), "")

    # ── next_market_open ────────────────────────────────────────────
    print("\n--- next_market_open ---")
    dt_weekend = TZ_UTC.localize(datetime(2026, 4, 11, 15, 0, 0))  # Saturday
    next_open = next_market_open("US", dt_weekend)
    assert next_open is not None
    next_et = next_open.astimezone(TZ_NEW_YORK)
    check("next_market_open from Saturday is a weekday",
          next_et.weekday() < 5, True)
    check("next_market_open time is 09:30 ET",
          (next_et.hour, next_et.minute) == NYSE_OPEN_LOCAL, True)
    check("CRYPTO next_market_open returns None",
          next_market_open("CRYPTO") is None, True)

    # ── Status summary ──────────────────────────────────────────────
    print("\n--- get_market_status_summary ---")
    summary = get_market_status_summary()
    check("Summary has CRYPTO key", "CRYPTO" in summary, True)
    check("Summary has US key", "US" in summary, True)
    check("Summary has screenshot field",
          "screenshot" in summary["CRYPTO"], True)

    # ── Final result ────────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    if failed == 0:
        print("All tests passed ✅")
    else:
        print(f"⚠️  {failed} test(s) FAILED — see above")
    print("=" * 60)
    return failed == 0


if __name__ == "__main__":
    import sys as _sys
    _sys.exit(0 if _run_self_test() else 1)
