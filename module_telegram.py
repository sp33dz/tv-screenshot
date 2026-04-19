"""
module_telegram.py — Telegram Bot Integration
TradingView Auto Screenshot System

Features:
- ส่งรูป screenshot ไป Telegram ผ่าน sendPhoto API
- Caption format + hashtag ตามที่กำหนด
- Personal Bot mode (bot ส่งหาตัวเองผ่าน chat_id)
- Retry with exponential backoff
- Rate limit aware (30 msg/sec global, 1 msg/sec/chat)
- รองรับ Drive label ใน caption ("💾 Drive2 [US-A]")

Caption format:
    📊 {SYMBOL} | {MARKET}
    🕐 {YYYY-MM-DD HH:MM} ET
    💾 {DriveN} [{LABEL}]

    #{SYMBOL} #{MARKET} #{DriveN} #TV_{YYYYMMDD} #TV_{YYYYMM} #H{HH}00

GitHub Secrets ที่ต้องมี:
    TELEGRAM_BOT_TOKEN   e.g. "123456789:ABCDEFxxxxxxx"
    TELEGRAM_CHAT_ID     e.g. "987654321"  (ดูได้จาก @userinfobot)
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pytz
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TELEGRAM_API_BASE: str = "https://api.telegram.org/bot{token}"
SEND_PHOTO_ENDPOINT: str = "/sendPhoto"

MAX_CAPTION_BYTES: int = 1024           # Telegram limit for caption
MAX_RETRIES: int = 3
RETRY_BASE_DELAY_SEC: float = 2.0      # exponential backoff base
REQUEST_TIMEOUT_SEC: int = 30

TZ_NEW_YORK: pytz.BaseTzInfo = pytz.timezone("America/New_York")
TZ_UTC: pytz.BaseTzInfo = pytz.utc
TZ_BANGKOK: pytz.BaseTzInfo = pytz.timezone("Asia/Bangkok")  # UTC+7


# ---------------------------------------------------------------------------
# TelegramSender
# ---------------------------------------------------------------------------

class TelegramSender:
    """
    Sends screenshots to Telegram Bot.

    Usage:
        sender = TelegramSender.from_env()
        result = sender.send_screenshot(
            image_path=Path("./screenshots/US/AAPL/2026-04/AAPL_2026-04-14_09-30_NY_OPEN.png"),
            symbol="AAPL",
            market="US",
            dt=datetime(2026, 4, 14, 9, 30, tzinfo=pytz.timezone("America/New_York")),
            drive_name="Drive2",
            drive_label="US-A",
        )
    """

    def __init__(self, bot_token: str, chat_id: str) -> None:
        """
        Parameters
        ----------
        bot_token : Telegram bot token (from @BotFather)
        chat_id   : Target chat ID (your own user ID for personal bot)
        """
        if not bot_token:
            raise ValueError("TelegramSender: bot_token must not be empty")
        if not chat_id:
            raise ValueError("TelegramSender: chat_id must not be empty")

        self._token: str = bot_token.strip()
        self._chat_id: str = str(chat_id).strip()
        self._base_url: str = TELEGRAM_API_BASE.format(token=self._token)
        self._session: Optional[requests.Session] = None

        logger.info(
            "TelegramSender init: chat_id=%s token=***%s",
            self._chat_id,
            self._token[-4:],
        )

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "TelegramSender":
        """
        Create TelegramSender from environment variables.

        Reads:
            TELEGRAM_BOT_TOKEN
            TELEGRAM_CHAT_ID

        Raises ValueError if either is missing.
        """
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

        if not token:
            raise ValueError(
                "TELEGRAM_BOT_TOKEN environment variable is not set. "
                "Add it as a GitHub Secret or export locally."
            )
        if not chat_id:
            raise ValueError(
                "TELEGRAM_CHAT_ID environment variable is not set. "
                "Get your ID from @userinfobot on Telegram."
            )

        return cls(bot_token=token, chat_id=chat_id)

    @classmethod
    def from_config(cls, config: dict) -> Optional["TelegramSender"]:
        """
        Create TelegramSender from config dict.

        Reads 'telegram_bot_token' and 'telegram_chat_id' from config,
        falling back to environment variables.

        Returns None if credentials are not available (Telegram disabled).
        """
        token = (
            config.get("telegram_bot_token", "")
            or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        ).strip()
        chat_id = (
            config.get("telegram_chat_id", "")
            or os.environ.get("TELEGRAM_CHAT_ID", "")
        ).strip()

        if not token or not chat_id:
            logger.info(
                "TelegramSender: credentials not found — Telegram disabled. "
                "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
            )
            return None

        return cls(bot_token=token, chat_id=chat_id)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_screenshot(
        self,
        image_path: Path,
        symbol: str,
        market: str,
        dt: datetime,
        drive_name: str = "",
        drive_label: str = "",
        tag: str = "",
    ) -> bool:
        """
        Send a screenshot to Telegram with formatted caption.

        Parameters
        ----------
        image_path  : local path to PNG file
        symbol      : e.g. "AAPL"
        market      : e.g. "US" or "CRYPTO"
        dt          : datetime of the screenshot (any tz — will convert to ET for caption)
        drive_name  : e.g. "Drive2"  (shown in caption)
        drive_label : e.g. "US-A"    (shown in caption brackets)
        tag         : optional event tag e.g. "NY_OPEN" (added to caption)

        Returns True on success, False on failure.
        """
        image_path = Path(image_path)
        if not image_path.exists():
            logger.error("send_screenshot: file not found: %s", image_path)
            return False

        caption = self._build_caption(
            symbol=symbol,
            market=market,
            dt=dt,
            drive_name=drive_name,
            drive_label=drive_label,
            tag=tag,
        )

        return self._send_photo_with_retry(image_path, caption)

    def send_text(self, text: str) -> bool:
        """
        Send a plain text message to the configured chat.

        Returns True on success, False on failure.
        """
        url = self._base_url + "/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": text[:4096],  # Telegram message limit
            "parse_mode": "HTML",
        }
        try:
            resp = self._get_session().post(
                url, data=payload, timeout=REQUEST_TIMEOUT_SEC
            )
            if resp.status_code == 200:
                logger.info("Telegram text sent OK")
                return True
            logger.warning(
                "Telegram sendMessage failed [%d]: %s",
                resp.status_code, resp.text[:200],
            )
            return False
        except Exception as exc:
            logger.error("Telegram sendMessage error: %s", exc)
            return False

    def verify_connection(self) -> bool:
        """
        Verify bot token is valid via getMe API call.

        Returns True if bot is reachable and token is valid.
        """
        url = self._base_url + "/getMe"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                bot_info = data.get("result", {})
                logger.info(
                    "Telegram bot verified: @%s (id=%s)",
                    bot_info.get("username", "?"),
                    bot_info.get("id", "?"),
                )
                return True
            logger.warning(
                "Telegram getMe failed [%d]: %s",
                resp.status_code, resp.text[:200],
            )
            return False
        except Exception as exc:
            logger.error("Telegram verify_connection error: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Caption builder
    # ------------------------------------------------------------------

    @staticmethod
    def _build_caption(
        symbol: str,
        market: str,
        dt: datetime,
        drive_name: str = "",
        drive_label: str = "",
        tag: str = "",
    ) -> str:
        """
        Build Telegram caption in the required format.

        Format:
            📊 {SYMBOL} | {MARKET}
            🕐 {YYYY-MM-DD HH:MM} ET
            💾 {DriveN} [{LABEL}]

            #{SYMBOL} #{MARKET} #{DriveN} #TV_{YYYYMMDD} #TV_{YYYYMM} #H{HH}00

        If drive_name is empty, the 💾 line is omitted.
        If tag is set, it's appended to the first line: "📊 AAPL | US [NY_OPEN]"
        """
        # Convert dt to ET for display
        if dt.tzinfo is None:
            dt_et = TZ_NEW_YORK.localize(dt)
        else:
            dt_et = dt.astimezone(TZ_NEW_YORK)

        # Convert to Bangkok time (UTC+7) for display
        dt_bkk = dt_et.astimezone(TZ_BANGKOK)

        # --- Line 1: symbol + market + optional tag ---
        tag_suffix = f" [{tag}]" if tag else ""
        line1 = f"📊 {symbol} | {market}{tag_suffix}"

        # --- Line 2: timestamp ET + Bangkok ---
        ts_et  = dt_et.strftime("%Y-%m-%d %H:%M")
        ts_bkk = dt_bkk.strftime("%H:%M")
        line2 = f"🕐 {ts_et} ET  |  {ts_bkk} ICT"

        # --- Line 3: drive info (optional) ---
        drive_str = ""
        if drive_name:
            if drive_label:
                drive_str = f"💾 {drive_name} [{drive_label}]"
            else:
                drive_str = f"💾 {drive_name}"

        # --- Hashtags ---
        yyyymmdd = dt_et.strftime("%Y%m%d")
        yyyymm = dt_et.strftime("%Y%m")
        hh = dt_et.strftime("%H")
        drive_tag = f" #{drive_name}" if drive_name else ""
        hashtags = (
            f"#{symbol} #{market}{drive_tag}"
            f" #TV_{yyyymmdd} #TV_{yyyymm} #H{hh}00"
        )

        # --- Assemble ---
        parts = [line1, line2]
        if drive_str:
            parts.append(drive_str)
        parts.append("")  # blank line before hashtags
        parts.append(hashtags)

        caption = "\n".join(parts)

        # Enforce Telegram caption byte limit (1024 bytes)
        caption_bytes = caption.encode("utf-8")
        if len(caption_bytes) > MAX_CAPTION_BYTES:
            # Trim hashtags first
            caption = "\n".join(parts[:3])
            logger.warning("Caption truncated to fit Telegram limit")

        return caption

    # ------------------------------------------------------------------
    # HTTP sender with retry
    # ------------------------------------------------------------------

    def _send_photo_with_retry(self, image_path: Path, caption: str) -> bool:
        """
        Upload photo to Telegram with retries and exponential backoff.

        Returns True on success.
        """
        url = self._base_url + SEND_PHOTO_ENDPOINT

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with open(image_path, "rb") as photo_file:
                    resp = self._get_session().post(
                        url,
                        data={
                            "chat_id": self._chat_id,
                            "caption": caption,
                            "parse_mode": "HTML",
                        },
                        files={"photo": (image_path.name, photo_file, "image/png")},
                        timeout=REQUEST_TIMEOUT_SEC,
                    )

                if resp.status_code == 200:
                    logger.info(
                        "Telegram sendPhoto OK: %s [attempt %d/%d]",
                        image_path.name, attempt, MAX_RETRIES,
                    )
                    return True

                # Handle rate limit (429)
                if resp.status_code == 429:
                    retry_after = 1
                    try:
                        retry_after = resp.json().get("parameters", {}).get("retry_after", 1)
                    except Exception:
                        pass
                    logger.warning(
                        "Telegram rate limit — retry after %ds [attempt %d/%d]",
                        retry_after, attempt, MAX_RETRIES,
                    )
                    time.sleep(float(retry_after) + 0.5)
                    continue

                # Other HTTP error
                logger.warning(
                    "Telegram sendPhoto [%d] attempt %d/%d: %s",
                    resp.status_code, attempt, MAX_RETRIES,
                    resp.text[:300],
                )

            except requests.exceptions.ConnectionError as exc:
                logger.warning(
                    "Telegram connection error attempt %d/%d: %s",
                    attempt, MAX_RETRIES, exc,
                )
            except requests.exceptions.Timeout:
                logger.warning(
                    "Telegram timeout attempt %d/%d (timeout=%ds)",
                    attempt, MAX_RETRIES, REQUEST_TIMEOUT_SEC,
                )
            except Exception as exc:
                logger.error(
                    "Telegram unexpected error attempt %d/%d: %s",
                    attempt, MAX_RETRIES, exc, exc_info=True,
                )

            # Exponential backoff before next attempt
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1))
                logger.info("Retrying in %.1fs...", delay)
                time.sleep(delay)

        logger.error(
            "Telegram sendPhoto FAILED after %d attempts: %s",
            MAX_RETRIES, image_path.name,
        )
        return False

    def _get_session(self) -> requests.Session:
        """Get or create a reusable requests.Session."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": "TradingViewAutoShot/1.0",
            })
        return self._session

    def close(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None


# ---------------------------------------------------------------------------
# Convenience function used by module1_core post-capture hook
# ---------------------------------------------------------------------------

def send_screenshot_to_telegram(
    sender: Optional[TelegramSender],
    image_path: Path,
    symbol: str,
    market: str,
    dt: datetime,
    drive_name: str = "",
    drive_label: str = "",
    tag: str = "",
) -> bool:
    """
    Thin wrapper: send screenshot if sender is not None.

    Returns True on success or if sender is None (Telegram disabled).
    Used as post-capture hook in module1_core.
    """
    if sender is None:
        return True  # Telegram disabled — not an error

    try:
        return sender.send_screenshot(
            image_path=image_path,
            symbol=symbol,
            market=market,
            dt=dt,
            drive_name=drive_name,
            drive_label=drive_label,
            tag=tag,
        )
    except Exception as exc:
        logger.error("send_screenshot_to_telegram error: %s", exc, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Self-test (run with: python module_telegram.py)
# ---------------------------------------------------------------------------

def _run_self_test() -> None:
    """Smoke-test for TelegramSender (caption builder only — no real API call)."""
    import sys
    import tempfile

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    print("=" * 60)
    print("module_telegram.py — Self Test (caption builder, no API call)")
    print("=" * 60)

    passed = 0
    failed = 0

    def check(label: str, cond: bool) -> None:
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"  [PASS] {label}")
        else:
            failed += 1
            print(f"  [FAIL] {label}")

    # ── Caption builder ──────────────────────────────────────────────
    print("\n--- Caption builder ---")
    dt_et = TZ_NEW_YORK.localize(datetime(2026, 4, 14, 9, 30, 0))

    caption = TelegramSender._build_caption(
        symbol="AAPL",
        market="US",
        dt=dt_et,
        drive_name="Drive2",
        drive_label="US-A",
        tag="NY_OPEN",
    )
    print("  Caption:\n" + "\n".join("    " + l for l in caption.split("\n")))

    check("Contains symbol",          "AAPL" in caption)
    check("Contains market",          "US" in caption)
    check("Contains ET timestamp",    "2026-04-14 09:30 ET" in caption)
    check("Contains drive name",      "Drive2" in caption)
    check("Contains drive label",     "US-A" in caption)
    check("Contains tag",             "NY_OPEN" in caption)
    check("Contains #AAPL hashtag",   "#AAPL" in caption)
    check("Contains #US hashtag",     "#US" in caption)
    check("Contains #Drive2 hashtag", "#Drive2" in caption)
    check("Contains #TV_20260414",    "#TV_20260414" in caption)
    check("Contains #TV_202604",      "#TV_202604" in caption)
    check("Contains #H0900",          "#H0900" in caption)
    check("Emoji 📊 present",         "📊" in caption)
    check("Emoji 🕐 present",         "🕐" in caption)
    check("Emoji 💾 present",         "💾" in caption)
    check("Caption fits in 1024 bytes", len(caption.encode("utf-8")) <= MAX_CAPTION_BYTES)

    # ── No drive ─────────────────────────────────────────────────────
    print("\n--- Caption without drive ---")
    caption2 = TelegramSender._build_caption(
        symbol="BTCTHB",
        market="CRYPTO",
        dt=dt_et,
        drive_name="",
        drive_label="",
        tag="CRYPTO_FUNDING",
    )
    check("No 💾 when drive_name empty", "💾" not in caption2)
    check("CRYPTO_FUNDING tag in caption", "CRYPTO_FUNDING" in caption2)
    check("#BTCTHB hashtag present", "#BTCTHB" in caption2)

    # ── Timezone conversion ──────────────────────────────────────────
    print("\n--- Timezone conversion ---")
    dt_utc = TZ_UTC.localize(datetime(2026, 4, 14, 13, 30, 0))  # 13:30 UTC = 09:30 ET
    caption3 = TelegramSender._build_caption(
        symbol="TSLA",
        market="US",
        dt=dt_utc,
        drive_name="Drive3",
        drive_label="US-B",
    )
    check("UTC converted to ET in caption", "09:30 ET" in caption3)

    # ── Naive datetime (assumes local) ───────────────────────────────
    print("\n--- Naive datetime ---")
    dt_naive = datetime(2026, 4, 14, 9, 30, 0)
    caption4 = TelegramSender._build_caption(
        symbol="AAPL",
        market="US",
        dt=dt_naive,
        drive_name="Drive2",
        drive_label="US-A",
    )
    check("Naive datetime handled without crash", "AAPL" in caption4)

    # ── from_config returns None without credentials ─────────────────
    print("\n--- from_config without credentials ---")
    sender_none = TelegramSender.from_config({})
    check("from_config returns None without credentials", sender_none is None)

    # ── send helper with None sender ─────────────────────────────────
    print("\n--- send helper with None sender ---")
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        tmp_path = Path(f.name)
        f.write(b"fake")
    result = send_screenshot_to_telegram(
        sender=None,
        image_path=tmp_path,
        symbol="AAPL",
        market="US",
        dt=dt_et,
    )
    tmp_path.unlink(missing_ok=True)
    check("None sender returns True (disabled, not error)", result is True)

    # ── Final result ─────────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    if failed == 0:
        print("All tests passed ✅")
    else:
        print(f"⚠️  {failed} test(s) FAILED — see above")
    print("=" * 60)


if __name__ == "__main__":
    _run_self_test()
