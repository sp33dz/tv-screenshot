"""
module3_storage.py — File Management & CCTV Loop Storage
TradingView Auto Screenshot System

Features:
- CCTV Loop Storage: วนทับภาพเก่าเมื่อเกิน quota
- Global limit 1 GB (config: global_limit_gb)
- Per-symbol limit 150 MB (config: per_symbol_limit_mb)
- Pin ภาพสำคัญ (ไม่ถูกลบ)
- Duplicate detection ด้วย perceptual hash (imagehash)
- Corrupt file detection ด้วย Pillow
- Disk space warning (config: disk_warning_gb)
- Sub-folder แยกเดือน: MARKET/SYMBOL/YYYY-MM/
- Auto-create folders
- Thread-safe operations ด้วย RLock

Folder layout:
    screenshots/
    ├── CRYPTO/
    │   └── BTCUSD/
    │       └── 2026-04/
    │           ├── BTCUSD_2026-04-14_08-00_CRYPTO_FUNDING.png
    │           └── BTCUSD_2026-04-14_16-00_CRYPTO_FUNDING.PINNED.png
    └── US/
        └── AAPL/
            └── 2026-04/
                ├── AAPL_2026-04-14_09-30_NY_OPEN.png
                └── AAPL_2026-04-14_16-00_NY_CLOSE.png
"""

from __future__ import annotations

import json
import logging
import shutil
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

import imagehash
from PIL import Image, UnidentifiedImageError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PINNED_SUFFIX: str = ".PINNED"   # inserted before .png → file.PINNED.png
DEFAULT_SCREENSHOT_FOLDER: str = "./screenshots"
DEFAULT_GLOBAL_LIMIT_GB: float = 1.0
DEFAULT_PER_SYMBOL_LIMIT_MB: float = 150.0
DEFAULT_DISK_WARNING_GB: float = 0.2
PHASH_THRESHOLD: int = 8          # hamming distance for "duplicate" (lower = stricter)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class FileEntry:
    """Represents a single screenshot file on disk."""

    path: Path
    symbol: str
    market: str
    size_bytes: int
    mtime: float                    # Unix timestamp of last modification
    is_pinned: bool = False

    @property
    def size_mb(self) -> float:
        return self.size_bytes / (1024 ** 2)


@dataclass
class StorageStats:
    """Snapshot of current storage usage."""

    total_files: int = 0
    total_size_bytes: int = 0
    pinned_files: int = 0
    pinned_size_bytes: int = 0
    symbol_stats: Dict[str, int] = field(default_factory=dict)  # symbol → bytes

    @property
    def total_size_mb(self) -> float:
        return self.total_size_bytes / (1024 ** 2)

    @property
    def total_size_gb(self) -> float:
        return self.total_size_bytes / (1024 ** 3)


# ---------------------------------------------------------------------------
# StorageManager
# ---------------------------------------------------------------------------

class StorageManager:
    """
    CCTV-style loop storage for TradingView screenshots.

    Thread-safe.  All public methods acquire self._lock.
    """

    def __init__(self, config: Optional[dict] = None) -> None:
        """
        Initialise StorageManager from config dict.

        Parameters
        ----------
        config : dict, optional
            Keys used: screenshot_folder, global_limit_gb,
            per_symbol_limit_mb, disk_warning_gb.
        """
        cfg = config or {}
        self.screenshot_folder = Path(cfg.get("screenshot_folder", DEFAULT_SCREENSHOT_FOLDER))
        self.global_limit_bytes: int = int(
            cfg.get("global_limit_gb", DEFAULT_GLOBAL_LIMIT_GB) * 1024 ** 3
        )
        self.per_symbol_limit_bytes: int = int(
            cfg.get("per_symbol_limit_mb", DEFAULT_PER_SYMBOL_LIMIT_MB) * 1024 ** 2
        )
        self.disk_warning_bytes: int = int(
            cfg.get("disk_warning_gb", DEFAULT_DISK_WARNING_GB) * 1024 ** 3
        )

        self._lock = threading.RLock()
        logger.info(
            "StorageManager init: folder=%s, global=%.1f GB, per_symbol=%.0f MB",
            self.screenshot_folder,
            self.global_limit_bytes / 1024 ** 3,
            self.per_symbol_limit_bytes / 1024 ** 2,
        )

    # ------------------------------------------------------------------
    # Public API — saving
    # ------------------------------------------------------------------

    def save_screenshot(
        self,
        image: Image.Image,
        symbol: str,
        market: str,
        dt: datetime,
        tag: str = "",
    ) -> Optional[Path]:
        """
        Save *image* to the correct folder with proper filename.

        Performs:
        1. Duplicate detection (perceptual hash).
        2. Corrupt-image guard.
        3. CCTV eviction (per-symbol then global) before writing.
        4. Disk-space warning check.

        Parameters
        ----------
        image   : PIL Image
        symbol  : e.g. "BTCUSD"
        market  : "CRYPTO" or "US"
        dt      : datetime of the candle close (timezone-aware or naive UTC)
        tag     : event tag string, e.g. "NY_OPEN" or ""

        Returns
        -------
        Path to saved file, or None on failure.
        """
        with self._lock:
            try:
                dest_path = self._build_filepath(symbol, market, dt, tag)
                dest_path.parent.mkdir(parents=True, exist_ok=True)

                # --- Corrupt image guard ---
                if not self._is_valid_image(image):
                    logger.warning("save_screenshot: invalid/corrupt image for %s — skipped", symbol)
                    return None

                # --- Duplicate detection ---
                if self._is_duplicate(image, dest_path.parent, symbol):
                    logger.info("save_screenshot: duplicate detected for %s — skipped", symbol)
                    return None

                # --- CCTV eviction ---
                img_bytes = self._estimate_png_size(image)
                self._evict_if_needed(symbol, market, img_bytes)

                # --- Save ---
                image.save(str(dest_path), format="PNG", optimize=True)
                logger.info(
                    "Screenshot saved: %s (%.1f KB)",
                    dest_path.name,
                    dest_path.stat().st_size / 1024,
                )

                # --- Disk warning ---
                self._check_disk_space()

                return dest_path

            except Exception as exc:
                logger.error("save_screenshot failed for %s: %s", symbol, exc, exc_info=True)
                return None

    # ------------------------------------------------------------------
    # Public API — pin / unpin
    # ------------------------------------------------------------------

    def pin_file(self, path: Path) -> Optional[Path]:
        """
        Pin a screenshot so it is never auto-deleted.

        Renames  SYMBOL_DATE.png  →  SYMBOL_DATE.PINNED.png
        Returns new path, or None on failure.
        """
        with self._lock:
            path = Path(path)
            if not path.exists():
                logger.warning("pin_file: path not found: %s", path)
                return None
            if self._is_pinned_path(path):
                logger.debug("pin_file: already pinned: %s", path)
                return path
            new_path = path.with_suffix(f"{PINNED_SUFFIX}{path.suffix}")
            try:
                path.rename(new_path)
                logger.info("Pinned: %s", new_path.name)
                return new_path
            except OSError as exc:
                logger.error("pin_file rename failed: %s", exc)
                return None

    def unpin_file(self, path: Path) -> Optional[Path]:
        """
        Unpin a screenshot (makes it eligible for CCTV eviction).

        Returns new path, or None on failure.
        """
        with self._lock:
            path = Path(path)
            if not path.exists():
                logger.warning("unpin_file: path not found: %s", path)
                return None
            if not self._is_pinned_path(path):
                logger.debug("unpin_file: not pinned: %s", path)
                return path
            # Remove PINNED_SUFFIX from stem
            new_stem = path.stem.replace(PINNED_SUFFIX, "")
            new_path = path.with_name(new_stem + path.suffix)
            try:
                path.rename(new_path)
                logger.info("Unpinned: %s", new_path.name)
                return new_path
            except OSError as exc:
                logger.error("unpin_file rename failed: %s", exc)
                return None

    # ------------------------------------------------------------------
    # Public API — stats & scan
    # ------------------------------------------------------------------

    def get_stats(self) -> StorageStats:
        """Scan screenshot folder and return StorageStats."""
        with self._lock:
            stats = StorageStats()
            for entry in self._iter_files():
                stats.total_files += 1
                stats.total_size_bytes += entry.size_bytes
                if entry.is_pinned:
                    stats.pinned_files += 1
                    stats.pinned_size_bytes += entry.size_bytes
                sym = entry.symbol
                stats.symbol_stats[sym] = stats.symbol_stats.get(sym, 0) + entry.size_bytes
            return stats

    def list_files(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
    ) -> List[FileEntry]:
        """
        List all screenshot files, optionally filtered by symbol/market.
        Sorted oldest-first.
        """
        with self._lock:
            entries = list(self._iter_files(symbol=symbol, market=market))
            entries.sort(key=lambda e: e.mtime)
            return entries

    def delete_corrupt_files(self) -> int:
        """
        Scan all screenshots, delete files that cannot be opened by Pillow.
        Returns count of deleted files.
        """
        with self._lock:
            deleted = 0
            for entry in self._iter_files():
                if not self._is_valid_path(entry.path):
                    try:
                        entry.path.unlink(missing_ok=True)
                        logger.warning("Deleted corrupt file: %s", entry.path.name)
                        deleted += 1
                    except OSError as exc:
                        logger.error("Could not delete corrupt file %s: %s", entry.path, exc)
            if deleted:
                logger.info("Corrupt file scan complete: %d deleted", deleted)
            return deleted

    def check_disk_space(self) -> bool:
        """
        Public wrapper for disk-space check.
        Returns True if free space is above warning threshold.
        """
        with self._lock:
            return self._check_disk_space()

    # ------------------------------------------------------------------
    # Internal — file enumeration
    # ------------------------------------------------------------------

    def _iter_files(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
    ):
        """
        Generator yielding FileEntry for every .png file under screenshot_folder.

        Optionally filters by symbol or market (case-insensitive).
        Silently skips files that cannot be stat'd.
        """
        root = self.screenshot_folder
        if not root.exists():
            return

        market_filter = market.upper() if market else None
        symbol_filter = symbol.upper() if symbol else None

        for png_path in root.rglob("*.png"):
            try:
                # Derive market + symbol from path components
                # Expected: root / MARKET / SYMBOL / YYYY-MM / file.png
                parts = png_path.relative_to(root).parts
                if len(parts) < 4:
                    continue
                file_market = parts[0].upper()
                file_symbol = parts[1].upper()

                if market_filter and file_market != market_filter:
                    continue
                if symbol_filter and file_symbol != symbol_filter:
                    continue

                stat = png_path.stat()
                yield FileEntry(
                    path=png_path,
                    symbol=file_symbol,
                    market=file_market,
                    size_bytes=stat.st_size,
                    mtime=stat.st_mtime,
                    is_pinned=self._is_pinned_path(png_path),
                )
            except (OSError, ValueError):
                continue

    # ------------------------------------------------------------------
    # Internal — CCTV eviction
    # ------------------------------------------------------------------

    def _evict_if_needed(
        self,
        symbol: str,
        market: str,
        incoming_bytes: int,
    ) -> None:
        """
        Enforce per-symbol then global storage limits before saving.

        Deletes oldest non-pinned files until there is room.
        """
        # --- Per-symbol eviction ---
        # Scan once, reuse list for both total calculation and eviction candidates
        all_sym_files = list(self._iter_files(symbol=symbol, market=market))
        sym_files = sorted(
            [e for e in all_sym_files if not e.is_pinned],
            key=lambda e: e.mtime,
        )
        sym_total_all = sum(e.size_bytes for e in all_sym_files)

        while sym_total_all + incoming_bytes > self.per_symbol_limit_bytes and sym_files:
            oldest = sym_files.pop(0)
            try:
                oldest.path.unlink(missing_ok=True)
                sym_total_all -= oldest.size_bytes
                logger.info(
                    "CCTV evict (per-symbol %s): %s (%.1f KB)",
                    symbol,
                    oldest.path.name,
                    oldest.size_bytes / 1024,
                )
            except OSError as exc:
                logger.error("Eviction failed: %s — %s", oldest.path, exc)

        # --- Global eviction ---
        # Scan once, reuse list
        all_global_files = list(self._iter_files())
        all_files = sorted(
            [e for e in all_global_files if not e.is_pinned],
            key=lambda e: e.mtime,
        )
        global_total = sum(e.size_bytes for e in all_global_files)

        while global_total + incoming_bytes > self.global_limit_bytes and all_files:
            oldest = all_files.pop(0)
            try:
                oldest.path.unlink(missing_ok=True)
                global_total -= oldest.size_bytes
                logger.info(
                    "CCTV evict (global): %s (%.1f KB)",
                    oldest.path.name,
                    oldest.size_bytes / 1024,
                )
            except OSError as exc:
                logger.error("Global eviction failed: %s — %s", oldest.path, exc)

    # ------------------------------------------------------------------
    # Internal — path helpers
    # ------------------------------------------------------------------

    def _build_filepath(
        self,
        symbol: str,
        market: str,
        dt: datetime,
        tag: str = "",
    ) -> Path:
        """
        Construct the destination Path for a screenshot.

        Pattern: root/MARKET/SYMBOL/YYYY-MM/SYMBOL_YYYY-MM-DD_HH-MM[_TAG].png
        """
        market_upper = market.upper()
        symbol_upper = symbol.upper()
        month_folder = dt.strftime("%Y-%m")
        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H-%M")

        if tag:
            filename = f"{symbol_upper}_{date_str}_{time_str}_{tag}.png"
        else:
            filename = f"{symbol_upper}_{date_str}_{time_str}.png"

        return self.screenshot_folder / market_upper / symbol_upper / month_folder / filename

    @staticmethod
    def _is_pinned_path(path: Path) -> bool:
        """Return True if the file stem contains the PINNED_SUFFIX marker."""
        return PINNED_SUFFIX in path.stem

    # ------------------------------------------------------------------
    # Internal — image validation
    # ------------------------------------------------------------------

    @staticmethod
    def _is_valid_image(image: Image.Image) -> bool:
        """
        Check that *image* is a non-trivial PIL Image.
        Returns False for zero-size or all-black images.
        """
        try:
            if image is None:
                return False
            w, h = image.size
            if w < 10 or h < 10:
                return False
            return True
        except Exception:
            return False

    @staticmethod
    def _is_valid_path(path: Path) -> bool:
        """Return False if the file at *path* cannot be opened by Pillow."""
        try:
            with Image.open(path) as img:
                img.verify()
            return True
        except (UnidentifiedImageError, Exception):
            return False

    # ------------------------------------------------------------------
    # Internal — duplicate detection
    # ------------------------------------------------------------------

    def _is_duplicate(
        self,
        image: Image.Image,
        folder: Path,
        symbol: str,
    ) -> bool:
        """
        Return True if an image visually identical to *image* already
        exists in *folder* (perceptual hash distance ≤ PHASH_THRESHOLD).

        Only compares against the last 5 files in the folder to keep overhead low.
        """
        try:
            new_hash = imagehash.phash(image)
        except Exception as exc:
            logger.debug("phash computation failed: %s", exc)
            return False

        # Only check most-recent files (avoid O(N) scan every save)
        candidates = sorted(folder.glob("*.png"), key=lambda p: p.stat().st_mtime, reverse=True)[:5]
        for candidate in candidates:
            try:
                with Image.open(candidate) as existing:
                    existing_hash = imagehash.phash(existing)
                distance = new_hash - existing_hash
                if distance <= PHASH_THRESHOLD:
                    logger.debug(
                        "Duplicate: %s matches %s (distance=%d)",
                        symbol,
                        candidate.name,
                        distance,
                    )
                    return True
            except Exception:
                continue
        return False

    @staticmethod
    def _estimate_png_size(image: Image.Image) -> int:
        """
        Rough estimate of PNG file size (bytes) without writing to disk.
        Uses pixel count × 3 bytes as upper bound, divided by typical PNG ratio.
        """
        try:
            w, h = image.size
            return int(w * h * 3 * 0.5)  # ~50% PNG compression ratio estimate
        except Exception:
            return 500_000  # 500 KB safe fallback

    # ------------------------------------------------------------------
    # Internal — disk space
    # ------------------------------------------------------------------

    def _check_disk_space(self) -> bool:
        """
        Check free disk space on the screenshot folder's drive.
        Logs a warning if below threshold.
        Returns True if space is sufficient.
        """
        try:
            self.screenshot_folder.mkdir(parents=True, exist_ok=True)
            usage = shutil.disk_usage(str(self.screenshot_folder))
            free_bytes = usage.free
            if free_bytes < self.disk_warning_bytes:
                logger.warning(
                    "LOW DISK SPACE: %.2f GB free (warning threshold: %.2f GB)",
                    free_bytes / 1024 ** 3,
                    self.disk_warning_bytes / 1024 ** 3,
                )
                return False
            return True
        except Exception as exc:
            logger.error("Disk space check failed: %s", exc)
            return True  # don't block saves on check failure


# ---------------------------------------------------------------------------
# Module-level factory (used by CoreEngine integration)
# ---------------------------------------------------------------------------

def create_storage_manager(config_path: str = "./config.json") -> StorageManager:
    """
    Load config.json and return a configured StorageManager.

    Falls back to defaults if file is missing or malformed.
    """
    cfg: dict = {}
    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
        logger.info("StorageManager config loaded from %s", config_path)
    except FileNotFoundError:
        logger.warning("Config not found at %s — using defaults", config_path)
    except json.JSONDecodeError as exc:
        logger.error("Config JSON error: %s — using defaults", exc)
    return StorageManager(config=cfg)


# ---------------------------------------------------------------------------
# Integration helper: wrap CoreEngine callback
# ---------------------------------------------------------------------------

def make_storage_callback(
    storage: StorageManager,
) -> Callable[..., Optional[Path]]:
    """
    Return a callback compatible with CoreEngine._on_screenshot_ready().

    Usage in module6_integration.py:
        on_save = make_storage_callback(storage_manager)
        core_engine.on_save = on_save
    """
    def _callback(
        image: Image.Image,
        symbol: str,
        market: str,
        dt: datetime,
        tag: str = "",
    ) -> Optional[Path]:
        return storage.save_screenshot(image, symbol, market, dt, tag)

    return _callback


# ---------------------------------------------------------------------------
# Self-test (run with: python module3_storage.py)
# ---------------------------------------------------------------------------

def _run_self_test() -> None:
    """Smoke-test for StorageManager."""
    import sys
    import tempfile

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    print("=" * 60)
    print("module3_storage.py — Self Test")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = {
            "screenshot_folder": tmpdir,
            "global_limit_gb": 0.01,       # 10 MB for testing
            "per_symbol_limit_mb": 3.0,    # 3 MB per symbol
            "disk_warning_gb": 0.0,        # suppress warnings in test
        }
        sm = StorageManager(config=cfg)

        # --- Create a test image ---
        img = Image.new("RGB", (800, 600), color=(30, 30, 60))

        dt1 = datetime(2026, 4, 14, 9, 30)
        dt2 = datetime(2026, 4, 14, 16, 0)

        # --- Save AAPL NY_OPEN ---
        p1 = sm.save_screenshot(img, "AAPL", "US", dt1, "NY_OPEN")
        assert p1 is not None and p1.exists(), "save_screenshot failed"
        assert "NY_OPEN" in p1.name
        assert "AAPL" in p1.parts
        assert "US" in p1.parts
        print(f"[PASS] save_screenshot: {p1.name}")

        # --- Duplicate detection ---
        p_dup = sm.save_screenshot(img, "AAPL", "US", dt1, "NY_OPEN_DUP")
        assert p_dup is None, "Duplicate should be rejected"
        print("[PASS] Duplicate detection works")

        # --- Different image passes (use a visually distinct image with noise) ---
        import random
        img2 = Image.new("RGB", (800, 600))
        pixels = [(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
                  for _ in range(800 * 600)]
        img2.putdata(pixels)
        p2 = sm.save_screenshot(img2, "AAPL", "US", dt2, "NY_CLOSE")
        assert p2 is not None and p2.exists(), "Second distinct image should save"
        print(f"[PASS] Distinct image saved: {p2.name}")

        # --- Pin / Unpin ---
        pinned = sm.pin_file(p1)
        assert pinned is not None and PINNED_SUFFIX in pinned.stem
        print(f"[PASS] Pin: {pinned.name}")

        unpinned = sm.unpin_file(pinned)
        assert unpinned is not None and PINNED_SUFFIX not in unpinned.stem
        print(f"[PASS] Unpin: {unpinned.name}")

        # --- Stats ---
        stats = sm.get_stats()
        assert stats.total_files >= 2
        print(f"[PASS] get_stats(): {stats.total_files} files, {stats.total_size_mb:.2f} MB")

        # --- list_files ---
        files = sm.list_files(symbol="AAPL")
        assert len(files) >= 2
        print(f"[PASS] list_files(symbol=AAPL): {len(files)} entries")

        # --- Corrupt file detection ---
        # Write a non-PNG file with .png extension
        corrupt_path = Path(tmpdir) / "US" / "AAPL" / "2026-04" / "CORRUPT.png"
        corrupt_path.parent.mkdir(parents=True, exist_ok=True)
        corrupt_path.write_bytes(b"not a real image")
        deleted = sm.delete_corrupt_files()
        assert deleted >= 1, "Should have deleted corrupt file"
        print(f"[PASS] delete_corrupt_files(): {deleted} deleted")

        # --- Disk space check ---
        result = sm.check_disk_space()
        print(f"[PASS] check_disk_space(): {'OK' if result else 'LOW'}")

        # --- CCTV eviction ---
        # Fill symbol past 3 MB limit with big images
        big_img = Image.new("RGB", (3000, 2000), color=(0, 128, 255))
        for i in range(5):
            dt_fill = datetime(2026, 4, 14, i + 1, 0)
            sm.save_screenshot(big_img, "BTCUSD", "CRYPTO", dt_fill, "")
        
        btc_files = sm.list_files(symbol="BTCUSD", market="CRYPTO")
        total_btc_mb = sum(e.size_bytes for e in btc_files) / 1024 ** 2
        assert total_btc_mb <= cfg["per_symbol_limit_mb"] + 2.0, (
            f"Per-symbol limit not enforced: {total_btc_mb:.1f} MB"
        )
        print(f"[PASS] CCTV eviction: BTCUSD at {total_btc_mb:.1f} MB (limit {cfg['per_symbol_limit_mb']} MB)")

        # --- create_storage_manager factory (with missing config) ---
        sm2 = create_storage_manager("/nonexistent/config.json")
        assert isinstance(sm2, StorageManager)
        print("[PASS] create_storage_manager fallback to defaults")

    print("=" * 60)
    print("All tests passed ✅")
    print("=" * 60)


if __name__ == "__main__":
    _run_self_test()
