"""
module7_drive.py — Google Drive Auto-Assign & rclone Sync
TradingView Auto Screenshot System

Features:
- รองรับ 7-10 Google Drive accounts (10 GB/account)
- CRYPTO → Drive1 เสมอ
- US stocks → round-robin Drive2 ถึง DriveN
- respect limit_gb ต่อ Drive เด็ดขาด
- CCTV evict รูปเก่าออกก่อนถ้าใกล้ limit
- rclone sync ส่งไฟล์ขึ้น Google Drive
- Track drive usage via local state file

config.json ต้องมี:
{
  "drives": [
    {"name": "Drive1", "rclone_remote": "gdrive1:", "limit_gb": 9.5, "label": "CRYPTO"},
    {"name": "Drive2", "rclone_remote": "gdrive2:", "limit_gb": 9.5, "label": "US-A"},
    ...
  ]
}

GitHub Secrets ที่ต้องมี:
- RCLONE_CONF  ← rclone config file content (base64 หรือ raw)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_DRIVE_LIMIT_GB: float = 9.5       # safe limit per drive (actual 10 GB)
USAGE_CACHE_TTL_SEC: int = 300            # refresh drive usage every 5 min
RCLONE_TIMEOUT_SEC: int = 120            # max seconds per rclone call
STATE_FILE: str = ".drive_state.json"    # local state: round-robin index + usage cache
EVICT_HEADROOM_GB: float = 0.2           # trigger eviction when < 200 MB free


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DriveConfig:
    """One Google Drive account configuration."""

    name: str                  # e.g. "Drive1"
    rclone_remote: str         # e.g. "gdrive1:"
    limit_gb: float            # storage limit in GB
    label: str = ""            # human label e.g. "CRYPTO", "US-A"
    used_bytes: int = 0        # cached usage (refreshed periodically)
    last_usage_check: float = 0.0  # Unix timestamp of last rclone check

    @property
    def limit_bytes(self) -> int:
        return int(self.limit_gb * 1024 ** 3)

    @property
    def free_bytes(self) -> int:
        return max(0, self.limit_bytes - self.used_bytes)

    @property
    def free_gb(self) -> float:
        return self.free_bytes / (1024 ** 3)

    @property
    def used_gb(self) -> float:
        return self.used_bytes / (1024 ** 3)

    @property
    def is_full(self) -> bool:
        return self.free_gb < EVICT_HEADROOM_GB


@dataclass
class DriveAssignment:
    """Result of assign_drive() — which drive a stock is assigned to."""

    symbol: str
    market: str
    drive: DriveConfig
    drive_index: int  # 0-based index in drives list


@dataclass
class SyncResult:
    """Result of a rclone sync operation."""

    success: bool
    drive_name: str
    remote_path: str
    local_path: str
    duration_sec: float = 0.0
    error: str = ""
    files_transferred: int = 0
    drive_public_url: str = ""   # public viewable URL after set_public_link()


# ---------------------------------------------------------------------------
# DriveManager
# ---------------------------------------------------------------------------

class DriveManager:
    """
    Manages Google Drive accounts for screenshot storage.

    Thread-safe.  Uses rclone for all Drive operations.

    Usage:
        dm = DriveManager(config)
        assignment = dm.assign_drive("AAPL", "US")
        result = dm.sync_file(local_path, assignment)
    """

    def __init__(self, config: Optional[dict] = None) -> None:
        cfg = config or {}
        self._lock = threading.RLock()

        # Load drives from config
        self._drives: List[DriveConfig] = self._parse_drives(cfg)
        if not self._drives:
            logger.warning(
                "DriveManager: no drives configured — Drive sync disabled. "
                "Add 'drives' array to config.json"
            )

        # State: round-robin index for US stocks (Drive2 onwards)
        self._us_rr_index: int = 0        # index into self._drives[1:]
        # State: per-symbol → drive index (persistent)
        self._symbol_drive_map: Dict[str, int] = {}

        # rclone binary path
        self._rclone: str = cfg.get("rclone_path", "rclone")

        # State file path
        state_dir = Path(cfg.get("log_folder", "./logs"))
        state_dir.mkdir(parents=True, exist_ok=True)
        self._state_file: Path = state_dir / STATE_FILE

        self._load_state()

        logger.info(
            "DriveManager init: %d drives, rclone=%s",
            len(self._drives),
            self._rclone,
        )
        for i, d in enumerate(self._drives):
            logger.info(
                "  Drive%d: %s remote=%s limit=%.1f GB label=%s",
                i + 1, d.name, d.rclone_remote, d.limit_gb, d.label,
            )

    # ------------------------------------------------------------------
    # Public API — assignment
    # ------------------------------------------------------------------

    def assign_drive(self, symbol: str, market: str) -> Optional[DriveAssignment]:
        """
        Return the DriveConfig assigned to this symbol/market.

        Rules:
        - CRYPTO → Drive1 (index 0) always
        - US     → round-robin Drive2 … DriveN (sticky per symbol)

        Returns None if no drives configured or all drives are full.
        """
        with self._lock:
            if not self._drives:
                return None

            market_up = market.upper()

            # CRYPTO always → Drive1
            if market_up == "CRYPTO":
                drive = self._drives[0]
                idx = 0
                return DriveAssignment(
                    symbol=symbol,
                    market=market,
                    drive=drive,
                    drive_index=idx,
                )

            # US → sticky assignment (remember symbol → drive)
            if symbol in self._symbol_drive_map:
                idx = self._symbol_drive_map[symbol]
                # Validate stored index still valid
                if 0 <= idx < len(self._drives):
                    drive = self._drives[idx]
                    if not drive.is_full:
                        return DriveAssignment(
                            symbol=symbol,
                            market=market,
                            drive=drive,
                            drive_index=idx,
                        )
                    logger.warning(
                        "Assigned drive %s is full for %s — reassigning",
                        drive.name, symbol,
                    )

            # Assign via round-robin among Drive2 … DriveN
            # (indices 1 … len-1)
            us_drives = self._drives[1:] if len(self._drives) > 1 else self._drives
            if not us_drives:
                us_drives = self._drives  # fallback: use Drive1

            # Try round-robin until we find a non-full drive
            attempts = len(us_drives)
            for _ in range(attempts):
                rr = self._us_rr_index % len(us_drives)
                self._us_rr_index = (rr + 1) % len(us_drives)
                candidate = us_drives[rr]
                abs_idx = self._drives.index(candidate)

                if not candidate.is_full:
                    self._symbol_drive_map[symbol] = abs_idx
                    self._save_state()
                    logger.info(
                        "Assigned %s (%s) → %s",
                        symbol, market, candidate.name,
                    )
                    return DriveAssignment(
                        symbol=symbol,
                        market=market,
                        drive=candidate,
                        drive_index=abs_idx,
                    )

            logger.error(
                "All drives are full — cannot assign %s (%s)", symbol, market
            )
            return None

    def get_drive_for_symbol(self, symbol: str) -> Optional[DriveConfig]:
        """Return cached DriveConfig for a symbol, or None if not assigned."""
        with self._lock:
            idx = self._symbol_drive_map.get(symbol)
            if idx is not None and 0 <= idx < len(self._drives):
                return self._drives[idx]
            return None

    # ------------------------------------------------------------------
    # Public API — sync
    # ------------------------------------------------------------------

    def sync_file(
        self,
        local_path: Path,
        assignment: DriveAssignment,
        remote_subfolder: str = "",
    ) -> SyncResult:
        """
        Upload a single file to the assigned Google Drive via rclone copyto.

        Parameters
        ----------
        local_path      : path to local file
        assignment      : DriveAssignment from assign_drive()
        remote_subfolder: subfolder on Drive (e.g. "screenshots/CRYPTO/BTC/2026-04")

        Returns SyncResult.
        """
        drive = assignment.drive
        local_path = Path(local_path)

        if not local_path.exists():
            return SyncResult(
                success=False,
                drive_name=drive.name,
                remote_path="",
                local_path=str(local_path),
                error=f"Local file not found: {local_path}",
            )

        # Build remote path: remote:subfolder/filename
        if remote_subfolder:
            remote = f"{drive.rclone_remote}{remote_subfolder}/{local_path.name}"
        else:
            remote = f"{drive.rclone_remote}{local_path.name}"

        start = time.monotonic()
        try:
            result = self._rclone_run(
                ["copyto", str(local_path), remote, "--progress=false"]
            )
            duration = time.monotonic() - start

            if result.returncode == 0:
                # Update usage estimate
                with self._lock:
                    drive.used_bytes += local_path.stat().st_size
                logger.info(
                    "Drive sync OK: %s → %s (%.1fs)",
                    local_path.name, remote, duration,
                )
                # Get public link for gallery
                public_url = self.get_public_link(remote)
                return SyncResult(
                    success=True,
                    drive_name=drive.name,
                    remote_path=remote,
                    local_path=str(local_path),
                    duration_sec=duration,
                    files_transferred=1,
                    drive_public_url=public_url,
                )
            else:
                err = result.stderr.strip() or result.stdout.strip()
                logger.error(
                    "rclone copyto failed [rc=%d]: %s → %s\n  %s",
                    result.returncode, local_path.name, remote, err,
                )
                return SyncResult(
                    success=False,
                    drive_name=drive.name,
                    remote_path=remote,
                    local_path=str(local_path),
                    duration_sec=duration,
                    error=err,
                )

        except Exception as exc:
            duration = time.monotonic() - start
            logger.error("sync_file exception: %s", exc, exc_info=True)
            return SyncResult(
                success=False,
                drive_name=drive.name,
                remote_path=remote,
                local_path=str(local_path),
                duration_sec=duration,
                error=str(exc),
            )

    def sync_folder(
        self,
        local_folder: Path,
        assignment: DriveAssignment,
        remote_subfolder: str = "screenshots",
    ) -> SyncResult:
        """
        Sync an entire local folder to Google Drive via rclone sync.

        Uses --delete-after to handle CCTV evictions.

        Parameters
        ----------
        local_folder    : root local screenshots folder
        assignment      : DriveAssignment from assign_drive()
        remote_subfolder: top-level folder name on Drive
        """
        drive = assignment.drive
        local_folder = Path(local_folder)

        if not local_folder.exists():
            return SyncResult(
                success=False,
                drive_name=drive.name,
                remote_path="",
                local_path=str(local_folder),
                error=f"Local folder not found: {local_folder}",
            )

        remote = f"{drive.rclone_remote}{remote_subfolder}"
        start = time.monotonic()

        try:
            result = self._rclone_run([
                "sync",
                str(local_folder),
                remote,
                "--delete-after",
                "--progress=false",
                "--stats=0",
            ])
            duration = time.monotonic() - start

            if result.returncode == 0:
                logger.info(
                    "Folder sync OK: %s → %s (%.1fs)",
                    local_folder, remote, duration,
                )
                # Refresh usage after full sync
                self.refresh_usage(assignment.drive_index)
                return SyncResult(
                    success=True,
                    drive_name=drive.name,
                    remote_path=remote,
                    local_path=str(local_folder),
                    duration_sec=duration,
                )
            else:
                err = result.stderr.strip() or result.stdout.strip()
                logger.error(
                    "rclone sync failed [rc=%d]: %s → %s\n  %s",
                    result.returncode, local_folder, remote, err,
                )
                return SyncResult(
                    success=False,
                    drive_name=drive.name,
                    remote_path=remote,
                    local_path=str(local_folder),
                    duration_sec=duration,
                    error=err,
                )

        except Exception as exc:
            duration = time.monotonic() - start
            logger.error("sync_folder exception: %s", exc, exc_info=True)
            return SyncResult(
                success=False,
                drive_name=drive.name,
                remote_path=remote,
                local_path=str(local_folder),
                duration_sec=duration,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Public API — usage / eviction
    # ------------------------------------------------------------------

    def refresh_usage(self, drive_index: Optional[int] = None) -> None:
        """
        Query rclone about (used) bytes on Drive and update cache.

        Parameters
        ----------
        drive_index : int or None — refresh specific drive, or all if None
        """
        with self._lock:
            targets = (
                [self._drives[drive_index]]
                if drive_index is not None and 0 <= drive_index < len(self._drives)
                else self._drives
            )

        for drive in targets:
            now = time.time()
            with self._lock:
                if now - drive.last_usage_check < USAGE_CACHE_TTL_SEC:
                    continue  # cache still fresh

            try:
                result = self._rclone_run([
                    "about", drive.rclone_remote, "--json",
                ])
                if result.returncode == 0 and result.stdout.strip():
                    data = json.loads(result.stdout)
                    used = data.get("used", 0)
                    with self._lock:
                        drive.used_bytes = int(used)
                        drive.last_usage_check = time.time()
                    logger.info(
                        "Drive usage refreshed: %s = %.2f GB / %.1f GB",
                        drive.name,
                        drive.used_gb,
                        drive.limit_gb,
                    )
                else:
                    logger.warning(
                        "rclone about %s failed [rc=%d]: %s",
                        drive.rclone_remote,
                        result.returncode,
                        result.stderr.strip(),
                    )
            except Exception as exc:
                logger.warning("refresh_usage(%s) error: %s", drive.name, exc)

    def evict_old_files_remote(
        self,
        drive: DriveConfig,
        remote_folder: str,
        headroom_gb: float = EVICT_HEADROOM_GB,
    ) -> int:
        """
        Delete oldest files on Drive until headroom_gb is free.

        Lists files sorted by modification time (oldest first) and
        deletes via rclone deletefile until enough space is freed.

        Returns number of files deleted.
        """
        if drive.free_gb >= headroom_gb:
            return 0  # nothing to evict

        logger.warning(
            "Drive %s low on space (%.2f GB free) — evicting old files",
            drive.name, drive.free_gb,
        )

        deleted = 0
        try:
            # List all files with mod time (JSON format per file)
            result = self._rclone_run([
                "lsjson",
                f"{drive.rclone_remote}{remote_folder}",
                "--recursive",
                "--files-only",
            ])
            if result.returncode != 0 or not result.stdout.strip():
                logger.error("evict_old_files_remote: lsjson failed")
                return 0

            files = json.loads(result.stdout)
            # Sort oldest first by ModTime
            files.sort(key=lambda x: x.get("ModTime", ""))

            for f in files:
                if drive.free_gb >= headroom_gb:
                    break
                remote_path = f"{drive.rclone_remote}{remote_folder}/{f['Path']}"
                del_result = self._rclone_run(["deletefile", remote_path])
                if del_result.returncode == 0:
                    size = f.get("Size", 0)
                    with self._lock:
                        drive.used_bytes = max(0, drive.used_bytes - int(size))
                    deleted += 1
                    logger.info(
                        "Evicted: %s (%.1f KB freed)", f["Path"], int(size) / 1024
                    )
                else:
                    logger.warning(
                        "Failed to delete remote %s: %s",
                        remote_path, del_result.stderr.strip(),
                    )
        except Exception as exc:
            logger.error("evict_old_files_remote error: %s", exc, exc_info=True)

        logger.info("Eviction done: %d files deleted from %s", deleted, drive.name)
        return deleted

    def get_all_assignments(self) -> Dict[str, int]:
        """Return a copy of symbol → drive_index map."""
        with self._lock:
            return dict(self._symbol_drive_map)

    def get_drive_label(self, symbol: str) -> str:
        """
        Return the 'DriveN' label string for a symbol.

        Example: symbol assigned to drives[1] → "Drive2"
        Returns "" if not assigned.
        """
        with self._lock:
            idx = self._symbol_drive_map.get(symbol)
            if idx is None:
                # CRYPTO always on Drive1
                return "Drive1"
            return f"Drive{idx + 1}"

    def get_drive_info(self) -> List[dict]:
        """Return list of drive info dicts for status display."""
        with self._lock:
            return [
                {
                    "name": d.name,
                    "remote": d.rclone_remote,
                    "label": d.label,
                    "used_gb": round(d.used_gb, 2),
                    "limit_gb": d.limit_gb,
                    "free_gb": round(d.free_gb, 2),
                    "is_full": d.is_full,
                }
                for d in self._drives
            ]

    # ------------------------------------------------------------------
    # rclone setup helpers
    # ------------------------------------------------------------------

    @staticmethod
    def setup_rclone_from_env() -> bool:
        """
        Write rclone config from RCLONE_CONF environment variable.

        GitHub Actions injects this secret as env var.
        Writes to ~/.config/rclone/rclone.conf

        Returns True on success.
        """
        rclone_conf = os.environ.get("RCLONE_CONF", "")
        if not rclone_conf:
            logger.warning(
                "RCLONE_CONF env var not set — Drive sync will not work. "
                "Set RCLONE_CONF GitHub Secret."
            )
            return False

        conf_dir = Path.home() / ".config" / "rclone"
        conf_dir.mkdir(parents=True, exist_ok=True)
        conf_file = conf_dir / "rclone.conf"

        try:
            # Handle base64-encoded config
            import base64
            try:
                decoded = base64.b64decode(rclone_conf).decode("utf-8")
                conf_file.write_text(decoded, encoding="utf-8")
                logger.info("rclone.conf written from base64 RCLONE_CONF (%d bytes)", len(decoded))
            except Exception:
                # Not base64 — write raw
                conf_file.write_text(rclone_conf, encoding="utf-8")
                logger.info("rclone.conf written from raw RCLONE_CONF (%d bytes)", len(rclone_conf))

            conf_file.chmod(0o600)
            return True

        except Exception as exc:
            logger.error("Failed to write rclone.conf: %s", exc)
            return False

    @staticmethod
    def check_rclone_available(rclone_path: str = "rclone") -> bool:
        """Return True if rclone binary is available and executable."""
        try:
            result = subprocess.run(
                [rclone_path, "version"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                version_line = result.stdout.split("\n")[0]
                logger.info("rclone available: %s", version_line)
                return True
            return False
        except (FileNotFoundError, subprocess.TimeoutExpired, Exception) as exc:
            logger.warning("rclone not available: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Internal — rclone runner
    # ------------------------------------------------------------------

    def _rclone_run(self, args: List[str]) -> subprocess.CompletedProcess:
        """Run rclone with given args. Returns CompletedProcess."""
        cmd = [self._rclone] + args
        logger.debug("rclone: %s", " ".join(cmd))
        try:
            return subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=RCLONE_TIMEOUT_SEC,
            )
        except subprocess.TimeoutExpired:
            logger.error("rclone timeout after %ds: %s", RCLONE_TIMEOUT_SEC, " ".join(cmd))
            # Return fake failed result
            proc = subprocess.CompletedProcess(cmd, 1)
            proc.stdout = ""
            proc.stderr = f"Timeout after {RCLONE_TIMEOUT_SEC}s"
            return proc

    # ------------------------------------------------------------------
    # Public API — Drive public link
    # ------------------------------------------------------------------

    def get_public_link(self, remote_path: str) -> str:
        """
        Make a Drive file publicly readable and return its direct image URL.

        Uses rclone link to get a shareable URL, then converts to direct
        download format for use in <img src="">.

        Parameters
        ----------
        remote_path : full rclone remote path e.g. "gdrive1:screenshots/US/AAPL/2026-04/AAPL_2026-04-19_13-35.png"

        Returns direct URL string, or "" on failure.
        """
        try:
            result = self._rclone_run([
                "link",
                remote_path,
            ])
            if result.returncode != 0 or not result.stdout.strip():
                logger.warning(
                    "rclone link failed for %s [rc=%d]: %s",
                    remote_path, result.returncode, result.stderr.strip(),
                )
                return ""

            share_url = result.stdout.strip()
            # Parse file ID from any Google Drive URL format:
            # https://drive.google.com/file/d/FILE_ID/view?usp=sharing
            # https://drive.google.com/open?id=FILE_ID
            # https://drive.google.com/uc?id=FILE_ID
            import re as _re
            file_id = ""
            m = _re.search(r"/d/([a-zA-Z0-9_-]+)", share_url)
            if m:
                file_id = m.group(1)
            else:
                m2 = _re.search(r"[?&]id=([a-zA-Z0-9_-]+)", share_url)
                if m2:
                    file_id = m2.group(1)

            if file_id:
                direct_url = f"https://drive.google.com/uc?export=view&id={file_id}"
                logger.debug("Public link: %s -> %s", remote_path, direct_url)
                return direct_url

            logger.warning("Could not parse file ID from: %s", share_url)
            return share_url

        except Exception as exc:
            logger.warning("get_public_link error for %s: %s", remote_path, exc)
            return ""

    # ------------------------------------------------------------------
    # Internal — config parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_drives(cfg: dict) -> List[DriveConfig]:
        """
        Parse 'drives' array from config dict.

        Expected format:
        [
          {"name": "Drive1", "rclone_remote": "gdrive1:", "limit_gb": 9.5, "label": "CRYPTO"},
          {"name": "Drive2", "rclone_remote": "gdrive2:", "limit_gb": 9.5, "label": "US-A"},
          ...
        ]
        """
        raw: list = cfg.get("drives", [])
        if not isinstance(raw, list):
            logger.error("config 'drives' must be a list — got %s", type(raw).__name__)
            return []

        drives: List[DriveConfig] = []
        for i, item in enumerate(raw):
            if not isinstance(item, dict):
                logger.warning("drives[%d]: expected dict, got %s — skipped", i, type(item).__name__)
                continue
            name = item.get("name", f"Drive{i + 1}")
            remote = item.get("rclone_remote", "")
            if not remote:
                logger.warning("drives[%d] '%s': missing rclone_remote — skipped", i, name)
                continue
            # Ensure remote ends with ':'
            if not remote.endswith(":") and "/" not in remote:
                remote = remote + ":"
            limit_gb = float(item.get("limit_gb", DEFAULT_DRIVE_LIMIT_GB))
            label = str(item.get("label", ""))
            drives.append(DriveConfig(
                name=name,
                rclone_remote=remote,
                limit_gb=limit_gb,
                label=label,
            ))

        return drives

    # ------------------------------------------------------------------
    # Internal — state persistence
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        """Load round-robin index and symbol→drive map from state file."""
        if not self._state_file.exists():
            return
        try:
            data = json.loads(self._state_file.read_text(encoding="utf-8"))
            self._us_rr_index = int(data.get("us_rr_index", 0))
            raw_map = data.get("symbol_drive_map", {})
            if isinstance(raw_map, dict):
                # Convert keys/values to correct types
                self._symbol_drive_map = {
                    str(k): int(v) for k, v in raw_map.items()
                }
            logger.debug(
                "Drive state loaded: rr_index=%d, %d symbols mapped",
                self._us_rr_index, len(self._symbol_drive_map),
            )
        except Exception as exc:
            logger.warning("Failed to load drive state: %s — starting fresh", exc)

    def _save_state(self) -> None:
        """Persist round-robin index and symbol→drive map to state file."""
        try:
            data = {
                "us_rr_index": self._us_rr_index,
                "symbol_drive_map": self._symbol_drive_map,
                "updated_at": datetime.now().isoformat(),
            }
            self._state_file.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("Failed to save drive state: %s", exc)


# ---------------------------------------------------------------------------
# Convenience: build remote path matching local screenshot folder structure
# ---------------------------------------------------------------------------

def build_remote_path(
    symbol: str,
    market: str,
    dt: datetime,
    base_remote_folder: str = "screenshots",
) -> str:
    """
    Build the remote subfolder path for rclone upload.

    Mirrors local structure: screenshots/MARKET/SYMBOL/YYYY-MM/

    Parameters
    ----------
    symbol          : e.g. "AAPL"
    market          : e.g. "US" or "CRYPTO"
    dt              : datetime of the screenshot
    base_remote_folder : root folder name on Drive

    Returns e.g. "screenshots/US/AAPL/2026-04"
    """
    month = dt.strftime("%Y-%m")
    return f"{base_remote_folder}/{market.upper()}/{symbol}/{month}"


# ---------------------------------------------------------------------------
# Module-level factory
# ---------------------------------------------------------------------------

def create_drive_manager(config_path: str = "./config.json") -> DriveManager:
    """
    Load config.json and return a configured DriveManager.

    Also sets up rclone from RCLONE_CONF env var if present.
    Falls back to defaults if file is missing or malformed.
    """
    cfg: dict = {}
    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
        logger.info("DriveManager config loaded from %s", config_path)
    except FileNotFoundError:
        logger.warning("Config not found at %s — using defaults", config_path)
    except json.JSONDecodeError as exc:
        logger.error("Config JSON error: %s — using defaults", exc)

    # Setup rclone from env if RCLONE_CONF is set
    if os.environ.get("RCLONE_CONF"):
        DriveManager.setup_rclone_from_env()

    return DriveManager(config=cfg)


# ---------------------------------------------------------------------------
# Self-test (run with: python module7_drive.py)
# ---------------------------------------------------------------------------

def _run_self_test() -> None:
    """Smoke-test for DriveManager (no real Drive access required)."""
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    print("=" * 60)
    print("module7_drive.py — Self Test (no real Drive access)")
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

    # ── Config parsing ──────────────────────────────────────────────
    print("\n--- Config parsing ---")

    cfg = {
        "drives": [
            {"name": "Drive1", "rclone_remote": "gdrive1:", "limit_gb": 9.5, "label": "CRYPTO"},
            {"name": "Drive2", "rclone_remote": "gdrive2:", "limit_gb": 9.5, "label": "US-A"},
            {"name": "Drive3", "rclone_remote": "gdrive3:", "limit_gb": 9.5, "label": "US-B"},
        ],
        "log_folder": "/tmp/tv_test_logs",
    }

    dm = DriveManager(config=cfg)
    check("3 drives parsed", len(dm._drives) == 3)
    check("Drive1 remote = gdrive1:", dm._drives[0].rclone_remote == "gdrive1:")
    check("Drive1 label = CRYPTO", dm._drives[0].label == "CRYPTO")
    check("Drive2 label = US-A", dm._drives[1].label == "US-A")
    check("limit_bytes correct", dm._drives[0].limit_bytes == int(9.5 * 1024 ** 3))

    # ── Drive assignment ─────────────────────────────────────────────
    print("\n--- Drive assignment ---")

    a_crypto = dm.assign_drive("BTCTHB", "CRYPTO")
    check("CRYPTO → Drive1", a_crypto is not None and a_crypto.drive_index == 0)
    check("CRYPTO drive name = Drive1", a_crypto is not None and a_crypto.drive.name == "Drive1")

    a_us1 = dm.assign_drive("AAPL", "US")
    check("US AAPL assigned", a_us1 is not None)
    check("US AAPL → Drive2 or Drive3 (not Drive1)", a_us1 is not None and a_us1.drive_index >= 1)

    a_us1_again = dm.assign_drive("AAPL", "US")
    check("AAPL sticky (same drive)", a_us1 is not None and a_us1_again is not None and a_us1.drive_index == a_us1_again.drive_index)

    a_us2 = dm.assign_drive("TSLA", "US")
    check("TSLA assigned", a_us2 is not None)

    # ── build_remote_path ────────────────────────────────────────────
    print("\n--- build_remote_path ---")
    dt = datetime(2026, 4, 14, 9, 30)
    remote = build_remote_path("AAPL", "US", dt)
    check("Remote path format correct", remote == "screenshots/US/AAPL/2026-04")

    remote_crypto = build_remote_path("BTCTHB", "CRYPTO", dt)
    check("CRYPTO remote path correct", remote_crypto == "screenshots/CRYPTO/BTCTHB/2026-04")

    # ── DriveConfig properties ───────────────────────────────────────
    print("\n--- DriveConfig properties ---")
    d = DriveConfig(name="Test", rclone_remote="gdrive_test:", limit_gb=9.5, label="TEST")
    d.used_bytes = int(9.3 * 1024 ** 3)
    check("free_gb < headroom → is_full", d.is_full)
    d.used_bytes = int(5.0 * 1024 ** 3)
    check("5 GB used → not full", not d.is_full)

    # ── No drives configured ─────────────────────────────────────────
    print("\n--- No drives configured ---")
    dm_empty = DriveManager(config={})
    a_none = dm_empty.assign_drive("AAPL", "US")
    check("No drives → assign returns None", a_none is None)

    # ── get_drive_label ──────────────────────────────────────────────
    print("\n--- get_drive_label ---")
    label = dm.get_drive_label("BTCTHB")
    check("BTCTHB drive label = Drive1 (default for unassigned CRYPTO)", "Drive" in label)

    # ── rclone availability ──────────────────────────────────────────
    print("\n--- rclone availability ---")
    available = DriveManager.check_rclone_available()
    print(f"  rclone available: {available} (OK if False in test env)")
    check("check_rclone_available returns bool", isinstance(available, bool))

    # ── State persistence ────────────────────────────────────────────
    print("\n--- State persistence ---")
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg2 = {
            "drives": [
                {"name": "Drive1", "rclone_remote": "gdrive1:", "limit_gb": 9.5, "label": "CRYPTO"},
                {"name": "Drive2", "rclone_remote": "gdrive2:", "limit_gb": 9.5, "label": "US-A"},
            ],
            "log_folder": tmpdir,
        }
        dm2 = DriveManager(config=cfg2)
        dm2.assign_drive("AAPL", "US")
        dm2._save_state()
        dm3 = DriveManager(config=cfg2)
        check("State persisted and reloaded", "AAPL" in dm3._symbol_drive_map)

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
