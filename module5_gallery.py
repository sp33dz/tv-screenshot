"""
module5_gallery.py — HTML Gallery & Chart Replay
TradingView Auto Screenshot System

Features:
- HTML Gallery: browse screenshots by symbol / market / date / drive
- Chart Replay: Play / Pause / Speed control (0.5x – 16x)
- Step ← → ทีละภาพ
- Jump to date
- Filter by Symbol / Market / Drive / Session tag
- Drive filter dropdown (Drive1…DriveN)
- Note / Annotation บนภาพ (saved to JSON sidecar)
- Mark trade feature (Bull / Bear / Note)
- Export เป็น Video (.mp4) ด้วย OpenCV
- GitHub Pages URL support (absPath resolves to Pages URL when configured)

Outputs:
- gallery/index.html         ← main gallery page
- gallery/replay.html        ← chart replay player
- gallery/data.json          ← image index + metadata
- gallery/notes.json         ← user annotations
- gallery/exports/*.mp4      ← video exports

Integration:
    from module5_gallery import GalleryManager
    gm = GalleryManager(config=cfg)
    gm.build()                     # scan screenshots → generate HTML
    gm.export_video("BTCUSD")      # export symbol as mp4

config.json keys used:
    screenshot_folder   : path to screenshots root
    gallery_folder      : path to gallery output folder
    video_fps           : frames per second for mp4 export
    github_pages_url    : e.g. "https://user.github.io/repo" (optional)
                          When set, gallery uses GitHub Pages URLs for images
    drives              : [{name, rclone_remote, limit_gb, label}] (optional)
                          When set, gallery shows Drive filter dropdown
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_SCREENSHOT_FOLDER: str = "./screenshots"
DEFAULT_GALLERY_FOLDER: str = "./gallery"
DEFAULT_VIDEO_FPS: int = 2
VIDEO_CODEC: str = "mp4v"
NOTES_FILE: str = "notes.json"
DATA_FILE: str = "data.json"
GALLERY_HTML: str = "index.html"
REPLAY_HTML: str = "replay.html"

# Filename pattern: SYMBOL_YYYY-MM-DD_HH-MM[_TAG][.PINNED].png
_FNAME_RE = re.compile(
    r"^(?P<symbol>[A-Z0-9]+)_"
    r"(?P<date>\d{4}-\d{2}-\d{2})_"
    r"(?P<time>\d{2}-\d{2})"
    r"(?:_(?P<tag>[A-Z_]+))?"
    r"(?:\.PINNED)?\.png$",
    re.IGNORECASE,
)

SESSION_TAGS: Tuple[str, ...] = ("NY_OPEN", "NY_CLOSE", "CRYPTO_FUNDING")

TRADE_MARKS: Tuple[str, ...] = ("BULL", "BEAR", "NOTE", "")


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ImageEntry:
    """Metadata for a single screenshot file."""

    path: str             # relative to screenshot_folder
    abs_path: str         # absolute path
    symbol: str
    market: str
    date: str             # YYYY-MM-DD
    time: str             # HH-MM
    tag: str              # event tag or ""
    is_pinned: bool
    size_bytes: int
    month: str            # YYYY-MM
    drive_name: str = ""  # "Drive1" … "DriveN" from module7 assignment (optional)
    drive_public_url: str = ""  # Google Drive public direct URL for <img src="">, "" = use github pages

    @property
    def datetime_str(self) -> str:
        return f"{self.date} {self.time.replace('-', ':')}"

    @property
    def sort_key(self) -> str:
        return f"{self.date}_{self.time}"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["datetime_str"] = self.datetime_str
        return d


@dataclass
class Annotation:
    """User annotation / note on a screenshot."""

    path: str             # relative path (key)
    note: str = ""
    trade_mark: str = ""  # "BULL" | "BEAR" | "NOTE" | ""
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class GalleryIndex:
    """Full gallery index used to generate HTML and data.json."""

    entries: List[ImageEntry] = field(default_factory=list)
    symbols: List[str] = field(default_factory=list)
    markets: List[str] = field(default_factory=list)
    months: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    drives: List[str] = field(default_factory=list)   # ["Drive1", "Drive2", …]
    total_files: int = 0
    generated_at: str = ""

    def to_dict(self) -> dict:
        return {
            "entries": [e.to_dict() for e in self.entries],
            "symbols": self.symbols,
            "markets": self.markets,
            "months": self.months,
            "tags": self.tags,
            "drives": self.drives,
            "total_files": self.total_files,
            "generated_at": self.generated_at,
        }


# ---------------------------------------------------------------------------
# GalleryManager — main class
# ---------------------------------------------------------------------------

class GalleryManager:
    """
    Manages HTML gallery generation and chart replay for TradingView screenshots.

    Thread-safe build() and export_video() operations.
    """

    def __init__(self, config: Optional[dict] = None) -> None:
        """
        Initialise from config dict.

        Parameters
        ----------
        config : dict, optional
            Keys used:
              screenshot_folder  : root folder for screenshots
              gallery_folder     : output folder for HTML gallery
              video_fps          : frames/second for mp4 export
              github_pages_url   : base URL for GitHub Pages (e.g.
                                   "https://user.github.io/repo").
                                   When set, image paths in HTML are built as
                                   {github_pages_url}/screenshots/{rel_path}.
                                   When empty/absent, falls back to relative
                                   path "../screenshots/{rel}" (local server)
                                   or "file:///" (file:// protocol).
              drives             : list of drive config dicts from module7
                                   [{name, rclone_remote, limit_gb, label}]
                                   When present, gallery shows Drive filter.
        """
        cfg = config or {}
        self.screenshot_folder = Path(cfg.get("screenshot_folder", DEFAULT_SCREENSHOT_FOLDER))
        self.gallery_folder = Path(cfg.get("gallery_folder", DEFAULT_GALLERY_FOLDER))
        self.video_fps: int = int(cfg.get("video_fps", DEFAULT_VIDEO_FPS))

        # GitHub Pages base URL — stripped of trailing slash
        raw_url: str = str(cfg.get("github_pages_url", "") or "").strip().rstrip("/")
        self.github_pages_url: str = raw_url  # "" means disabled

        # Drive names list from config (for filter dropdown)
        raw_drives: list = cfg.get("drives", []) or []
        self.drive_names: List[str] = [
            str(d.get("name", "")).strip()
            for d in raw_drives
            if isinstance(d, dict) and d.get("name")
        ]

        # Gallery password hash — read from env GALLERY_PASSWORD_HASH (SHA-256 hex)
        # Set by workflow from GALLERY_PASSWORD secret
        import os as _os, hashlib as _hl
        raw_pw = _os.environ.get("GALLERY_PASSWORD", "").strip()
        env_hash = _os.environ.get("GALLERY_PASSWORD_HASH", "").strip()
        if env_hash:
            self.password_hash: str = env_hash
        elif raw_pw:
            self.password_hash = _hl.sha256(raw_pw.encode()).hexdigest()
        else:
            self.password_hash = ""  # no password = open access

        self._lock = threading.RLock()

        self.gallery_folder.mkdir(parents=True, exist_ok=True)
        (self.gallery_folder / "exports").mkdir(exist_ok=True)

        logger.info(
            "GalleryManager init: screenshots=%s, gallery=%s, github_pages_url=%r, drives=%s",
            self.screenshot_folder,
            self.gallery_folder,
            self.github_pages_url or "(local)",
            self.drive_names or "(none)",
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self) -> GalleryIndex:
        """
        Scan screenshot folder, generate data.json + HTML gallery + replay page.

        Merges newly-scanned entries with historical entries from the existing
        data.json so that runs on ephemeral environments (e.g. GitHub Actions)
        accumulate all screenshots across runs rather than overwriting with
        only the current run's files.

        Returns
        -------
        GalleryIndex with all discovered screenshots (current + historical).
        """
        with self._lock:
            logger.info("Building gallery from %s …", self.screenshot_folder)
            # Ensure gallery folder exists (may have been deleted after init)
            self.gallery_folder.mkdir(parents=True, exist_ok=True)
            (self.gallery_folder / "exports").mkdir(exist_ok=True)
            index = self._scan_screenshots()
            index = self._merge_historical(index)
            ok_data = self._write_data_json(index)
            notes = self._load_notes()
            ok_html = self._generate_gallery_html(index, notes)
            ok_replay = self._generate_replay_html(index, notes)
            if not (ok_data and ok_html and ok_replay):
                logger.warning(
                    "Gallery build incomplete — %d/%d files written",
                    sum([ok_data, ok_html, ok_replay]), 3,
                )
            logger.info(
                "Gallery built: %d images, %d symbols → %s",
                index.total_files,
                len(index.symbols),
                self.gallery_folder / GALLERY_HTML,
            )
            return index

    def open_gallery(self) -> None:
        """Open gallery/index.html in the default browser."""
        path = self.gallery_folder / GALLERY_HTML
        if not path.exists():
            logger.warning("Gallery HTML not found — run build() first.")
            return
        try:
            import sys as _sys
            if _sys.platform == "win32":
                os.startfile(str(path))
            elif _sys.platform == "darwin":
                import subprocess as _sp
                _sp.run(["open", str(path)], check=False)
            else:
                import subprocess as _sp
                _sp.run(["xdg-open", str(path)], check=False)
        except Exception as exc:
            logger.error("open_gallery failed: %s", exc)

    # -- Annotations -------------------------------------------------------

    def save_annotation(self, rel_path: str, note: str, trade_mark: str = "") -> bool:
        """
        Save or update annotation for a screenshot.

        Parameters
        ----------
        rel_path   : relative path from screenshot_folder (used as key)
        note       : free-text note
        trade_mark : "BULL" | "BEAR" | "NOTE" | ""

        Returns
        -------
        True on success.
        """
        if trade_mark not in TRADE_MARKS:
            logger.warning("Invalid trade_mark '%s' — cleared", trade_mark)
            trade_mark = ""
        with self._lock:
            notes = self._load_notes()
            now = datetime.utcnow().isoformat(timespec="seconds")
            existing = notes.get(rel_path)
            ann = Annotation(
                path=rel_path,
                note=note,
                trade_mark=trade_mark,
                created_at=existing.get("created_at", now) if existing else now,
                updated_at=now,
            )
            notes[rel_path] = ann.to_dict()
            self._save_notes(notes)
            logger.info("Annotation saved for %s (mark=%s)", rel_path, trade_mark)
            return True

    def get_annotation(self, rel_path: str) -> Optional[Annotation]:
        """Return annotation for *rel_path*, or None if absent."""
        with self._lock:
            notes = self._load_notes()
            data = notes.get(rel_path)
            if data:
                return Annotation(**{k: v for k, v in data.items() if k in Annotation.__dataclass_fields__})
            return None

    def delete_annotation(self, rel_path: str) -> bool:
        """Remove annotation for *rel_path*. Returns True if existed."""
        with self._lock:
            notes = self._load_notes()
            if rel_path in notes:
                del notes[rel_path]
                self._save_notes(notes)
                return True
            return False

    # -- Video export -------------------------------------------------------

    def export_video(
        self,
        symbol: str,
        market: Optional[str] = None,
        tag_filter: Optional[str] = None,
        fps: Optional[int] = None,
        output_path: Optional[Path] = None,
    ) -> Optional[Path]:
        """
        Export screenshots for *symbol* as an .mp4 video.

        Parameters
        ----------
        symbol      : e.g. "BTCUSD"
        market      : filter by market ("CRYPTO" / "US"), or None for all
        tag_filter  : only include images with this tag, or None for all
        fps         : frames per second (default: self.video_fps)
        output_path : override output path; default: gallery/exports/SYMBOL.mp4

        Returns
        -------
        Path to mp4 file, or None on failure.
        """
        try:
            import cv2  # type: ignore
        except ImportError:
            logger.error("opencv-python not installed — cannot export video.")
            return None

        with self._lock:
            index = self._scan_screenshots()
            entries = [
                e for e in index.entries
                if e.symbol.upper() == symbol.upper()
                and (market is None or e.market.upper() == market.upper())
                and (tag_filter is None or e.tag == tag_filter)
            ]

        if not entries:
            logger.warning("export_video: no images found for symbol=%s", symbol)
            return None

        entries.sort(key=lambda e: e.sort_key)
        fps = fps or self.video_fps
        out_path = output_path or (self.gallery_folder / "exports" / f"{symbol}.mp4")
        out_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Exporting %d frames → %s (fps=%d)",
            len(entries),
            out_path,
            fps,
        )

        writer: Optional[cv2.VideoWriter] = None
        frame_size: Optional[Tuple[int, int]] = None

        try:

            for entry in entries:
                img_bgr = cv2.imread(entry.abs_path)
                if img_bgr is None:
                    logger.debug("Skipping unreadable frame: %s", entry.abs_path)
                    continue

                h, w = img_bgr.shape[:2]
                if writer is None:
                    frame_size = (w, h)
                    fourcc = cv2.VideoWriter_fourcc(*VIDEO_CODEC)
                    writer = cv2.VideoWriter(str(out_path), fourcc, fps, frame_size)
                    if not writer.isOpened():
                        logger.error("Failed to open VideoWriter for %s", out_path)
                        return None
                elif (w, h) != frame_size:
                    img_bgr = cv2.resize(img_bgr, frame_size)

                # Overlay timestamp + tag on frame
                label = f"{entry.datetime_str}  {entry.tag}" if entry.tag else entry.datetime_str
                cv2.putText(
                    img_bgr, label,
                    (12, h - 12),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.55, (255, 255, 255), 1, cv2.LINE_AA,
                )
                cv2.putText(
                    img_bgr, label,
                    (12, h - 12),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.55, (0, 0, 0), 2, cv2.LINE_AA,
                )
                writer.write(img_bgr)

            if writer:
                writer.release()
                size_mb = out_path.stat().st_size / 1024 ** 2
                logger.info("Video exported: %s (%.1f MB)", out_path, size_mb)
                return out_path
            return None

        except Exception as exc:
            logger.error("export_video failed for %s: %s", symbol, exc, exc_info=True)
            if writer:
                try:
                    writer.release()
                except Exception:
                    pass
            return None

    # ------------------------------------------------------------------
    # Internal — scanning
    # ------------------------------------------------------------------

    def _scan_screenshots(self) -> GalleryIndex:
        """
        Walk screenshot_folder and collect ImageEntry for each PNG.

        Path pattern: MARKET/SYMBOL/YYYY-MM/filename.png
        """
        entries: List[ImageEntry] = []
        symbols: set[str] = set()
        markets: set[str] = set()
        months: set[str] = set()
        tags: set[str] = set()
        drives_seen: set[str] = set()

        if not self.screenshot_folder.exists():
            logger.warning("Screenshot folder not found: %s", self.screenshot_folder)
            return GalleryIndex(generated_at=datetime.utcnow().isoformat())

        for png in self.screenshot_folder.rglob("*.png"):
            try:
                entry = self._parse_file(png)
                if entry:
                    entries.append(entry)
                    symbols.add(entry.symbol)
                    markets.add(entry.market)
                    months.add(entry.month)
                    if entry.tag:
                        tags.add(entry.tag)
                    if entry.drive_name:
                        drives_seen.add(entry.drive_name)
            except Exception as exc:
                logger.debug("Failed to parse %s: %s", png, exc)

        entries.sort(key=lambda e: (e.symbol, e.sort_key))

        # Merge drive names: from scanned entries + from config (even if no images yet)
        all_drives: list[str] = sorted(
            drives_seen | set(self.drive_names),
            key=lambda d: (int(d.replace("Drive", "")) if d.startswith("Drive") and d[5:].isdigit() else 999, d),
        )

        return GalleryIndex(
            entries=entries,
            symbols=sorted(symbols),
            markets=sorted(markets),
            months=sorted(months),
            tags=sorted(tags),
            drives=all_drives,
            total_files=len(entries),
            generated_at=datetime.utcnow().isoformat(timespec="seconds"),
        )

    def _parse_file(self, path: Path) -> Optional[ImageEntry]:
        """Parse a PNG path into an ImageEntry. Returns None if unrecognised."""
        m = _FNAME_RE.match(path.name)
        if not m:
            return None

        # Derive market from grandparent folder name (MARKET/SYMBOL/YYYY-MM/)
        parts = path.relative_to(self.screenshot_folder).parts
        market = parts[0].upper() if len(parts) >= 3 else "UNKNOWN"
        symbol = m.group("symbol").upper()
        date_str = m.group("date")
        time_str = m.group("time")
        tag = (m.group("tag") or "").upper()
        is_pinned = ".PINNED." in path.name.upper()

        try:
            size_bytes = path.stat().st_size
        except OSError:
            size_bytes = 0

        rel = str(path.relative_to(self.screenshot_folder)).replace("\\", "/")

        # Try to infer drive_name from a .drive sidecar file or metadata JSON
        # Pattern: same filename but extension .drive → contains drive name string
        # e.g. BTCTHB_2026-04-14_10-00.drive → "Drive1"
        drive_name: str = ""
        drive_sidecar = path.with_suffix(".drive")
        if drive_sidecar.exists():
            try:
                drive_name = drive_sidecar.read_text(encoding="utf-8").strip()
            except OSError:
                pass

        # Read .driveurl sidecar — Google Drive public direct URL
        # Written by module6_integration after successful Drive sync
        drive_public_url: str = ""
        driveurl_sidecar = path.with_suffix(".driveurl")
        if driveurl_sidecar.exists():
            try:
                drive_public_url = driveurl_sidecar.read_text(encoding="utf-8").strip()
            except OSError:
                pass

        return ImageEntry(
            path=rel,
            abs_path=str(path),
            symbol=symbol,
            market=market,
            date=date_str,
            time=time_str,
            tag=tag,
            is_pinned=is_pinned,
            size_bytes=size_bytes,
            month=date_str[:7],
            drive_name=drive_name,
            drive_public_url=drive_public_url,
        )

    # ------------------------------------------------------------------
    # Internal — data.json
    # ------------------------------------------------------------------

    def _write_data_json(self, index: GalleryIndex) -> bool:
        """Write gallery/data.json. Returns True on success."""
        out = self.gallery_folder / DATA_FILE
        try:
            self.gallery_folder.mkdir(parents=True, exist_ok=True)
            out.write_text(
                json.dumps(index.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.debug("data.json written → %s", out)
            return True
        except Exception as exc:
            logger.error("Failed to write data.json: %s", exc)
            return False

    def _merge_historical(self, current_index: GalleryIndex) -> GalleryIndex:
        """
        Merge entries from the existing data.json (historical) with the
        currently-scanned entries (current_index).

        This is essential for ephemeral CI environments (e.g. GitHub Actions)
        where screenshot PNG files are NOT persisted in the repo between runs.
        Only gallery/data.json and sidecar files (.drive, .driveurl) are
        committed to the repo; PNG files are uploaded to Google Drive.

        Strategy
        --------
        - Key each entry by its ``path`` field (relative to screenshot_folder).
        - Current-scan entries take priority over historical ones
          (they have fresher sidecar data: drive_name, drive_public_url).
        - Historical entries whose path is NOT found in the current scan
          are kept as-is (these are screenshots from previous runs that no
          longer exist as local PNG files).
        - Metadata sets (symbols, markets, months, tags, drives) are rebuilt
          from the merged entry list.

        Returns
        -------
        A new GalleryIndex with the merged result.
        """
        data_path = self.gallery_folder / DATA_FILE
        if not data_path.exists():
            # No history yet — nothing to merge
            return current_index

        try:
            raw = json.loads(data_path.read_text(encoding="utf-8"))
            historical_entries_raw: list = raw.get("entries", [])
        except Exception as exc:
            logger.warning("_merge_historical: could not read existing data.json (%s) — skipping merge", exc)
            return current_index

        if not historical_entries_raw:
            return current_index

        # Build lookup of current-scan entries by path (highest priority)
        current_by_path: Dict[str, ImageEntry] = {
            e.path: e for e in current_index.entries
        }

        # Reconstruct historical ImageEntry objects from stored dicts
        historical_count = 0
        for raw_entry in historical_entries_raw:
            path_key = raw_entry.get("path", "")
            if not path_key:
                continue
            if path_key in current_by_path:
                # Current scan already has this file — skip historical duplicate
                # BUT update drive_public_url / drive_name if historical has them
                # and current scan doesn't (file may have been uploaded since)
                cur = current_by_path[path_key]
                if not cur.drive_public_url and raw_entry.get("drive_public_url"):
                    cur.drive_public_url = raw_entry["drive_public_url"]
                if not cur.drive_name and raw_entry.get("drive_name"):
                    cur.drive_name = raw_entry["drive_name"]
                continue

            # This entry is NOT on disk now → keep it from history
            try:
                entry = ImageEntry(
                    path=path_key,
                    abs_path=raw_entry.get("abs_path", path_key),
                    symbol=raw_entry.get("symbol", ""),
                    market=raw_entry.get("market", "UNKNOWN"),
                    date=raw_entry.get("date", ""),
                    time=raw_entry.get("time", ""),
                    tag=raw_entry.get("tag", ""),
                    is_pinned=bool(raw_entry.get("is_pinned", False)),
                    size_bytes=int(raw_entry.get("size_bytes", 0)),
                    month=raw_entry.get("month", raw_entry.get("date", "")[:7]),
                    drive_name=raw_entry.get("drive_name", ""),
                    drive_public_url=raw_entry.get("drive_public_url", ""),
                )
                if entry.symbol:  # skip corrupt entries
                    current_by_path[path_key] = entry
                    historical_count += 1
            except Exception as exc:
                logger.debug("_merge_historical: skip corrupt entry %r: %s", path_key, exc)

        if historical_count:
            logger.info(
                "_merge_historical: kept %d historical entries not on disk this run",
                historical_count,
            )

        # Rebuild merged list and metadata
        merged_entries: List[ImageEntry] = sorted(
            current_by_path.values(),
            key=lambda e: (e.symbol, e.sort_key),
        )

        symbols: set = {e.symbol for e in merged_entries if e.symbol}
        markets: set = {e.market for e in merged_entries if e.market}
        months: set  = {e.month  for e in merged_entries if e.month}
        tags: set    = {e.tag    for e in merged_entries if e.tag}
        drives_seen: set = {e.drive_name for e in merged_entries if e.drive_name}

        all_drives: list = sorted(
            drives_seen | set(self.drive_names),
            key=lambda d: (
                int(d.replace("Drive", "")) if d.startswith("Drive") and d[5:].isdigit() else 999,
                d,
            ),
        )

        return GalleryIndex(
            entries=merged_entries,
            symbols=sorted(symbols),
            markets=sorted(markets),
            months=sorted(months),
            tags=sorted(tags),
            drives=all_drives,
            total_files=len(merged_entries),
            generated_at=datetime.utcnow().isoformat(timespec="seconds"),
        )

    # ------------------------------------------------------------------
    # Internal — notes.json
    # ------------------------------------------------------------------

    def _load_notes(self) -> dict:
        notes_path = self.gallery_folder / NOTES_FILE
        if notes_path.exists():
            try:
                return json.loads(notes_path.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning("notes.json load failed: %s", exc)
        return {}

    def _save_notes(self, notes: dict) -> None:
        notes_path = self.gallery_folder / NOTES_FILE
        try:
            notes_path.write_text(
                json.dumps(notes, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.error("notes.json save failed: %s", exc)

    # ------------------------------------------------------------------
    # Internal — HTML generation
    # ------------------------------------------------------------------

    def _generate_gallery_html(self, index: GalleryIndex, notes: Optional[dict] = None) -> bool:
        """Write gallery/index.html — main gallery with filter sidebar. Returns True on success."""
        abs_screenshot = self.screenshot_folder.resolve().as_posix()
        inline_data = json.dumps(index.to_dict(), ensure_ascii=False)
        inline_notes = json.dumps(notes or {}, ensure_ascii=False)
        html = (
            _GALLERY_HTML_TEMPLATE
            .replace("%%GENERATED_AT%%", index.generated_at)
            .replace("%%TOTAL_FILES%%", str(index.total_files))
            .replace("%%SCREENSHOT_FOLDER%%", str(self.screenshot_folder))
            .replace("%%ABS_SCREENSHOT_FOLDER%%", abs_screenshot)
            .replace("%%GITHUB_PAGES_URL%%", self.github_pages_url)
            .replace("%%INLINE_DATA%%", inline_data)
            .replace("%%INLINE_NOTES%%", inline_notes)
            .replace("%%GALLERY_PASSWORD_HASH%%", self.password_hash)
        )
        out = self.gallery_folder / GALLERY_HTML
        try:
            self.gallery_folder.mkdir(parents=True, exist_ok=True)
            out.write_text(html, encoding="utf-8")
            logger.debug("index.html written → %s", out)
            return True
        except Exception as exc:
            logger.error("Failed to write index.html: %s", exc)
            return False

    def _generate_replay_html(self, index: GalleryIndex, notes: Optional[dict] = None) -> bool:
        """Write gallery/replay.html — chart replay player. Returns True on success."""
        abs_screenshot = self.screenshot_folder.resolve().as_posix()
        inline_data = json.dumps(index.to_dict(), ensure_ascii=False)
        inline_notes = json.dumps(notes or {}, ensure_ascii=False)
        html = (
            _REPLAY_HTML_TEMPLATE
            .replace("%%GENERATED_AT%%", index.generated_at)
            .replace("%%SCREENSHOT_FOLDER%%", str(self.screenshot_folder))
            .replace("%%ABS_SCREENSHOT_FOLDER%%", abs_screenshot)
            .replace("%%GITHUB_PAGES_URL%%", self.github_pages_url)
            .replace("%%INLINE_DATA%%", inline_data)
            .replace("%%INLINE_NOTES%%", inline_notes)
            .replace("%%GALLERY_PASSWORD_HASH%%", self.password_hash)
        )
        out = self.gallery_folder / REPLAY_HTML
        try:
            self.gallery_folder.mkdir(parents=True, exist_ok=True)
            out.write_text(html, encoding="utf-8")
            logger.debug("replay.html written → %s", out)
            return True
        except Exception as exc:
            logger.error("Failed to write replay.html: %s", exc)
            return False


# ---------------------------------------------------------------------------
# HTML Templates
# ---------------------------------------------------------------------------

_GALLERY_HTML_TEMPLATE = r"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>TradingView AutoShot — Gallery</title>
<style>
:root {
  --bg: #0d1117; --panel: #161b22; --border: #30363d;
  --accent: #00c896; --text: #e6edf3; --muted: #8b949e;
  --bull: #2ea043; --bear: #da3633; --note: #e3b341;
  --pin: #a371f7; --card-bg: #1c2128;
  --tool-h: 52px;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; display: flex; height: 100vh; overflow: hidden; }

/* ── Sidebar ── */
#sidebar {
  width: 260px; min-width: 200px; background: var(--panel);
  border-right: 1px solid var(--border); padding: 16px;
  overflow-y: auto; flex-shrink: 0;
}
#sidebar h1 { font-size: 14px; color: var(--accent); margin-bottom: 16px; letter-spacing: 0.05em; }
.filter-group { margin-bottom: 14px; }
.filter-group label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; display: block; margin-bottom: 4px; }
.filter-group select, .filter-group input {
  width: 100%; background: var(--bg); border: 1px solid var(--border);
  color: var(--text); padding: 6px 8px; border-radius: 6px; font-size: 13px;
}
.filter-group select:focus, .filter-group input:focus { outline: none; border-color: var(--accent); }
.btn {
  display: inline-flex; align-items: center; justify-content: center; gap: 6px;
  padding: 7px 14px; border-radius: 6px; font-size: 13px; cursor: pointer;
  border: none; font-weight: 500; transition: opacity 0.15s;
}
.btn:hover { opacity: 0.85; }
.btn-accent { background: var(--accent); color: #000; }
.btn-outline { background: transparent; border: 1px solid var(--border); color: var(--text); }
.btn-full { width: 100%; margin-top: 6px; }
#stats { font-size: 12px; color: var(--muted); margin-top: 16px; line-height: 1.7; }

/* ── Main grid ── */
#main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
#toolbar {
  padding: 12px 16px; border-bottom: 1px solid var(--border);
  display: flex; align-items: center; gap: 10px; background: var(--panel); flex-shrink: 0;
}
#search {
  flex: 1; background: var(--bg); border: 1px solid var(--border);
  color: var(--text); padding: 7px 12px; border-radius: 6px; font-size: 13px;
}
#search:focus { outline: none; border-color: var(--accent); }
#sort-sel { background: var(--bg); border: 1px solid var(--border); color: var(--text); padding: 7px; border-radius: 6px; font-size: 13px; }
#count { font-size: 12px; color: var(--muted); white-space: nowrap; }
#grid {
  flex: 1; overflow-y: auto; padding: 16px;
  display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 12px;
}
.card {
  background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px;
  overflow: hidden; cursor: pointer; transition: border-color 0.15s, transform 0.1s; position: relative;
}
.card:hover { border-color: var(--accent); transform: translateY(-1px); }
.card.pinned { border-color: var(--pin); }
.card img { width: 100%; aspect-ratio: 16/9; object-fit: cover; display: block; background: #000; }
.card-info { padding: 8px 10px; }
.card-symbol { font-weight: 600; font-size: 13px; }
.card-date { font-size: 11px; color: var(--muted); margin-top: 2px; }
.badge { display: inline-block; font-size: 10px; padding: 1px 6px; border-radius: 4px; font-weight: 600; margin-left: 6px; vertical-align: middle; }
.badge-tag { background: #1f3d2e; color: var(--accent); }
.badge-pin { background: #2d1f5e; color: var(--pin); }
.badge-bull { background: #1f3d2e; color: var(--bull); }
.badge-bear { background: #3d1f1f; color: var(--bear); }
.badge-note { background: #3d331f; color: var(--note); }
.mark-bar { position: absolute; top: 0; left: 0; right: 0; height: 3px; }
.mark-bar.BULL { background: var(--bull); }
.mark-bar.BEAR { background: var(--bear); }
.mark-bar.NOTE { background: var(--note); }
#empty { display: none; text-align: center; color: var(--muted); padding: 60px 20px; grid-column: 1/-1; font-size: 14px; }
::-webkit-scrollbar { width: 6px; } ::-webkit-scrollbar-track { background: var(--bg); } ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

/* ══════════════════════════════════════════════════
   LIGHTBOX — full-screen annotation workspace
══════════════════════════════════════════════════ */
#lightbox {
  display: none; position: fixed; inset: 0; background: #080c10;
  z-index: 1000; flex-direction: column;
}
#lightbox.open { display: flex; }

/* ── Top nav bar ── */
#lb-topbar {
  height: 48px; background: #0d1117; border-bottom: 1px solid #21262d;
  display: flex; align-items: center; gap: 8px; padding: 0 14px; flex-shrink: 0;
}
#lb-title { font-size: 13px; font-weight: 600; color: var(--text); flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
#lb-counter { font-size: 12px; color: var(--muted); margin-right: 6px; }
.lb-nav-btn {
  background: #161b22; border: 1px solid #30363d; color: var(--text);
  padding: 5px 12px; border-radius: 6px; cursor: pointer; font-size: 13px;
  transition: background 0.12s;
}
.lb-nav-btn:hover { background: #21262d; }
#lb-close-btn {
  background: none; border: none; color: #8b949e; font-size: 22px;
  cursor: pointer; line-height: 1; padding: 4px 6px; margin-left: 4px;
}
#lb-close-btn:hover { color: var(--text); }

/* ── Tool bar ── */
#lb-toolbar {
  height: var(--tool-h); background: #0d1117; border-bottom: 1px solid #21262d;
  display: flex; align-items: center; gap: 4px; padding: 0 12px; flex-shrink: 0;
  overflow-x: auto; flex-wrap: nowrap;
}
.tb-sep { width: 1px; height: 28px; background: #30363d; margin: 0 4px; flex-shrink: 0; }
.tool-btn {
  position: relative; background: none; border: 1px solid transparent;
  border-radius: 6px; color: #c9d1d9; cursor: pointer; padding: 5px 8px;
  font-size: 18px; line-height: 1; transition: background 0.1s, border-color 0.1s;
  flex-shrink: 0; display: flex; align-items: center; gap: 4px;
}
.tool-btn:hover { background: #21262d; border-color: #30363d; }
.tool-btn.active { background: #1f3d2e; border-color: var(--accent); color: var(--accent); }
.tool-btn .tb-label { font-size: 10px; font-family: 'Segoe UI', sans-serif; white-space: nowrap; }

/* Tooltip */
.tool-btn::after {
  content: attr(data-tip);
  position: absolute; bottom: calc(100% + 8px); left: 50%; transform: translateX(-50%);
  background: #1c2128; border: 1px solid #30363d; color: #e6edf3;
  font-size: 11px; font-family: 'Segoe UI', sans-serif; padding: 4px 8px;
  border-radius: 5px; white-space: nowrap; pointer-events: none;
  opacity: 0; transition: opacity 0.15s; z-index: 9999;
}
.tool-btn:hover::after { opacity: 1; }

/* Color swatches */
#color-row { display: flex; gap: 5px; align-items: center; flex-shrink: 0; }
.swatch {
  width: 22px; height: 22px; border-radius: 50%; border: 2px solid transparent;
  cursor: pointer; transition: transform 0.1s, border-color 0.1s; flex-shrink: 0;
}
.swatch:hover { transform: scale(1.2); }
.swatch.active { border-color: #fff; }
#custom-color { width: 26px; height: 26px; border-radius: 50%; border: 2px solid #30363d; cursor: pointer; padding: 0; background: none; flex-shrink: 0; }
#custom-color::-webkit-color-swatch-wrapper { padding: 0; border-radius: 50%; }
#custom-color::-webkit-color-swatch { border-radius: 50%; border: none; }

/* Size slider */
#size-row { display: flex; align-items: center; gap: 6px; flex-shrink: 0; }
#size-row span { font-size: 11px; color: var(--muted); }
#stroke-size { width: 70px; accent-color: var(--accent); }
#size-preview { width: 20px; height: 20px; display: flex; align-items: center; justify-content: center; }

/* Opacity */
#opacity-row { display: flex; align-items: center; gap: 6px; flex-shrink: 0; }
#opacity-row span { font-size: 11px; color: var(--muted); }
#opacity-slider { width: 60px; accent-color: var(--accent); }

/* Line style */
#line-style-row { display: flex; gap: 4px; flex-shrink: 0; }
.ls-btn { background: none; border: 1px solid #30363d; border-radius: 5px; padding: 4px 8px; cursor: pointer; color: #c9d1d9; font-size: 12px; transition: background 0.1s; }
.ls-btn:hover { background: #21262d; }
.ls-btn.active { background: #1f3d2e; border-color: var(--accent); color: var(--accent); }

/* Font size */
#font-size-row { display: flex; align-items: center; gap: 5px; flex-shrink: 0; }
#font-size-row span { font-size: 11px; color: var(--muted); }
#font-size { width: 52px; background: #0d1117; border: 1px solid #30363d; color: var(--text); padding: 4px 6px; border-radius: 5px; font-size: 12px; }

/* ── Workspace ── */
#lb-workspace {
  flex: 1; position: relative; overflow: hidden;
  display: flex; align-items: center; justify-content: center; background: #080c10;
}
#lb-img-base {
  position: absolute; max-width: none; max-height: none;
  transform-origin: center center; pointer-events: none; user-select: none;
  image-rendering: -webkit-optimize-contrast;
}
#lb-canvas {
  position: absolute; cursor: crosshair;
  transform-origin: center center;
}

/* zoom indicator */
#zoom-badge {
  position: absolute; bottom: 12px; right: 14px;
  background: rgba(13,17,23,0.85); border: 1px solid #30363d;
  color: var(--muted); font-size: 11px; padding: 3px 8px; border-radius: 5px;
  pointer-events: none; user-select: none;
}

/* ── Bottom panel (note + mark + save) ── */
#lb-bottombar {
  background: #0d1117; border-top: 1px solid #21262d;
  padding: 10px 14px; display: flex; align-items: center; gap: 10px; flex-shrink: 0; flex-wrap: wrap;
}
#lb-note-input {
  flex: 1; min-width: 200px; background: #161b22; border: 1px solid #30363d;
  color: var(--text); padding: 7px 10px; border-radius: 6px; font-size: 13px;
}
#lb-note-input:focus { outline: none; border-color: var(--accent); }
#lb-mark-sel { background: #161b22; border: 1px solid #30363d; color: var(--text); padding: 7px; border-radius: 6px; font-size: 13px; }
.lb-action-btn { padding: 7px 16px; border-radius: 6px; font-size: 13px; cursor: pointer; border: none; font-weight: 500; transition: opacity 0.15s; }
.lb-action-btn:hover { opacity: 0.85; }
.btn-save { background: var(--accent); color: #000; }
.btn-undo { background: #161b22; border: 1px solid #30363d; color: var(--text); }
.btn-clear-ann { background: #161b22; border: 1px solid #da3633; color: #da3633; }
.btn-export { background: #161b22; border: 1px solid #58a6ff; color: #58a6ff; }

/* text input overlay */
#text-input-overlay {
  position: absolute; display: none; z-index: 200;
  background: rgba(13,17,23,0.9); border: 1px solid var(--accent);
  border-radius: 4px; padding: 0;
}
#text-input-field {
  background: transparent; border: none; outline: none;
  font-size: 16px; color: #fff; padding: 4px 6px;
  min-width: 120px; resize: none; overflow: hidden;
}

/* ── Password screen ── */
#pw-screen {
  position: fixed; inset: 0; background: var(--bg);
  display: flex; align-items: center; justify-content: center;
  z-index: 9999; flex-direction: column; gap: 16px;
}
#pw-screen h2 { font-size: 18px; color: var(--accent); font-weight: 500; }
#pw-screen p  { font-size: 13px; color: var(--muted); }
#pw-box {
  background: var(--panel); border: 1px solid var(--border);
  color: var(--text); padding: 10px 16px; border-radius: 8px;
  font-size: 15px; width: 260px; text-align: center;
  outline: none;
}
#pw-box:focus { border-color: var(--accent); }
#pw-btn {
  background: var(--accent); color: #000; border: none;
  padding: 10px 32px; border-radius: 8px; font-size: 14px;
  font-weight: 600; cursor: pointer; width: 260px;
}
#pw-btn:hover { opacity: 0.85; }
#pw-err { color: #da3633; font-size: 13px; display: none; }
</style>
</head>
<body>

<!-- Password gate -->
<div id="pw-screen">
  <h2>🔒 AutoShot Gallery</h2>
  <p>Enter password to view screenshots</p>
  <input id="pw-box" type="password" placeholder="Password"
         onkeydown="if(event.key==='Enter')checkPw()"/>
  <button id="pw-btn" onclick="checkPw()">Unlock</button>
  <span id="pw-err">Incorrect password — try again</span>
</div>

<div id="sidebar">
  <h1>📷 AutoShot Gallery</h1>
  <div class="filter-group">
    <label>Symbol</label>
    <select id="f-symbol"><option value="">All Symbols</option></select>
  </div>
  <div class="filter-group">
    <label>Market</label>
    <select id="f-market"><option value="">All Markets</option></select>
  </div>
  <div class="filter-group">
    <label>Month</label>
    <select id="f-month"><option value="">All Months</option></select>
  </div>
  <div class="filter-group">
    <label>Tag / Session</label>
    <select id="f-tag"><option value="">All Tags</option></select>
  </div>
  <div class="filter-group">
    <label>Mark</label>
    <select id="f-mark">
      <option value="">All Marks</option>
      <option value="BULL">🟢 Bull</option>
      <option value="BEAR">🔴 Bear</option>
      <option value="NOTE">📝 Note</option>
    </select>
  </div>
  <div class="filter-group">
    <label>Drive</label>
    <select id="f-drive"><option value="">All Drives</option></select>
  </div>
  <div class="filter-group">
    <label>Pinned Only</label>
    <select id="f-pin">
      <option value="">All</option>
      <option value="1">Pinned only</option>
    </select>
  </div>
  <button class="btn btn-accent btn-full" onclick="applyFilters()">Apply Filters</button>
  <button class="btn btn-outline btn-full" onclick="clearFilters()">Clear Filters</button>
  <button class="btn btn-outline btn-full" style="margin-top:10px;" onclick="openReplay()">▶ Chart Replay</button>
  <div id="stats"></div>
</div>

<div id="main">
  <div id="toolbar">
    <input id="search" type="text" placeholder="Search symbol, date, tag…" oninput="applyFilters()"/>
    <select id="sort-sel" onchange="applyFilters()">
      <option value="newest">Newest First</option>
      <option value="oldest">Oldest First</option>
      <option value="symbol">Symbol A-Z</option>
    </select>
    <span id="count"></span>
  </div>
  <div id="grid">
    <div id="empty">No screenshots found matching the current filters.</div>
  </div>
</div>

<!-- ═══════════════════════════════════════════════════════
     LIGHTBOX — full annotation workspace
═══════════════════════════════════════════════════════ -->
<div id="lightbox">

  <!-- Top bar: title + navigation + close -->
  <div id="lb-topbar">
    <div id="lb-title">—</div>
    <span id="lb-counter"></span>
    <button class="lb-nav-btn" onclick="lbStep(-1)" title="Previous (←)">← Prev</button>
    <button class="lb-nav-btn" onclick="lbStep(1)"  title="Next (→)">Next →</button>
    <button id="lb-close-btn" onclick="closeLightbox()" title="Close (Esc)">✕</button>
  </div>

  <!-- Tool bar -->
  <div id="lb-toolbar">

    <!-- Draw tools -->
    <button class="tool-btn active" id="tool-pen"       onclick="setTool('pen')"       data-tip="✏️ Pen — free draw (P)">✏️ <span class="tb-label">Pen</span></button>
    <button class="tool-btn"        id="tool-line"      onclick="setTool('line')"      data-tip="📏 Straight Line (L)">📏 <span class="tb-label">Line</span></button>
    <button class="tool-btn"        id="tool-arrow"     onclick="setTool('arrow')"     data-tip="➡️ Arrow (A)">➡️ <span class="tb-label">Arrow</span></button>
    <button class="tool-btn"        id="tool-rect"      onclick="setTool('rect')"      data-tip="▭ Rectangle (R)">▭ <span class="tb-label">Rect</span></button>
    <button class="tool-btn"        id="tool-circle"    onclick="setTool('circle')"    data-tip="⭕ Circle / Ellipse (C)">⭕ <span class="tb-label">Circle</span></button>
    <button class="tool-btn"        id="tool-text"      onclick="setTool('text')"      data-tip="T  Text label (T)">T <span class="tb-label">Text</span></button>
    <button class="tool-btn"        id="tool-measure"   onclick="setTool('measure')"   data-tip="📐 Measure distance (M)">📐 <span class="tb-label">Measure</span></button>
    <button class="tool-btn"        id="tool-highlight" onclick="setTool('highlight')" data-tip="🖌️ Highlight (H)">🖌️ <span class="tb-label">Hi-lite</span></button>
    <button class="tool-btn"        id="tool-eraser"    onclick="setTool('eraser')"    data-tip="🧹 Eraser (E)">🧹 <span class="tb-label">Erase</span></button>

    <div class="tb-sep"></div>

    <!-- Pan / Zoom -->
    <button class="tool-btn" id="tool-pan"  onclick="setTool('pan')" data-tip="✋ Pan / Move image (Space+drag)">✋ <span class="tb-label">Pan</span></button>
    <button class="tool-btn" onclick="zoomIn()"  data-tip="🔍 Zoom In (+)">🔍+</button>
    <button class="tool-btn" onclick="zoomOut()" data-tip="🔎 Zoom Out (-)">🔎−</button>
    <button class="tool-btn" onclick="zoomFit()" data-tip="⊡ Fit to screen (F)">⊡ <span class="tb-label">Fit</span></button>
    <button class="tool-btn" onclick="zoom100()" data-tip="1:1 Actual size (1)">1:1</button>

    <div class="tb-sep"></div>

    <!-- Colors -->
    <div id="color-row">
      <span style="font-size:10px;color:var(--muted);margin-right:2px;">Color</span>
      <div class="swatch active" style="background:#ff4444" data-color="#ff4444" onclick="setColor(this)" title="Red"></div>
      <div class="swatch" style="background:#00c896" data-color="#00c896" onclick="setColor(this)" title="Green"></div>
      <div class="swatch" style="background:#58a6ff" data-color="#58a6ff" onclick="setColor(this)" title="Blue"></div>
      <div class="swatch" style="background:#ffd700" data-color="#ffd700" onclick="setColor(this)" title="Yellow"></div>
      <div class="swatch" style="background:#ff8c00" data-color="#ff8c00" onclick="setColor(this)" title="Orange"></div>
      <div class="swatch" style="background:#da70d6" data-color="#da70d6" onclick="setColor(this)" title="Purple"></div>
      <div class="swatch" style="background:#ffffff" data-color="#ffffff" onclick="setColor(this)" title="White"></div>
      <div class="swatch" style="background:#000000;border:1px solid #555" data-color="#000000" onclick="setColor(this)" title="Black"></div>
      <input type="color" id="custom-color" value="#ff4444" onchange="setCustomColor(this.value)" title="Custom color" data-tip="Custom color picker"/>
    </div>

    <div class="tb-sep"></div>

    <!-- Stroke size -->
    <div id="size-row">
      <span>Size</span>
      <input type="range" id="stroke-size" min="1" max="40" value="3" oninput="updateSizePreview()" title="Stroke size"/>
      <div id="size-preview"><svg id="size-dot" viewBox="0 0 20 20" width="20" height="20"><circle cx="10" cy="10" r="4" fill="#ff4444"/></svg></div>
    </div>

    <div class="tb-sep"></div>

    <!-- Opacity -->
    <div id="opacity-row">
      <span>Opacity</span>
      <input type="range" id="opacity-slider" min="5" max="100" value="100" title="Opacity"/>
      <span id="opacity-val" style="font-size:11px;color:var(--muted);min-width:28px;">100%</span>
    </div>

    <div class="tb-sep"></div>

    <!-- Line style -->
    <div id="line-style-row">
      <button class="ls-btn active" id="ls-solid"  onclick="setLineStyle('solid')"  title="Solid line">—</button>
      <button class="ls-btn"        id="ls-dashed" onclick="setLineStyle('dashed')" title="Dashed line">- -</button>
      <button class="ls-btn"        id="ls-dotted" onclick="setLineStyle('dotted')" title="Dotted line">···</button>
    </div>

    <div class="tb-sep"></div>

    <!-- Font size (shown when text tool active) -->
    <div id="font-size-row">
      <span>Font</span>
      <input type="number" id="font-size" value="18" min="8" max="120" title="Font size (px)"/>
      <span>px</span>
    </div>

  </div><!-- end lb-toolbar -->

  <!-- Workspace: image + canvas -->
  <div id="lb-workspace">
    <img id="lb-img-base" src="" alt="screenshot"/>
    <canvas id="lb-canvas"></canvas>
    <div id="zoom-badge">100%</div>
    <!-- Text input floating box -->
    <div id="text-input-overlay">
      <textarea id="text-input-field" rows="1" placeholder="Type…"></textarea>
    </div>
  </div>

  <!-- Bottom bar: note + mark + actions -->
  <div id="lb-bottombar">
    <input id="lb-note-input" type="text" placeholder="📝 Add note / trade journal…"/>
    <select id="lb-mark-sel">
      <option value="">No mark</option>
      <option value="BULL">🟢 Bull</option>
      <option value="BEAR">🔴 Bear</option>
      <option value="NOTE">📝 Note</option>
    </select>
    <button class="lb-action-btn btn-undo"      onclick="undoStroke()"        title="Undo last stroke (Ctrl+Z)">↩ Undo</button>
    <button class="lb-action-btn btn-clear-ann" onclick="clearAnnotations()"  title="Clear all annotations">🗑 Clear</button>
    <button class="lb-action-btn btn-export"    onclick="exportAnnotated()"   title="Save annotated image as PNG">💾 Export PNG</button>
    <button class="lb-action-btn btn-save"      onclick="saveAnnotation()"    title="Save note + mark (Ctrl+S)">✔ Save</button>
  </div>

</div><!-- end lightbox -->

<script>
/* ═══════════════════════════════════════════════
   Gallery state
═══════════════════════════════════════════════ */
let ALL = [];
let FILTERED = [];
let NOTES = {};
let lbIdx = 0;

const _INLINE_DATA      = %%INLINE_DATA%%;
const _INLINE_NOTES     = %%INLINE_NOTES%%;
const _ABS_SHOT_DIR     = '%%ABS_SCREENSHOT_FOLDER%%';
const _GITHUB_PAGES_URL = '%%GITHUB_PAGES_URL%%';  // "" = disabled

/* ═══════════════════════════════════════════════
   Annotation / drawing state
═══════════════════════════════════════════════ */
let currentTool   = 'pen';
let drawColor     = '#ff4444';
let strokeSize    = 3;
let drawOpacity   = 1.0;
let lineStyle     = 'solid';      // 'solid' | 'dashed' | 'dotted'
let fontSize      = 18;

// Per-image strokes (stack for undo)
// Key = entry.path, Value = array of {type, points, color, size, opacity, style, ...}
const strokeMap = {};
let currentStrokes = [];          // reference to strokeMap[currentPath]

// Drawing in progress
let isDrawing = false;
let startX = 0;
let startY = 0;
let lastX  = 0;
let lastY  = 0;
let tmpCanvas = null;             // used for live preview while drawing shapes

// Zoom / pan
let zoomLevel  = 1.0;
let panX       = 0;
let panY       = 0;
let isPanning  = false;
let panStartX  = 0;
let panStartY  = 0;
let spaceDown  = false;

const canvas   = document.getElementById('lb-canvas');
const ctx      = canvas.getContext('2d');
const imgEl    = document.getElementById('lb-img-base');
const workspace = document.getElementById('lb-workspace');

/* ─── Colour helper: hex → rgba ─── */
function hexAlpha(hex, alpha) {
  const r = parseInt(hex.slice(1,3),16);
  const g = parseInt(hex.slice(3,5),16);
  const b = parseInt(hex.slice(5,7),16);
  return `rgba(${r},${g},${b},${alpha})`;
}

/* ═══════════════════════════════════════════════
   Tool selection
═══════════════════════════════════════════════ */
function setTool(t) {
  currentTool = t;
  document.querySelectorAll('.tool-btn[id^="tool-"]').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById('tool-' + t);
  if (btn) btn.classList.add('active');
  // cursor
  if (t === 'pan')     canvas.style.cursor = 'grab';
  else if (t === 'eraser') canvas.style.cursor = "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='24' height='24'%3E%3Ccircle cx='12' cy='12' r='10' stroke='white' stroke-width='2' fill='none'/%3E%3C/svg%3E\") 12 12, crosshair";
  else if (t === 'text') canvas.style.cursor = 'text';
  else canvas.style.cursor = 'crosshair';
}

/* ═══════════════════════════════════════════════
   Color
═══════════════════════════════════════════════ */
function setColor(el) {
  drawColor = el.dataset.color;
  document.querySelectorAll('.swatch').forEach(s => s.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('custom-color').value = drawColor;
  updateSizePreview();
}
function setCustomColor(val) {
  drawColor = val;
  document.querySelectorAll('.swatch').forEach(s => s.classList.remove('active'));
  updateSizePreview();
}
function updateSizePreview() {
  strokeSize = parseInt(document.getElementById('stroke-size').value);
  const r = Math.min(strokeSize / 2, 9);
  document.getElementById('size-dot').innerHTML = `<circle cx="10" cy="10" r="${r}" fill="${drawColor}"/>`;
}

/* ═══════════════════════════════════════════════
   Opacity & Line style
═══════════════════════════════════════════════ */
document.getElementById('opacity-slider').addEventListener('input', function() {
  drawOpacity = parseInt(this.value) / 100;
  document.getElementById('opacity-val').textContent = this.value + '%';
});
function setLineStyle(s) {
  lineStyle = s;
  document.querySelectorAll('.ls-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('ls-' + s).classList.add('active');
}

/* ═══════════════════════════════════════════════
   Zoom / Pan
═══════════════════════════════════════════════ */
function applyTransform() {
  const t = `translate(${panX}px, ${panY}px) scale(${zoomLevel})`;
  imgEl.style.transform  = t;
  canvas.style.transform = t;
  document.getElementById('zoom-badge').textContent = Math.round(zoomLevel * 100) + '%';
}

function zoomIn()  { zoomLevel = Math.min(zoomLevel * 1.25, 16); applyTransform(); }
function zoomOut() { zoomLevel = Math.max(zoomLevel / 1.25, 0.1); applyTransform(); }
function zoom100() { zoomLevel = 1; panX = 0; panY = 0; applyTransform(); }
function zoomFit() {
  if (!imgEl.naturalWidth) return;
  const ws = workspace.getBoundingClientRect();
  const scale = Math.min(ws.width / imgEl.naturalWidth, ws.height / imgEl.naturalHeight, 1);
  zoomLevel = scale; panX = 0; panY = 0; applyTransform();
}

// Mouse-wheel zoom
workspace.addEventListener('wheel', e => {
  e.preventDefault();
  const factor = e.deltaY < 0 ? 1.12 : 0.88;
  const newZoom = Math.min(Math.max(zoomLevel * factor, 0.1), 16);
  // Zoom toward cursor
  const rect = workspace.getBoundingClientRect();
  const cx = e.clientX - rect.left - rect.width  / 2;
  const cy = e.clientY - rect.top  - rect.height / 2;
  panX = cx - (cx - panX) * (newZoom / zoomLevel);
  panY = cy - (cy - panY) * (newZoom / zoomLevel);
  zoomLevel = newZoom;
  applyTransform();
}, { passive: false });

// Space-bar panning
document.addEventListener('keydown', e => {
  if (e.code === 'Space' && document.getElementById('lightbox').classList.contains('open')) {
    spaceDown = true; canvas.style.cursor = 'grab'; e.preventDefault();
  }
});
document.addEventListener('keyup', e => {
  if (e.code === 'Space') { spaceDown = false; if (currentTool !== 'pan') setTool(currentTool); }
});

/* ═══════════════════════════════════════════════
   Canvas helpers — coord conversion
═══════════════════════════════════════════════ */
function canvasCoords(e) {
  const rect   = canvas.getBoundingClientRect();
  const cx     = (e.clientX - rect.left) / zoomLevel;
  const cy     = (e.clientY - rect.top)  / zoomLevel;
  return [cx, cy];
}

/* ═══════════════════════════════════════════════
   Apply dash style to ctx
═══════════════════════════════════════════════ */
function applyLineStyle(c, sz) {
  if (lineStyle === 'dashed') c.setLineDash([sz * 4, sz * 2]);
  else if (lineStyle === 'dotted') c.setLineDash([sz, sz * 2]);
  else c.setLineDash([]);
}

/* ═══════════════════════════════════════════════
   Draw a single stroke object onto a context
═══════════════════════════════════════════════ */
function drawStroke(c, s) {
  c.save();
  c.globalAlpha  = s.opacity;
  c.strokeStyle  = s.color;
  c.fillStyle    = s.color;
  c.lineWidth    = s.size;
  c.lineCap      = 'round';
  c.lineJoin     = 'round';
  applyLineStyle(c, s.size);

  switch (s.type) {
    case 'pen':
    case 'highlight': {
      if (s.type === 'highlight') { c.globalAlpha = s.opacity * 0.35; c.lineWidth = s.size * 4; }
      if (!s.pts || s.pts.length < 2) break;
      c.beginPath(); c.moveTo(s.pts[0], s.pts[1]);
      for (let i = 2; i < s.pts.length; i += 2) c.lineTo(s.pts[i], s.pts[i+1]);
      c.stroke(); break;
    }
    case 'line': {
      c.beginPath(); c.moveTo(s.x1, s.y1); c.lineTo(s.x2, s.y2); c.stroke(); break;
    }
    case 'arrow': {
      const dx = s.x2 - s.x1; const dy = s.y2 - s.y1;
      const len = Math.sqrt(dx*dx + dy*dy); if (len < 1) break;
      const ux = dx/len; const uy = dy/len;
      const hw = Math.max(s.size * 3, 10);
      const hlen = Math.max(hw * 1.5, 15);
      c.beginPath(); c.moveTo(s.x1, s.y1); c.lineTo(s.x2, s.y2); c.stroke();
      c.beginPath();
      c.moveTo(s.x2, s.y2);
      c.lineTo(s.x2 - ux*hlen + uy*hw, s.y2 - uy*hlen - ux*hw);
      c.lineTo(s.x2 - ux*hlen - uy*hw, s.y2 - uy*hlen + ux*hw);
      c.closePath(); c.setLineDash([]); c.fill(); break;
    }
    case 'rect': {
      const w = s.x2 - s.x1; const h = s.y2 - s.y1;
      c.beginPath(); c.rect(s.x1, s.y1, w, h); c.stroke(); break;
    }
    case 'circle': {
      const rx = Math.abs(s.x2 - s.x1) / 2; const ry = Math.abs(s.y2 - s.y1) / 2;
      const cx2 = (s.x1 + s.x2) / 2; const cy2 = (s.y1 + s.y2) / 2;
      c.beginPath(); c.ellipse(cx2, cy2, rx, ry, 0, 0, Math.PI*2); c.stroke(); break;
    }
    case 'text': {
      c.setLineDash([]);
      c.font = `${s.fontSize || 18}px 'Segoe UI', sans-serif`;
      c.fillStyle = s.color; c.globalAlpha = s.opacity;
      (s.text || '').split('\n').forEach((line, i) => c.fillText(line, s.x1, s.y1 + i * (s.fontSize + 4)));
      break;
    }
    case 'measure': {
      const dx2 = s.x2 - s.x1; const dy2 = s.y2 - s.y1;
      const dist = Math.round(Math.sqrt(dx2*dx2 + dy2*dy2));
      c.beginPath(); c.moveTo(s.x1, s.y1); c.lineTo(s.x2, s.y2); c.stroke();
      c.setLineDash([]);
      c.font = 'bold 13px monospace'; c.fillStyle = s.color; c.globalAlpha = s.opacity;
      const mx = (s.x1+s.x2)/2; const my = (s.y1+s.y2)/2;
      c.fillText(`${dist}px`, mx + 4, my - 4); break;
    }
    case 'eraser': {
      c.globalCompositeOperation = 'destination-out';
      c.globalAlpha = 1; c.lineWidth = s.size * 3;
      if (!s.pts || s.pts.length < 2) break;
      c.beginPath(); c.moveTo(s.pts[0], s.pts[1]);
      for (let i = 2; i < s.pts.length; i += 2) c.lineTo(s.pts[i], s.pts[i+1]);
      c.stroke(); break;
    }
  }
  c.restore();
}

/* Redraw all committed strokes */
function redrawCanvas() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  currentStrokes.forEach(s => drawStroke(ctx, s));
}

/* ═══════════════════════════════════════════════
   Mouse events on canvas
═══════════════════════════════════════════════ */
canvas.addEventListener('mousedown', e => {
  if (e.button !== 0) return;
  const [cx, cy] = canvasCoords(e);

  // Pan mode
  if (currentTool === 'pan' || spaceDown) {
    isPanning = true; panStartX = e.clientX - panX; panStartY = e.clientY - panY;
    canvas.style.cursor = 'grabbing'; return;
  }

  // Text tool — show input overlay
  if (currentTool === 'text') {
    const overlay = document.getElementById('text-input-overlay');
    const field   = document.getElementById('text-input-field');
    const rect    = canvas.getBoundingClientRect();
    // position at click in screen space
    overlay.style.left    = (e.clientX - rect.left) + 'px';
    overlay.style.top     = (e.clientY - rect.top) + 'px';
    overlay.style.display = 'block';
    field.value = '';
    field.style.fontSize  = Math.max(fontSize * zoomLevel, 10) + 'px';
    field.style.color     = drawColor;
    field.focus();
    field.onkeydown = ev => {
      if (ev.key === 'Enter' && !ev.shiftKey) {
        const txt = field.value.trim();
        if (txt) {
          currentStrokes.push({ type:'text', x1:cx, y1:cy, text:txt, color:drawColor, size:strokeSize, opacity:drawOpacity, fontSize });
          redrawCanvas();
        }
        overlay.style.display = 'none'; ev.preventDefault();
      }
      if (ev.key === 'Escape') { overlay.style.display = 'none'; }
    };
    return;
  }

  isDrawing = true; startX = cx; startY = cy; lastX = cx; lastY = cy;
  strokeSize = parseInt(document.getElementById('stroke-size').value);
  fontSize   = parseInt(document.getElementById('font-size').value);
});

canvas.addEventListener('mousemove', e => {
  if (isPanning) {
    panX = e.clientX - panStartX; panY = e.clientY - panStartY; applyTransform(); return;
  }
  if (!isDrawing) return;
  const [cx, cy] = canvasCoords(e);

  if (currentTool === 'pen' || currentTool === 'highlight' || currentTool === 'eraser') {
    // Draw segment directly
    ctx.save();
    if (currentTool === 'eraser') {
      ctx.globalCompositeOperation = 'destination-out';
      ctx.globalAlpha = 1; ctx.lineWidth = strokeSize * 3;
    } else if (currentTool === 'highlight') {
      ctx.globalAlpha = drawOpacity * 0.35; ctx.lineWidth = strokeSize * 4; ctx.strokeStyle = drawColor;
    } else {
      ctx.globalAlpha = drawOpacity; ctx.lineWidth = strokeSize; ctx.strokeStyle = drawColor;
    }
    applyLineStyle(ctx, strokeSize);
    ctx.lineCap = 'round'; ctx.lineJoin = 'round';
    ctx.beginPath(); ctx.moveTo(lastX, lastY); ctx.lineTo(cx, cy); ctx.stroke();
    ctx.restore();
    lastX = cx; lastY = cy;
  } else {
    // Shape preview — redraw committed + preview
    redrawCanvas();
    ctx.save();
    ctx.globalAlpha = drawOpacity; ctx.strokeStyle = drawColor; ctx.fillStyle = drawColor;
    ctx.lineWidth = strokeSize; ctx.lineCap = 'round'; ctx.lineJoin = 'round';
    applyLineStyle(ctx, strokeSize);
    const tmp = { type: currentTool, x1: startX, y1: startY, x2: cx, y2: cy, color: drawColor, size: strokeSize, opacity: drawOpacity, style: lineStyle };
    drawStroke(ctx, tmp);
    ctx.restore();
  }
});

function endDraw(e) {
  if (isPanning) { isPanning = false; canvas.style.cursor = currentTool === 'pan' ? 'grab' : 'crosshair'; return; }
  if (!isDrawing) return;
  isDrawing = false;
  const [cx, cy] = canvasCoords(e);

  if (currentTool === 'pen' || currentTool === 'highlight' || currentTool === 'eraser') {
    // Store stroke as polyline (we'll rebuild pts from the last continuous segment — simpler: store start)
    // For undo, we stamp current canvas state as ImageData
    currentStrokes.push({ type: currentTool, pts: [startX, startY, cx, cy], color: drawColor, size: strokeSize, opacity: drawOpacity, style: lineStyle });
    // Note: for continuous pen, pts array is incomplete (just start/end); we use ctx directly for rendering live
    // For undo we'll snapshot ImageData
    _snapshots.push(ctx.getImageData(0, 0, canvas.width, canvas.height));
  } else {
    const s = { type: currentTool, x1: startX, y1: startY, x2: cx, y2: cy, color: drawColor, size: strokeSize, opacity: drawOpacity, style: lineStyle, fontSize };
    currentStrokes.push(s);
    _snapshots.push(ctx.getImageData(0, 0, canvas.width, canvas.height));
    redrawCanvas();
  }
}

canvas.addEventListener('mouseup',    endDraw);
canvas.addEventListener('mouseleave', endDraw);

// Snapshots for undo (ImageData stack)
let _snapshots = [];

/* ═══════════════════════════════════════════════
   Undo
═══════════════════════════════════════════════ */
function undoStroke() {
  if (_snapshots.length > 1) {
    _snapshots.pop();
    ctx.putImageData(_snapshots[_snapshots.length - 1], 0, 0);
    currentStrokes.pop();
  } else if (_snapshots.length === 1) {
    _snapshots.pop();
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    currentStrokes.pop();
  }
}

/* ═══════════════════════════════════════════════
   Clear all annotations
═══════════════════════════════════════════════ */
function clearAnnotations() {
  if (!confirm('Clear all annotations on this image?')) return;
  currentStrokes.length = 0;
  _snapshots.length = 0;
  ctx.clearRect(0, 0, canvas.width, canvas.height);
}

/* ═══════════════════════════════════════════════
   Export annotated PNG
═══════════════════════════════════════════════ */
function exportAnnotated() {
  const merged = document.createElement('canvas');
  merged.width  = canvas.width;
  merged.height = canvas.height;
  const mc = merged.getContext('2d');
  mc.drawImage(imgEl, 0, 0, canvas.width, canvas.height);
  mc.drawImage(canvas, 0, 0);
  const link  = document.createElement('a');
  const entry = FILTERED[lbIdx];
  link.download = (entry ? entry.symbol + '_' + entry.date + '_annotated' : 'annotated') + '.png';
  link.href = merged.toDataURL('image/png');
  link.click();
}

/* ═══════════════════════════════════════════════
   Keyboard shortcuts
═══════════════════════════════════════════════ */
document.addEventListener('keydown', e => {
  const lb = document.getElementById('lightbox');
  if (!lb.classList.contains('open')) return;
  if (document.getElementById('text-input-overlay').style.display === 'block') return;
  if (document.activeElement === document.getElementById('lb-note-input')) return;
  switch(e.key) {
    case 'Escape':     closeLightbox(); break;
    case 'ArrowLeft':  lbStep(-1); break;
    case 'ArrowRight': lbStep(1); break;
    case 'p': case 'P': setTool('pen'); break;
    case 'l': case 'L': setTool('line'); break;
    case 'a': case 'A': setTool('arrow'); break;
    case 'r': case 'R': setTool('rect'); break;
    case 'c': case 'C': setTool('circle'); break;
    case 't': case 'T': setTool('text'); break;
    case 'm': case 'M': setTool('measure'); break;
    case 'h': case 'H': setTool('highlight'); break;
    case 'e': case 'E': setTool('eraser'); break;
    case '+': case '=': zoomIn(); break;
    case '-': case '_': zoomOut(); break;
    case 'f': case 'F': zoomFit(); break;
    case '1':           zoom100(); break;
    case 'z': case 'Z':
      if (e.ctrlKey || e.metaKey) { undoStroke(); e.preventDefault(); } break;
    case 's': case 'S':
      if (e.ctrlKey || e.metaKey) { saveAnnotation(); e.preventDefault(); } break;
  }
});

/* ═══════════════════════════════════════════════
   Lightbox open / close / navigate
═══════════════════════════════════════════════ */
function openLightbox(i) {
  lbIdx = i;
  document.getElementById('lightbox').classList.add('open');
  loadLightboxImage();
}

function closeLightbox() {
  document.getElementById('lightbox').classList.remove('open');
  document.getElementById('text-input-overlay').style.display = 'none';
}

function lbStep(d) {
  lbIdx = Math.max(0, Math.min(FILTERED.length - 1, lbIdx + d));
  loadLightboxImage();
}

function loadLightboxImage() {
  const entry = FILTERED[lbIdx];
  if (!entry) return;

  // Update info
  document.getElementById('lb-title').textContent =
    `${entry.symbol}  ·  ${entry.datetime_str}  ·  ${entry.market}${entry.tag ? '  ·  '+entry.tag : ''}`;
  document.getElementById('lb-counter').textContent = `${lbIdx+1} / ${FILTERED.length}`;

  // Note + mark
  const n = NOTES[entry.path] || {};
  document.getElementById('lb-note-input').value  = n.note || '';
  document.getElementById('lb-mark-sel').value    = n.trade_mark || '';

  // Load image — set canvas size once loaded
  const img = document.getElementById('lb-img-base');
  img.onload = () => {
    canvas.width  = img.naturalWidth;
    canvas.height = img.naturalHeight;
    canvas.style.width  = img.naturalWidth  + 'px';
    canvas.style.height = img.naturalHeight + 'px';
    img.style.width   = img.naturalWidth  + 'px';
    img.style.height  = img.naturalHeight + 'px';

    // Restore strokes for this image
    currentStrokes = strokeMap[entry.path] = strokeMap[entry.path] || [];
    _snapshots = [];
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    redrawCanvas();
    if (currentStrokes.length) _snapshots.push(ctx.getImageData(0, 0, canvas.width, canvas.height));

    // Fit to screen
    zoomFit();
  };
  img.src = absPath(entry.path, entry);
}

/* ═══════════════════════════════════════════════
   Save annotation (note + mark)
═══════════════════════════════════════════════ */
async function saveAnnotation() {
  const entry = FILTERED[lbIdx];
  if (!entry) return;
  const note  = document.getElementById('lb-note-input').value.trim();
  const mark  = document.getElementById('lb-mark-sel').value;
  NOTES[entry.path] = { path: entry.path, note, trade_mark: mark, updated_at: new Date().toISOString() };
  if (window.location.protocol === 'file:') {
    try { localStorage.setItem('tv_notes', JSON.stringify(NOTES)); } catch(_) {}
  } else {
    try {
      await fetch('/save_note', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ path: entry.path, note, trade_mark: mark }),
      });
    } catch(_) {}
  }
  applyFilters();
  // Brief visual feedback
  const btn = document.querySelector('.btn-save');
  const orig = btn.textContent; btn.textContent = '✔ Saved!'; btn.style.background = '#2ea043';
  setTimeout(() => { btn.textContent = orig; btn.style.background = ''; }, 1200);
}

/* ═══════════════════════════════════════════════
   Gallery data + filters (unchanged from original)
═══════════════════════════════════════════════ */
function absPath(rel, entry) {
  // Priority: 1) Google Drive public URL (must be non-empty string)
  //           2) GitHub Pages URL (must be non-empty string)
  //           3) file:// local (file: protocol)
  //           4) relative path (http/https server)
  const driveUrl = entry && entry.drive_public_url && entry.drive_public_url.trim();
  if (driveUrl) return driveUrl;
  const pagesUrl = typeof _GITHUB_PAGES_URL === 'string' && _GITHUB_PAGES_URL.trim();
  if (pagesUrl) return pagesUrl + '/screenshots/' + rel;
  if (window.location.protocol === 'file:') {
    const base = _ABS_SHOT_DIR.replace(/\\/g, '/').replace(/^\/*/, '');
    return 'file:///' + base + '/' + rel;
  }
  return '../screenshots/' + rel;
}

async function init() {
  try {
    const data = _INLINE_DATA;
    ALL = data.entries || [];
    populateFilters(data);
    updateStats(data);
    NOTES = Object.assign({}, _INLINE_NOTES);
    if (window.location.protocol === 'file:') {
      try { const st = localStorage.getItem('tv_notes'); if (st) NOTES = Object.assign({}, NOTES, JSON.parse(st)); } catch(_) {}
    }
    if (window.location.protocol !== 'file:') {
      try {
        const [dr, nr] = await Promise.allSettled([
          fetch('data.json').then(r => r.json()),
          fetch('notes.json').then(r => r.json()),
        ]);
        if (dr.status === 'fulfilled' && dr.value && dr.value.entries) {
          // Merge server entries with inline entries — deduplicate by path
          const serverEntries = dr.value.entries || [];
          const inlineEntries = data.entries || [];
          const pathSet = new Set(serverEntries.map(e => e.path));
          const merged = [...serverEntries];
          inlineEntries.forEach(e => { if (!pathSet.has(e.path)) merged.push(e); });
          ALL = merged;
          // Rebuild filter sets from merged data
          const mergedData = Object.assign({}, dr.value, {
            entries: merged,
            total_files: merged.length,
            symbols: [...new Set(merged.map(e => e.symbol))].sort(),
            markets: [...new Set(merged.map(e => e.market))].sort(),
            months:  [...new Set(merged.map(e => e.month))].sort(),
            tags:    [...new Set(merged.map(e => e.tag).filter(Boolean))].sort(),
            drives:  [...new Set(merged.map(e => e.drive_name).filter(Boolean))].sort(),
          });
          populateFilters(mergedData);
          updateStats(mergedData);
        }
        if (nr.status === 'fulfilled') NOTES = nr.value;
      } catch(_) {}
    }
  } catch(e) { console.error('Gallery init error:', e); }
  applyFilters();
}

function populateFilters(data) {
  fill('f-symbol', data.symbols || []);
  fill('f-market', data.markets || []);
  fill('f-month', data.months || []);
  fill('f-tag', data.tags || []);
  fill('f-drive', data.drives || []);
}
function fill(id, arr) {
  const sel = document.getElementById(id);
  const first = sel.options[0];
  sel.innerHTML = '';
  sel.appendChild(first);
  arr.forEach(v => { const o = document.createElement('option'); o.value = v; o.textContent = v; sel.appendChild(o); });
}
function updateStats(data) {
  document.getElementById('stats').innerHTML =
    `Total: <b style="color:var(--text)">${data.total_files}</b> images<br>
     Symbols: <b style="color:var(--text)">${data.symbols?.length||0}</b><br>
     Updated: ${data.generated_at?.slice(0,16)||'—'}<br><br>
     <a href="replay.html" style="color:var(--accent);font-size:12px;">Open Replay →</a>`;
}
function applyFilters() {
  const sym   = document.getElementById('f-symbol').value;
  const mkt   = document.getElementById('f-market').value;
  const mon   = document.getElementById('f-month').value;
  const tag   = document.getElementById('f-tag').value;
  const drv   = document.getElementById('f-drive').value;
  const mark  = document.getElementById('f-mark').value;
  const pin   = document.getElementById('f-pin').value;
  const q     = document.getElementById('search').value.toLowerCase().trim();
  const sort  = document.getElementById('sort-sel').value;
  FILTERED = ALL.filter(e => {
    if (sym  && e.symbol     !== sym) return false;
    if (mkt  && e.market     !== mkt) return false;
    if (mon  && e.month      !== mon) return false;
    if (tag  && e.tag        !== tag) return false;
    if (drv  && e.drive_name !== drv) return false;
    if (pin  && !e.is_pinned) return false;
    const n = NOTES[e.path];
    if (mark && (!n || n.trade_mark !== mark)) return false;
    if (q) { if (!(e.symbol+' '+e.date+' '+e.time+' '+e.tag+' '+(e.drive_name||'')).toLowerCase().includes(q)) return false; }
    return true;
  });
  const safeKey = e => (e.sort_key || e.date+'_'+e.time || '');
  const safeSym = e => (e.symbol || '');
  if (sort === 'newest') FILTERED.sort((a,b) => safeKey(b).localeCompare(safeKey(a)));
  else if (sort === 'oldest') FILTERED.sort((a,b) => safeKey(a).localeCompare(safeKey(b)));
  else FILTERED.sort((a,b) => safeSym(a).localeCompare(safeSym(b)) || safeKey(a).localeCompare(safeKey(b)));
  renderGrid();
}
function clearFilters() {
  ['f-symbol','f-market','f-month','f-tag','f-drive','f-mark','f-pin'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('search').value = '';
  applyFilters();
}
function renderGrid() {
  const grid  = document.getElementById('grid');
  const empty = document.getElementById('empty');
  document.getElementById('count').textContent = `${FILTERED.length} of ${ALL.length} images`;
  Array.from(grid.children).forEach(c => { if (c !== empty) c.remove(); });
  if (!FILTERED.length) { empty.style.display = 'block'; return; }
  empty.style.display = 'none';
  const frag = document.createDocumentFragment();
  FILTERED.forEach((e, i) => {
    const n    = NOTES[e.path];
    const mark = n?.trade_mark || '';
    const card = document.createElement('div');
    card.className = 'card' + (e.is_pinned ? ' pinned' : '');
    card.onclick   = () => openLightbox(i);
    card.innerHTML = `
      ${mark ? `<div class="mark-bar ${mark}"></div>` : ''}
      <img src="${absPath(e.path, e)}" alt="${e.symbol}" loading="lazy" onerror="this.style.background='#1c2128'"/>
      <div class="card-info">
        <div class="card-symbol">
          ${e.symbol}
          ${e.is_pinned ? '<span class="badge badge-pin">📌 PIN</span>' : ''}
          ${e.tag  ? `<span class="badge badge-tag">${e.tag}</span>` : ''}
          ${mark   ? `<span class="badge badge-${mark.toLowerCase()}">${mark}</span>` : ''}
        </div>
        <div class="card-date">${e.date} ${e.time.replace('-',':')} · ${e.market}${e.drive_name ? ' · '+e.drive_name : ''}</div>
        ${n?.note ? `<div style="font-size:11px;color:var(--muted);margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${n.note}</div>` : ''}
      </div>`;
    frag.appendChild(card);
  });
  grid.appendChild(frag);
}
function openReplay() { window.open('replay.html', '_blank'); }

/* ═══════════════════════════════════════════════
   Password gate
═══════════════════════════════════════════════ */
const _PW_HASH = '%%GALLERY_PASSWORD_HASH%%';  // SHA-256 hex, "" = no password

async function sha256hex(str) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2,'0')).join('');
}

async function checkPw() {
  const val = document.getElementById('pw-box').value;
  const hash = await sha256hex(val);
  if (hash === _PW_HASH) {
    sessionStorage.setItem('tv_auth', hash);
    document.getElementById('pw-screen').style.display = 'none';
  } else {
    document.getElementById('pw-err').style.display = 'block';
    document.getElementById('pw-box').value = '';
    document.getElementById('pw-box').focus();
  }
}

async function initAuth() {
  if (!_PW_HASH) {
    // No password configured — hide gate immediately
    document.getElementById('pw-screen').style.display = 'none';
    return true;
  }
  const saved = sessionStorage.getItem('tv_auth');
  if (saved === _PW_HASH) {
    document.getElementById('pw-screen').style.display = 'none';
    return true;
  }
  // Show gate — focus input
  document.getElementById('pw-box').focus();
  return false;
}

(async () => {
  await initAuth();
  init();
})();
</script>
</body>
</html>
"""

_REPLAY_HTML_TEMPLATE = r"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>TradingView AutoShot — Chart Replay</title>
<style>
:root {
  --bg: #0d1117; --panel: #161b22; --border: #30363d;
  --accent: #00c896; --text: #e6edf3; --muted: #8b949e;
  --bull: #2ea043; --bear: #da3633;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif;
        display: flex; flex-direction: column; height: 100vh; overflow: hidden; }

/* ── Header toolbar ── */
#header {
  background: var(--panel); border-bottom: 1px solid var(--border);
  padding: 10px 16px; display: flex; align-items: center; gap: 10px; flex-shrink: 0; flex-wrap: wrap;
}
#header h1 { font-size: 14px; color: var(--accent); margin-right: 8px; white-space: nowrap; }
select, input[type=date] {
  background: var(--bg); border: 1px solid var(--border); color: var(--text);
  padding: 6px 8px; border-radius: 6px; font-size: 13px;
}
select:focus, input:focus { outline: none; border-color: var(--accent); }
.btn {
  display: inline-flex; align-items: center; justify-content: center; gap: 5px;
  padding: 6px 14px; border-radius: 6px; font-size: 13px; cursor: pointer;
  border: none; font-weight: 500; transition: opacity 0.15s;
}
.btn:hover { opacity: 0.82; }
.btn-accent { background: var(--accent); color: #000; }
.btn-outline { background: transparent; border: 1px solid var(--border); color: var(--text); }
.btn-danger { background: transparent; border: 1px solid var(--bear); color: var(--bear); }
#frame-info { font-size: 12px; color: var(--muted); white-space: nowrap; }

/* ── Main image ── */
#viewport {
  flex: 1; display: flex; align-items: center; justify-content: center;
  background: #000; position: relative; overflow: hidden;
}
/* wrapper link — ขยายเต็ม viewport */
#replay-img-href {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  overflow: hidden;
}
/* Double-buffer: img-a / img-b วางซ้อนกัน absolute */
#viewport .replay-buf {
  position: absolute;
  max-width: 100%; max-height: 100%;
  object-fit: contain;
  user-select: none;
  display: block;
  top: 50%; left: 50%;
  transform: translate(-50%, -50%);
  transition: opacity 0.15s ease-in-out;
  will-change: opacity;
}
#buf-a { opacity: 1; z-index: 2; }
#buf-b { opacity: 0; z-index: 1; }
#overlay {
  position: absolute; top: 12px; left: 12px; background: rgba(0,0,0,0.65);
  border-radius: 6px; padding: 8px 12px; font-size: 13px; line-height: 1.7;
  pointer-events: none;
}
#no-img {
  position: absolute; color: var(--muted); font-size: 15px; text-align: center;
}

/* ── Timeline scrubber ── */
#scrubber-wrap {
  padding: 8px 16px 4px; background: var(--panel); border-top: 1px solid var(--border); flex-shrink: 0;
}
#scrubber { width: 100%; accent-color: var(--accent); cursor: pointer; }
#timeline-labels {
  display: flex; justify-content: space-between; font-size: 10px;
  color: var(--muted); margin-top: 2px; padding: 0 2px;
}

/* ── Controls ── */
#controls {
  background: var(--panel); border-top: 1px solid var(--border);
  padding: 10px 16px; display: flex; align-items: center; gap: 10px;
  flex-shrink: 0; flex-wrap: wrap;
}
#speed-label { font-size: 12px; color: var(--muted); white-space: nowrap; }
#speed-range { accent-color: var(--accent); width: 100px; cursor: pointer; }
#note-input {
  flex: 1; background: var(--bg); border: 1px solid var(--border); color: var(--text);
  padding: 6px 10px; border-radius: 6px; font-size: 13px; min-width: 140px;
}
#mark-sel {
  background: var(--bg); border: 1px solid var(--border); color: var(--text);
  padding: 6px; border-radius: 6px; font-size: 13px;
}

/* ── Loading spinner ── */
@keyframes spin { to { transform: translate(-50%,-50%) rotate(360deg); } }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

/* ── Password screen ── */
#pw-screen {
  position: fixed; inset: 0; background: var(--bg);
  display: flex; align-items: center; justify-content: center;
  z-index: 9999; flex-direction: column; gap: 16px;
}
#pw-screen h2 { font-size: 18px; color: var(--accent); font-weight: 500; }
#pw-screen p  { font-size: 13px; color: var(--muted); }
#pw-box {
  background: var(--panel); border: 1px solid var(--border);
  color: var(--text); padding: 10px 16px; border-radius: 8px;
  font-size: 15px; width: 260px; text-align: center; outline: none;
}
#pw-box:focus { border-color: var(--accent); }
#pw-btn {
  background: var(--accent); color: #000; border: none;
  padding: 10px 32px; border-radius: 8px; font-size: 14px;
  font-weight: 600; cursor: pointer; width: 260px;
}
#pw-btn:hover { opacity: 0.85; }
#pw-err { color: #da3633; font-size: 13px; display: none; }
</style>
</head>
<body>

<div id="pw-screen">
  <h2>🔒 Chart Replay</h2>
  <p>Enter password to continue</p>
  <input id="pw-box" type="password" placeholder="Password"
         onkeydown="if(event.key==='Enter')checkPw()"/>
  <button id="pw-btn" onclick="checkPw()">Unlock</button>
  <span id="pw-err">Incorrect password — try again</span>
</div>

<!-- Header -->
<div id="header">
  <h1>▶ Chart Replay</h1>

  <select id="sel-symbol" onchange="onSymbolChange()">
    <option value="">— Symbol —</option>
  </select>
  <select id="sel-market" onchange="applySource()">
    <option value="">All Markets</option>
    <option value="CRYPTO">CRYPTO</option>
    <option value="US">US</option>
  </select>
  <select id="sel-drive" onchange="applySource()">
    <option value="">All Drives</option>
  </select>
  <select id="sel-tag" onchange="applySource()">
    <option value="">All Tags</option>
  </select>

  <input type="date" id="jump-date" title="Jump to date"/>
  <button class="btn btn-outline" onclick="jumpToDate()">Jump</button>

  <button class="btn btn-outline" onclick="openGallery()">Gallery ↗</button>
  <span id="frame-info">—</span>
</div>

<!-- Viewport -->
<div id="viewport">
  <div id="no-img">Select a symbol to begin replay</div>
  <!-- Double-buffer: buf-a แสดงอยู่, buf-b โหลดถัดไป แล้ว swap -->
  <img id="buf-a" class="replay-buf" src="" alt="" style="display:none"/>
  <img id="buf-b" class="replay-buf" src="" alt="" style="display:none"/>
  <!-- loading spinner -->
  <div id="loading-ring" style="display:none;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
       width:40px;height:40px;border:3px solid #30363d;border-top-color:var(--accent);
       border-radius:50%;animation:spin 0.7s linear infinite;z-index:10;"></div>
  <div id="overlay" style="display:none">
    <div id="ov-symbol" style="font-weight:600;color:var(--accent)"></div>
    <div id="ov-datetime" style="color:var(--text)"></div>
    <div id="ov-tag" style="color:#888"></div>
    <div id="ov-note" style="color:var(--note);margin-top:3px"></div>
  </div>
</div>

<!-- Scrubber -->
<div id="scrubber-wrap">
  <input type="range" id="scrubber" min="0" max="0" value="0"
         oninput="onScrub()" onchange="onScrub()"/>
  <div id="timeline-labels">
    <span id="tl-start">—</span>
    <span id="tl-end">—</span>
  </div>
</div>

<!-- Controls -->
<div id="controls">
  <button class="btn btn-outline" onclick="stepFrame(-1)" title="Previous [←]">⏮ Prev</button>
  <button class="btn btn-accent" id="play-btn" onclick="togglePlay()" title="Play/Pause [Space]">▶ Play</button>
  <button class="btn btn-outline" onclick="stepFrame(1)" title="Next [→]">Next ⏭</button>

  <span id="speed-label">Speed: <b id="speed-val">1×</b></span>
  <input type="range" id="speed-range" min="0" max="5" value="2" step="1" oninput="onSpeedChange()"/>

  <input id="note-input" type="text" placeholder="Note for this frame…"/>
  <select id="mark-sel">
    <option value="">No mark</option>
    <option value="BULL">🟢 Bull</option>
    <option value="BEAR">🔴 Bear</option>
    <option value="NOTE">📝 Note</option>
  </select>
  <button class="btn btn-outline" onclick="saveFrame()">Save Note</button>
</div>

<script>
const SPEEDS = [0.5, 1, 2, 4, 8, 16];  // index maps to range value 0-5
const DEFAULT_SPEED_IDX = 1;            // 1× default
const PRELOAD_AHEAD   = 3;              // โหลดล่วงหน้า N ภาพ
const PRELOAD_BEHIND  = 1;              // เก็บ cache ย้อนหลัง N ภาพ
const GDRIVE_SZ       = 's1600-rw-v1'; // ขนาด thumbnail ที่ใช้แสดง

// ── Inline data (embedded at build time — works with file://) ──────
const _INLINE_DATA      = %%INLINE_DATA%%;
const _INLINE_NOTES     = %%INLINE_NOTES%%;
const _ABS_SHOT_DIR     = '%%ABS_SCREENSHOT_FOLDER%%';
const _GITHUB_PAGES_URL = '%%GITHUB_PAGES_URL%%';  // "" = disabled

let ALL    = [];
let NOTES  = {};
let source = [];
let idx    = 0;
let playing  = false;
let timer    = null;
let speedIdx = DEFAULT_SPEED_IDX;

// Double-buffer state
// activeBuf = 'a' | 'b'  — buffer ที่กำลังแสดงอยู่
let activeBuf = 'a';
const bufEl = {
  a: document.getElementById('buf-a'),
  b: document.getElementById('buf-b'),
};

// Preload cache: Map<absUrl, HTMLImageElement>
const imgCache = new Map();

// ── URL helper ─────────────────────────────────────────────────────
function getAbsUrl(e) {
  const driveUrl = e.drive_public_url && e.drive_public_url.trim();
  if (driveUrl) {
    // ปรับ sz parameter ให้ใช้ขนาดที่ต้องการ (s0-v1 → sz ที่กำหนด)
    return driveUrl.replace(/sz=s\d+-[a-z0-9]+-v1|sz=s0-v1/i, 'sz=' + GDRIVE_SZ);
  }
  if (typeof _GITHUB_PAGES_URL === 'string' && _GITHUB_PAGES_URL.trim()) {
    return _GITHUB_PAGES_URL.trim() + '/screenshots/' + e.path;
  }
  if (window.location.protocol === 'file:') {
    const base = _ABS_SHOT_DIR.replace(/\\/g, '/').replace(/^\/*/, '');
    return 'file:///' + base + '/' + e.path;
  }
  return '../screenshots/' + e.path;
}

// ── Preload engine ─────────────────────────────────────────────────
function preloadAround(centerIdx) {
  if (!source.length) return;

  const keep = new Set();
  const lo = Math.max(0, centerIdx - PRELOAD_BEHIND);
  const hi = Math.min(source.length - 1, centerIdx + PRELOAD_AHEAD);

  for (let i = lo; i <= hi; i++) {
    const url = getAbsUrl(source[i]);
    keep.add(url);
    if (!imgCache.has(url)) {
      const img = new Image();
      img.src = url;
      imgCache.set(url, img);
    }
  }

  // Evict entries outside the keep-window to limit memory
  for (const [url] of imgCache) {
    if (!keep.has(url)) imgCache.delete(url);
  }
}

// ── Double-buffer swap ─────────────────────────────────────────────
// แสดงภาพที่ absUrl บน buffer ถัดไป แล้ว cross-fade
// resolve() เมื่อภาพ onload (หรือ timeout 3 วินาที)
function swapToUrl(absUrl) {
  return new Promise(resolve => {
    const next = activeBuf === 'a' ? 'b' : 'a';
    const cur  = activeBuf;
    const nextEl = bufEl[next];
    const curEl  = bufEl[cur];

    const spinner = document.getElementById('loading-ring');

    // ถ้าภาพอยู่ใน cache และโหลดเสร็จแล้ว — swap ทันที ไม่ต้องรอ
    const cached = imgCache.get(absUrl);
    const alreadyLoaded = cached && cached.complete && cached.naturalWidth > 0;

    let resolved = false;
    const done = () => {
      if (resolved) return;
      resolved = true;
      spinner.style.display = 'none';
      // cross-fade: next fade-in, cur fade-out
      nextEl.style.opacity = '1';
      nextEl.style.zIndex  = '2';
      curEl.style.opacity  = '0';
      curEl.style.zIndex   = '1';
      activeBuf = next;
      resolve();
    };

    // Timeout guard — ถ้าโหลดนานเกิน 3s ให้ swap ทันทีไม่ต้องรอ
    const guard = setTimeout(done, 3000);

    nextEl.onload  = () => { clearTimeout(guard); done(); };
    nextEl.onerror = () => { clearTimeout(guard); done(); };
    nextEl.style.display = '';
    nextEl.src = absUrl;

    if (alreadyLoaded) {
      // ภาพพร้อมแล้ว — swap ทันที (ผ่าน microtask เพื่อให้ browser paint)
      clearTimeout(guard);
      nextEl.onload = nextEl.onerror = null;
      Promise.resolve().then(done);
    } else {
      // แสดง spinner เฉพาะเมื่อต้องรอโหลดจริง
      spinner.style.display = '';
    }

    curEl.style.display = '';
  });
}

// ── Init ──────────────────────────────────────────────────────────
async function init() {
  try {
    ALL   = _INLINE_DATA.entries || [];
    fillSymbols(_INLINE_DATA.symbols || []);
    fillTags(_INLINE_DATA.tags || []);
    fillDrives(_INLINE_DATA.drives || []);
    NOTES = Object.assign({}, _INLINE_NOTES);

    if (window.location.protocol === 'file:') {
      try {
        const stored = localStorage.getItem('tv_notes');
        if (stored) NOTES = Object.assign({}, NOTES, JSON.parse(stored));
      } catch(_) {}
    }

    if (window.location.protocol !== 'file:') {
      try {
        const [dr, nr] = await Promise.allSettled([
          fetch('data.json').then(r => r.json()),
          fetch('notes.json').then(r => r.json()),
        ]);
        if (dr.status === 'fulfilled' && dr.value && dr.value.entries) {
          const serverEntries = dr.value.entries || [];
          const inlineEntries = _INLINE_DATA.entries || [];
          const pathSet = new Set(serverEntries.map(e => e.path));
          const merged  = [...serverEntries];
          inlineEntries.forEach(e => { if (!pathSet.has(e.path)) merged.push(e); });
          ALL = merged;
          const allSyms = [...new Set(merged.map(e => e.symbol))].sort();
          const allTags = [...new Set(merged.map(e => e.tag).filter(Boolean))].sort();
          const allDrvs = [...new Set(merged.map(e => e.drive_name).filter(Boolean))].sort();
          fillSymbols(allSyms);
          fillTags(allTags);
          fillDrives(allDrvs);
        }
        if (nr.status === 'fulfilled') NOTES = nr.value;
      } catch(_) {}
    }
  } catch(e) { console.error(e); }
}

function fillSymbols(syms) {
  const sel = document.getElementById('sel-symbol');
  const first = sel.options[0];
  sel.innerHTML = '';
  sel.appendChild(first);
  syms.forEach(s => {
    const o = document.createElement('option');
    o.value = s; o.textContent = s;
    sel.appendChild(o);
  });
}

function fillTags(tags) {
  const sel = document.getElementById('sel-tag');
  const first = sel.options[0];
  sel.innerHTML = '';
  sel.appendChild(first);
  tags.forEach(t => {
    const o = document.createElement('option');
    o.value = t; o.textContent = t;
    sel.appendChild(o);
  });
}

function fillDrives(drives) {
  const sel = document.getElementById('sel-drive');
  const first = sel.options[0];
  sel.innerHTML = '';
  sel.appendChild(first);
  (drives || []).forEach(d => {
    const o = document.createElement('option');
    o.value = d; o.textContent = d;
    sel.appendChild(o);
  });
}

// ── Source / filters ──────────────────────────────────────────────
function onSymbolChange() { applySource(); }

function applySource() {
  const sym = document.getElementById('sel-symbol').value;
  const mkt = document.getElementById('sel-market').value;
  const drv = document.getElementById('sel-drive').value;
  const tag = document.getElementById('sel-tag').value;

  clearTimeout(timer);
  playing = false;
  document.getElementById('play-btn').textContent = '▶ Play';

  if (!sym) {
    source = [];
    imgCache.clear();
    renderFrame();
    return;
  }

  source = ALL.filter(e =>
    e.symbol === sym &&
    (!mkt || e.market === mkt) &&
    (!drv || e.drive_name === drv) &&
    (!tag || e.tag === tag)
  ).sort((a, b) => (a.sort_key || '').localeCompare(b.sort_key || ''));

  idx = 0;
  imgCache.clear();
  const scrub = document.getElementById('scrubber');
  scrub.max   = Math.max(0, source.length - 1);
  scrub.value = 0;
  updateTimeline();
  renderFrame();
}

function updateTimeline() {
  const tls = document.getElementById('tl-start');
  const tle = document.getElementById('tl-end');
  if (source.length) {
    tls.textContent = source[0].date;
    tle.textContent = source[source.length - 1].date;
  } else {
    tls.textContent = tle.textContent = '—';
  }
}

// ── Playback — drift-corrected scheduler ──────────────────────────
function togglePlay() {
  playing = !playing;
  document.getElementById('play-btn').textContent = playing ? '⏸ Pause' : '▶ Play';
  if (playing) advanceFrame();
  else clearTimeout(timer);
}

// advanceFrame: advance idx → renderFrame → wait remaining time → repeat
// ใช้ Date.now() วัดเวลาจริง เพื่อชดเชย network latency
async function advanceFrame() {
  if (!playing) return;
  if (idx >= source.length - 1) {
    playing = false;
    document.getElementById('play-btn').textContent = '▶ Play';
    return;
  }

  const targetMs = 1000 / SPEEDS[speedIdx];  // เวลาที่ควรใช้ต่อ frame
  const t0 = Date.now();

  idx++;
  syncScrubber();
  await renderFrame();                        // รอภาพโหลดเสร็จ (หรือ timeout)

  if (!playing) return;                       // ถูก pause ระหว่างรอ

  const elapsed = Date.now() - t0;
  const wait    = Math.max(0, targetMs - elapsed);  // ชดเชยเวลาที่ใช้ไปแล้ว

  timer = setTimeout(advanceFrame, wait);
}

function stepFrame(d) {
  clearTimeout(timer);
  playing = false;
  document.getElementById('play-btn').textContent = '▶ Play';
  idx = Math.max(0, Math.min(source.length - 1, idx + d));
  syncScrubber();
  renderFrame();
}

function onScrub() {
  idx = parseInt(document.getElementById('scrubber').value, 10);
  renderFrame();
}

function syncScrubber() {
  document.getElementById('scrubber').value = idx;
}

function onSpeedChange() {
  speedIdx = parseInt(document.getElementById('speed-range').value, 10);
  document.getElementById('speed-val').textContent = SPEEDS[speedIdx] + '×';
}

function jumpToDate() {
  const d = document.getElementById('jump-date').value;
  if (!d || !source.length) return;
  const i = source.findIndex(e => e.date >= d);
  if (i >= 0) { idx = i; syncScrubber(); renderFrame(); }
}

// ── Render (async — รอ double-buffer swap) ─────────────────────────
async function renderFrame() {
  const noImg   = document.getElementById('no-img');
  const overlay = document.getElementById('overlay');

  if (!source.length) {
    bufEl.a.style.display = bufEl.b.style.display = 'none';
    noImg.style.display   = '';
    overlay.style.display = 'none';
    document.getElementById('frame-info').textContent = '—';
    return;
  }

  noImg.style.display = 'none';

  const e      = source[idx];
  const absUrl = getAbsUrl(e);

  // เริ่ม preload ภาพรอบข้างทันที (non-blocking)
  preloadAround(idx);

  // swap buffer → cross-fade
  await swapToUrl(absUrl);

  // อัปเดต overlay + UI
  overlay.style.display = '';
  document.getElementById('ov-symbol').textContent   = e.symbol + ' · ' + e.market;
  document.getElementById('ov-datetime').textContent = e.datetime_str;
  document.getElementById('ov-tag').textContent      = e.tag || '';

  const n = NOTES[e.path];
  document.getElementById('ov-note').textContent     = n?.note ? '📝 ' + n.note : '';
  document.getElementById('note-input').value        = n?.note || '';
  document.getElementById('mark-sel').value          = n?.trade_mark || '';

  document.getElementById('frame-info').textContent =
    'Frame ' + (idx + 1) + ' / ' + source.length + ' — ' + e.datetime_str;
}

// ── Annotation ────────────────────────────────────────────────────
async function saveFrame() {
  if (!source.length) return;
  const e    = source[idx];
  const note = document.getElementById('note-input').value.trim();
  const mark = document.getElementById('mark-sel').value;
  NOTES[e.path] = { path: e.path, note, trade_mark: mark, updated_at: new Date().toISOString() };
  if (window.location.protocol === 'file:') {
    try { localStorage.setItem('tv_notes', JSON.stringify(NOTES)); } catch(_) {}
  } else {
    try {
      await fetch('/save_note', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: e.path, note, trade_mark: mark }),
      });
    } catch(_) {}
  }
  renderFrame();
}

function openGallery() { window.open('index.html', '_blank'); }

// ── Keyboard ──────────────────────────────────────────────────────
document.addEventListener('keydown', e => {
  if (e.target.tagName === 'INPUT') return;
  if (e.key === 'ArrowLeft')  stepFrame(-1);
  if (e.key === 'ArrowRight') stepFrame(1);
  if (e.key === ' ') { e.preventDefault(); togglePlay(); }
});

// ── Speed init ────────────────────────────────────────────────────
document.getElementById('speed-range').value = DEFAULT_SPEED_IDX;
document.getElementById('speed-val').textContent = SPEEDS[DEFAULT_SPEED_IDX] + '×';

/* ═══════════════════════════════════════════════
   Password gate
═══════════════════════════════════════════════ */
const _PW_HASH = '%%GALLERY_PASSWORD_HASH%%';

async function sha256hex(str) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function checkPw() {
  const val  = document.getElementById('pw-box').value;
  const hash = await sha256hex(val);
  if (hash === _PW_HASH) {
    sessionStorage.setItem('tv_auth', hash);
    document.getElementById('pw-screen').style.display = 'none';
  } else {
    document.getElementById('pw-err').style.display = 'block';
    document.getElementById('pw-box').value = '';
    document.getElementById('pw-box').focus();
  }
}

(async () => {
  if (!_PW_HASH) {
    document.getElementById('pw-screen').style.display = 'none';
  } else {
    const saved = sessionStorage.getItem('tv_auth');
    if (saved === _PW_HASH) {
      document.getElementById('pw-screen').style.display = 'none';
    } else {
      document.getElementById('pw-box').focus();
      return;
    }
  }
  init();
})();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Annotation HTTP server (optional — enables Save Note from browser)
# ---------------------------------------------------------------------------

class AnnotationServer:
    """
    Tiny HTTP server so gallery HTML can POST /save_note back to Python.

    Usage:
        server = AnnotationServer(gallery_manager)
        server.start(port=8765)
        # opens gallery in browser — notes sync back via HTTP
        server.stop()
    """

    def __init__(self, gallery: GalleryManager, port: int = 8765) -> None:
        self._gallery = gallery
        self._port = port
        self._server: Optional[object] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start server in background thread."""
        from http.server import BaseHTTPRequestHandler, HTTPServer

        gallery = self._gallery

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args) -> None:  # type: ignore[override]
                logger.debug("AnnotationServer: " + fmt, *args)

            def do_GET(self) -> None:  # noqa: N802
                """Serve gallery folder files."""
                path = self.path.split("?")[0].lstrip("/") or GALLERY_HTML
                file_path = gallery.gallery_folder / path
                if not file_path.exists():
                    self.send_response(404); self.end_headers(); return
                try:
                    data = file_path.read_bytes()
                    ct = "text/html" if path.endswith(".html") else \
                         "application/json" if path.endswith(".json") else \
                         "image/png" if path.endswith(".png") else "application/octet-stream"
                    self.send_response(200)
                    self.send_header("Content-Type", ct)
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except Exception as exc:
                    logger.debug("GET %s failed: %s", path, exc)
                    self.send_response(500); self.end_headers()

            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/save_note":
                    self.send_response(404); self.end_headers(); return
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length))
                    gallery.save_annotation(
                        rel_path=body.get("path", ""),
                        note=body.get("note", ""),
                        trade_mark=body.get("trade_mark", ""),
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"ok":true}')
                except Exception as exc:
                    logger.error("POST /save_note error: %s", exc)
                    self.send_response(500); self.end_headers()

        self._server = HTTPServer(("localhost", self._port), _Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,  # type: ignore[union-attr]
            daemon=True,
            name="AnnotationServer",
        )
        self._thread.start()
        logger.info("AnnotationServer listening on http://localhost:%d", self._port)

    def stop(self) -> None:
        """Stop the HTTP server."""
        if self._server:
            self._server.shutdown()  # type: ignore[union-attr]
            logger.info("AnnotationServer stopped.")

    def open_in_browser(self) -> None:
        """Open gallery in default browser via local server."""
        import webbrowser
        webbrowser.open(f"http://localhost:{self._port}/{GALLERY_HTML}")


# ---------------------------------------------------------------------------
# Module-level factory
# ---------------------------------------------------------------------------

def create_gallery_manager(config_path: str = "./config.json") -> GalleryManager:
    """
    Load config.json and return a configured GalleryManager.

    Falls back to defaults if file is missing or malformed.
    """
    cfg: dict = {}
    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
        logger.info("GalleryManager config loaded from %s", config_path)
    except FileNotFoundError:
        logger.warning("Config not found at %s — using defaults", config_path)
    except json.JSONDecodeError as exc:
        logger.error("Config JSON error: %s — using defaults", exc)
    return GalleryManager(config=cfg)


# ---------------------------------------------------------------------------
# Self-test (run with: python module5_gallery.py)
# ---------------------------------------------------------------------------

def write_drive_sidecar(image_path: str, drive_name: str) -> bool:
    """
    Write a .drive sidecar file alongside a screenshot PNG so the gallery
    can display and filter by drive.

    Called by module1_core / module6_integration after rclone sync:

        write_drive_sidecar(result.filepath, assignment.drive.name)

    Parameters
    ----------
    image_path : absolute or relative path to the .png file
    drive_name : e.g. "Drive1"

    Returns
    -------
    True on success, False on failure.
    """
    try:
        sidecar = Path(image_path).with_suffix(".drive")
        sidecar.write_text(drive_name.strip(), encoding="utf-8")
        return True
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "write_drive_sidecar failed for %s: %s", image_path, exc
        )
        return False


def _run_self_test() -> None:
    """Smoke-test for GalleryManager."""
    import sys
    import tempfile

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    print("=" * 60)
    print("module5_gallery.py — Self Test")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        shot_dir = Path(tmpdir) / "screenshots"
        gallery_dir = Path(tmpdir) / "gallery"

        # Create fake screenshot files
        from PIL import Image

        test_shots = [
            ("CRYPTO", "BTCUSD", "2026-04-14", "00-00", "CRYPTO_FUNDING"),
            ("CRYPTO", "BTCUSD", "2026-04-14", "08-00", "CRYPTO_FUNDING"),
            ("CRYPTO", "BTCUSD", "2026-04-14", "10-00", ""),
            ("US",     "AAPL",   "2026-04-14", "09-30", "NY_OPEN"),
            ("US",     "AAPL",   "2026-04-14", "12-00", ""),
            ("US",     "AAPL",   "2026-04-14", "16-00", "NY_CLOSE"),
        ]

        for market, sym, date, time_, tag in test_shots:
            folder = shot_dir / market / sym / date[:7]
            folder.mkdir(parents=True, exist_ok=True)
            fname = f"{sym}_{date}_{time_}"
            if tag:
                fname += f"_{tag}"
            fname += ".png"
            img = Image.new("RGB", (400, 225), color=(20, 30, 50))
            img.save(str(folder / fname))

        cfg = {
            "screenshot_folder": str(shot_dir),
            "gallery_folder": str(gallery_dir),
            "video_fps": 2,
            "github_pages_url": "https://example.github.io/tv-shots",
            "drives": [
                {"name": "Drive1", "rclone_remote": "gdrive1:", "limit_gb": 9.5, "label": "CRYPTO"},
                {"name": "Drive2", "rclone_remote": "gdrive2:", "limit_gb": 9.5, "label": "US-A"},
            ],
        }
        gm = GalleryManager(config=cfg)

        # --- build ---
        index = gm.build()
        assert index.total_files == len(test_shots), \
            f"Expected {len(test_shots)} files, got {index.total_files}"
        assert "BTCUSD" in index.symbols
        assert "AAPL" in index.symbols
        assert (gallery_dir / GALLERY_HTML).exists()
        assert (gallery_dir / REPLAY_HTML).exists()
        assert (gallery_dir / DATA_FILE).exists()
        # Drives from config should appear in index even with no .drive sidecar files
        assert "Drive1" in index.drives and "Drive2" in index.drives,             f"Expected Drive1/Drive2 in index.drives, got: {index.drives}"
        print(f"[PASS] build(): {index.total_files} entries, HTML generated, drives={index.drives}")

        # --- data.json parseable ---
        data = json.loads((gallery_dir / DATA_FILE).read_text(encoding="utf-8"))
        assert data["total_files"] == len(test_shots)
        assert "drives" in data, "drives key missing from data.json"
        assert "Drive1" in data["drives"]
        print("[PASS] data.json: valid JSON, correct count, drives present")

        # --- GitHub Pages URL in HTML ---
        html_content = (gallery_dir / GALLERY_HTML).read_text(encoding="utf-8")
        assert "example.github.io" in html_content, "GitHub Pages URL not embedded in gallery HTML"
        print("[PASS] GitHub Pages URL embedded in gallery HTML")

        # --- annotation ---
        rel = index.entries[0].path
        ok = gm.save_annotation(rel, note="Test note", trade_mark="BULL")
        assert ok
        ann = gm.get_annotation(rel)
        assert ann is not None
        assert ann.note == "Test note"
        assert ann.trade_mark == "BULL"
        print(f"[PASS] save_annotation + get_annotation: '{ann.note}' mark={ann.trade_mark}")

        # --- delete annotation ---
        deleted = gm.delete_annotation(rel)
        assert deleted
        assert gm.get_annotation(rel) is None
        print("[PASS] delete_annotation works")

        # --- invalid trade_mark silently cleared ---
        gm.save_annotation(rel, note="x", trade_mark="INVALID")
        ann2 = gm.get_annotation(rel)
        assert ann2 is not None and ann2.trade_mark == ""
        print("[PASS] Invalid trade_mark cleared")

        # --- video export ---
        out_mp4 = gm.export_video("BTCUSD", fps=2)
        if out_mp4 is None:
            print("[SKIP] export_video: opencv not installed")
        else:
            assert out_mp4.exists() and out_mp4.suffix == ".mp4"
            print(f"[PASS] export_video: {out_mp4.name} ({out_mp4.stat().st_size // 1024} KB)")

        # --- write_drive_sidecar helper ---
        first_png = shot_dir / "CRYPTO" / "BTCUSD" / "2026-04" / "BTCUSD_2026-04-14_00-00_CRYPTO_FUNDING.png"
        if first_png.exists():
            ok_sidecar = write_drive_sidecar(str(first_png), "Drive1")
            assert ok_sidecar
            sidecar_file = first_png.with_suffix(".drive")
            assert sidecar_file.exists() and sidecar_file.read_text().strip() == "Drive1"
            print("[PASS] write_drive_sidecar: .drive file written correctly")
            # Rebuild gallery — drive1 should now appear in entries
            index2 = gm.build()
            e0 = next((e for e in index2.entries if e.symbol == "BTCUSD" and e.date == "2026-04-14" and e.time == "00-00"), None)
            assert e0 is not None and e0.drive_name == "Drive1", f"Expected drive_name=Drive1, got: {e0}"
            print(f"[PASS] drive_name read from .drive sidecar: {e0.drive_name}")

        # --- factory ---
        gm2 = create_gallery_manager("/nonexistent/config.json")
        assert isinstance(gm2, GalleryManager)
        print("[PASS] create_gallery_manager fallback to defaults")

    print("=" * 60)
    print("All tests passed ✅")
    print("=" * 60)


if __name__ == "__main__":
    _run_self_test()
