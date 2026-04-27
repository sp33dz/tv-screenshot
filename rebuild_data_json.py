"""
rebuild_data_json.py — Rebuild gallery/data.json from sidecar files only
========================================================================

Use case
--------
When PNG files are NOT present locally (e.g. GitHub Actions / CI runs where
screenshots live on Google Drive), this script reconstructs data.json by
walking every .driveurl sidecar file in the screenshot folder.

Each .driveurl file sits next to a (possibly absent) .png and contains the
Google Drive public direct URL for that image.  The optional .drive sidecar
contains the drive name ("Drive1", "Drive2", …).

No PNG files are downloaded.  No GalleryManager.build() scan is needed.
The result is a fully valid data.json that module5_gallery can consume.

Expected folder layout
----------------------
screenshots/
  MARKET/
    SYMBOL/
      YYYY-MM/
        SYMBOL_YYYY-MM-DD_HH-MM[_TAG][.PINNED].png      ← may be absent
        SYMBOL_YYYY-MM-DD_HH-MM[_TAG][.PINNED].driveurl  ← contains Drive URL
        SYMBOL_YYYY-MM-DD_HH-MM[_TAG][.PINNED].drive     ← contains "DriveN"

Usage
-----
# Basic (uses config.json for paths):
    python rebuild_data_json.py

# Explicit paths:
    python rebuild_data_json.py \\
        --screenshots ./screenshots \\
        --gallery     ./gallery \\
        --pages-url   https://user.github.io/repo

# Preview only (no write):
    python rebuild_data_json.py --dry-run

GitHub Actions step example:
    - name: Rebuild data.json from sidecars
      run: python rebuild_data_json.py
      env:
        GALLERY_PASSWORD: ${{ secrets.GALLERY_PASSWORD }}

Output
------
gallery/data.json       ← rebuilt index (atomic write)
gallery/data.json.bak   ← backup copy of previous data.json (if existed)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("rebuild_data_json")

# ---------------------------------------------------------------------------
# Constants (keep in sync with module5_gallery.py)
# ---------------------------------------------------------------------------

DEFAULT_SCREENSHOT_FOLDER = "./screenshots"
DEFAULT_GALLERY_FOLDER    = "./gallery"
DATA_FILE                 = "data.json"

_FNAME_RE = re.compile(
    r"^(?P<symbol>[A-Z0-9]+)_"
    r"(?P<date>\d{4}-\d{2}-\d{2})_"
    r"(?P<time>\d{2}-\d{2})"
    r"(?:_(?P<tag>[A-Z_]+))?"
    r"(?:\.PINNED)?\.png$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Minimal dataclasses (mirrors module5_gallery — no dependency needed)
# ---------------------------------------------------------------------------

@dataclass
class ImageEntry:
    path:             str
    abs_path:         str
    symbol:           str
    market:           str
    date:             str
    time:             str
    tag:              str
    is_pinned:        bool
    size_bytes:       int
    month:            str
    drive_name:       str = ""
    drive_public_url: str = ""

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
class GalleryIndex:
    entries:      List[ImageEntry] = field(default_factory=list)
    symbols:      List[str]        = field(default_factory=list)
    markets:      List[str]        = field(default_factory=list)
    months:       List[str]        = field(default_factory=list)
    tags:         List[str]        = field(default_factory=list)
    drives:       List[str]        = field(default_factory=list)
    total_files:  int              = 0
    generated_at: str              = ""

    def to_dict(self) -> dict:
        return {
            "entries":      [e.to_dict() for e in self.entries],
            "symbols":      self.symbols,
            "markets":      self.markets,
            "months":       self.months,
            "tags":         self.tags,
            "drives":       self.drives,
            "total_files":  self.total_files,
            "generated_at": self.generated_at,
        }


# ---------------------------------------------------------------------------
# Core rebuild logic
# ---------------------------------------------------------------------------

def _parse_driveurl_file(
    driveurl_path: Path,
    screenshot_folder: Path,
) -> Optional[ImageEntry]:
    """
    Parse a single .driveurl sidecar into an ImageEntry.

    The sidecar filename mirrors the PNG name:
        SYMBOL_YYYY-MM-DD_HH-MM[_TAG][.PINNED].driveurl

    Path structure (relative to screenshot_folder):
        MARKET/SYMBOL/YYYY-MM/filename.driveurl
    """
    # Reconstruct the virtual PNG filename from sidecar name
    png_name = driveurl_path.with_suffix(".png").name

    m = _FNAME_RE.match(png_name)
    if not m:
        logger.debug("Skipping unrecognised filename: %s", driveurl_path)
        return None

    # Market = grandparent folder (MARKET/SYMBOL/YYYY-MM/file)
    try:
        rel_parts = driveurl_path.relative_to(screenshot_folder).parts
    except ValueError:
        logger.debug("Skipping path outside screenshot_folder: %s", driveurl_path)
        return None

    market  = rel_parts[0].upper() if len(rel_parts) >= 3 else "UNKNOWN"
    symbol  = m.group("symbol").upper()
    date_s  = m.group("date")
    time_s  = m.group("time")
    tag     = (m.group("tag") or "").upper()
    is_pin  = ".PINNED." in png_name.upper()

    # Relative path as if the PNG existed (used as the unique key)
    rel_png = str(driveurl_path.relative_to(screenshot_folder)
                   .with_suffix(".png")).replace("\\", "/")

    # Read Google Drive public URL from .driveurl
    try:
        drive_public_url = driveurl_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        logger.warning("Cannot read %s: %s", driveurl_path, exc)
        drive_public_url = ""

    # Read drive name from optional .drive sidecar
    drive_name = ""
    drive_sidecar = driveurl_path.with_suffix(".drive")
    if drive_sidecar.exists():
        try:
            drive_name = drive_sidecar.read_text(encoding="utf-8").strip()
        except OSError:
            pass

    return ImageEntry(
        path             = rel_png,
        abs_path         = str(driveurl_path.with_suffix(".png")),
        symbol           = symbol,
        market           = market,
        date             = date_s,
        time             = time_s,
        tag              = tag,
        is_pinned        = is_pin,
        size_bytes       = 0,          # PNG not available locally
        month            = date_s[:7],
        drive_name       = drive_name,
        drive_public_url = drive_public_url,
    )


def rebuild_from_sidecars(
    screenshot_folder: Path,
    gallery_folder: Path,
    extra_drive_names: Optional[List[str]] = None,
    dry_run: bool = False,
) -> GalleryIndex:
    """
    Walk screenshot_folder for .driveurl files, build a complete GalleryIndex,
    and write gallery/data.json atomically.

    Parameters
    ----------
    screenshot_folder : root of screenshots tree
    gallery_folder    : where data.json is written
    extra_drive_names : drive names from config (shown in filter even if no entries)
    dry_run           : if True, print summary but do NOT write any files

    Returns
    -------
    The rebuilt GalleryIndex.
    """
    extra_drive_names = extra_drive_names or []

    if not screenshot_folder.exists():
        logger.error("Screenshot folder not found: %s", screenshot_folder)
        sys.exit(1)

    logger.info("Scanning sidecar files in: %s", screenshot_folder)

    entries: List[ImageEntry] = []
    skipped = 0

    for driveurl_file in sorted(screenshot_folder.rglob("*.driveurl")):
        entry = _parse_driveurl_file(driveurl_file, screenshot_folder)
        if entry:
            entries.append(entry)
        else:
            skipped += 1

    if not entries:
        logger.warning("No .driveurl sidecar files found — data.json will be empty.")

    # Sort: symbol then datetime
    entries.sort(key=lambda e: (e.symbol, e.sort_key))

    # Build metadata sets
    symbols:     List[str] = sorted({e.symbol for e in entries if e.symbol})
    markets:     List[str] = sorted({e.market for e in entries if e.market})
    months:      List[str] = sorted({e.month  for e in entries if e.month})
    tags:        List[str] = sorted({e.tag    for e in entries if e.tag})
    drives_seen: set       = {e.drive_name for e in entries if e.drive_name}

    def _drive_sort_key(d: str) -> Tuple:
        if d.startswith("Drive") and d[5:].isdigit():
            return (int(d[5:]), d)
        return (999, d)

    all_drives: List[str] = sorted(
        drives_seen | set(extra_drive_names),
        key=_drive_sort_key,
    )

    index = GalleryIndex(
        entries      = entries,
        symbols      = symbols,
        markets      = markets,
        months       = months,
        tags         = tags,
        drives       = all_drives,
        total_files  = len(entries),
        generated_at = datetime.utcnow().isoformat(timespec="seconds"),
    )

    logger.info(
        "Rebuilt index: %d entries | %d symbols | %d markets | drives=%s",
        index.total_files, len(symbols), len(markets), all_drives,
    )
    if skipped:
        logger.info("Skipped %d unrecognised sidecar filenames", skipped)

    if dry_run:
        logger.info("DRY RUN — no files written.")
        _print_summary(index)
        return index

    # ── Write data.json atomically ──────────────────────────────────────
    gallery_folder.mkdir(parents=True, exist_ok=True)
    (gallery_folder / "exports").mkdir(exist_ok=True)

    out_path    = gallery_folder / DATA_FILE
    tmp_path    = out_path.with_suffix(".json.tmp")
    backup_path = gallery_folder / (DATA_FILE + ".bak")

    # Backup existing data.json before overwriting
    if out_path.exists():
        try:
            backup_path.write_bytes(out_path.read_bytes())
            logger.info("Backed up existing data.json → data.json.bak")
        except OSError as exc:
            logger.warning("Could not write backup: %s", exc)

    payload = json.dumps(index.to_dict(), ensure_ascii=False, indent=2)
    try:
        tmp_path.write_text(payload, encoding="utf-8")
        tmp_path.replace(out_path)
        logger.info("data.json written → %s  (%d bytes)", out_path, len(payload))
    except Exception as exc:
        logger.error("Failed to write data.json: %s", exc)
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        sys.exit(1)

    _print_summary(index)
    return index


def _print_summary(index: GalleryIndex) -> None:
    print("\n" + "=" * 60)
    print(f"  Rebuild Summary")
    print("=" * 60)
    print(f"  Total entries : {index.total_files}")
    print(f"  Symbols       : {', '.join(index.symbols) or '—'}")
    print(f"  Markets       : {', '.join(index.markets) or '—'}")
    print(f"  Months        : {index.months[0] if index.months else '—'}"
          f" → {index.months[-1] if index.months else '—'}")
    print(f"  Tags          : {', '.join(index.tags) or '—'}")
    print(f"  Drives        : {', '.join(index.drives) or '—'}")
    print(f"  Generated at  : {index.generated_at}")
    print("=" * 60 + "\n")

    # Show per-symbol breakdown
    by_sym: Dict[str, int] = {}
    for e in index.entries:
        by_sym[e.symbol] = by_sym.get(e.symbol, 0) + 1
    for sym, cnt in sorted(by_sym.items()):
        print(f"    {sym:<12} {cnt:>5} entries")
    print()


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config(config_path: str = "./config.json") -> dict:
    try:
        with open(config_path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.info("config.json not found at %s — using CLI args / defaults", config_path)
    except json.JSONDecodeError as exc:
        logger.warning("config.json parse error: %s — using CLI args / defaults", exc)
    return {}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Rebuild gallery/data.json from .driveurl sidecar files (no PNG needed).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--screenshots",
        metavar="DIR",
        default=None,
        help="Screenshot folder root (overrides config.json)",
    )
    p.add_argument(
        "--gallery",
        metavar="DIR",
        default=None,
        help="Gallery output folder (overrides config.json)",
    )
    p.add_argument(
        "--config",
        metavar="FILE",
        default="./config.json",
        help="Path to config.json (default: ./config.json)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary without writing any files",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    cfg = _load_config(args.config)

    screenshot_folder = Path(
        args.screenshots
        or cfg.get("screenshot_folder", DEFAULT_SCREENSHOT_FOLDER)
    )
    gallery_folder = Path(
        args.gallery
        or cfg.get("gallery_folder", DEFAULT_GALLERY_FOLDER)
    )

    # Drive names from config (shown in filter dropdown even with no entries)
    raw_drives = cfg.get("drives", []) or []
    extra_drive_names = [
        str(d.get("name", "")).strip()
        for d in raw_drives
        if isinstance(d, dict) and d.get("name")
    ]

    logger.info("screenshot_folder : %s", screenshot_folder.resolve())
    logger.info("gallery_folder    : %s", gallery_folder.resolve())
    if extra_drive_names:
        logger.info("extra drives      : %s", extra_drive_names)

    rebuild_from_sidecars(
        screenshot_folder = screenshot_folder,
        gallery_folder    = gallery_folder,
        extra_drive_names = extra_drive_names,
        dry_run           = args.dry_run,
    )


if __name__ == "__main__":
    main()
