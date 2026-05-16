#!/usr/bin/env python3
"""
write_status.py — Generates snapshots/STATUS_{SYMBOL}.json
Called by GitHub Actions after snapshot_run.py completes.

Reads environment variables set by the workflow:
  INPUT_SYMBOL, INPUT_TIMEFRAMES, INPUT_THEME, INPUT_RUN_TOKEN
  PAGES_BASE_URL  (e.g. https://username.github.io/repo-name)

Output file: snapshots/STATUS_{SYMBOL}.json
Format consumed by the HTML polling loop:
  {
    "status":    "done" | "error",
    "symbol":    "AAPL",
    "run_token": "AAPL_1716000000000",
    "theme":     "dark",
    "timeframes": ["60","240","D","W"],
    "images": [
      {"label": "1H",  "url": "https://...github.io/repo/snapshots/AAPL_1H.png"},
      ...
    ],
    "timestamp": "2025-05-16T10:00:00Z"
  }
"""
import os
import json
from pathlib import Path
from datetime import datetime

# ── Read env vars ──────────────────────────────────────────────
symbol     = os.environ.get("INPUT_SYMBOL",     "").strip().upper()
tfs_raw    = os.environ.get("INPUT_TIMEFRAMES", "60,240,D,W").strip()
theme      = os.environ.get("INPUT_THEME",      "dark").strip()
run_token  = os.environ.get("INPUT_RUN_TOKEN",  "").strip()
pages_url  = os.environ.get("PAGES_BASE_URL",   "").strip().rstrip("/")

if not symbol:
    print("ERROR: INPUT_SYMBOL is empty")
    raise SystemExit(1)

timeframes = [t.strip() for t in tfs_raw.split(",") if t.strip()]

snap_dir = Path("snapshots")
snap_dir.mkdir(exist_ok=True)

# ── Discover PNG files for this symbol ─────────────────────────
images = []
for fpath in sorted(snap_dir.glob(f"{symbol}_*.png")):
    label = fpath.stem[len(symbol) + 1:]  # e.g. "AAPL_1H.png" → "1H"
    if pages_url:
        url = f"{pages_url}/snapshots/{fpath.name}"
    else:
        url = f"snapshots/{fpath.name}"
    images.append({"label": label, "url": url, "file": fpath.name})

status = "done" if images else "error"

status_data = {
    "status":     status,
    "symbol":     symbol,
    "run_token":  run_token,
    "theme":      theme,
    "timeframes": timeframes,
    "images":     images,
    "timestamp":  datetime.utcnow().isoformat() + "Z",
}

out_path = snap_dir / f"STATUS_{symbol}.json"
out_path.write_text(
    json.dumps(status_data, indent=2, ensure_ascii=False),
    encoding="utf-8",
)

print(f"Wrote: {out_path}")
print(f"  status    = {status}")
print(f"  run_token = {run_token!r}")
print(f"  images    = {len(images)}")
for img in images:
    print(f"    {img['label']:>4}  {img['url']}")

if status == "error":
    print("WARNING: No PNG files found — STATUS will be 'error'")
    raise SystemExit(1)
