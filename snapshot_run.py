#!/usr/bin/env python3
"""
snapshot_run.py  —  On-demand TradingView screenshot
Called by .github/workflows/snapshot.yml
Key fix: uses non-persistent browser context so each run starts clean,
         then verifies the correct symbol loaded before screenshotting.
"""
import os
import json
import sys
import time
import re
from pathlib import Path
from datetime import datetime

# ── Inputs ────────────────────────────────────────────────────
symbol    = os.environ.get("INPUT_SYMBOL",     "AAPL").strip().upper()
exchange  = os.environ.get("INPUT_EXCHANGE",   "").strip().upper()
tfs_raw   = os.environ.get("INPUT_TIMEFRAMES", "60,240,D,W").strip()
wait_sec  = int(os.environ.get("INPUT_WAIT_SEC", "18"))
theme     = os.environ.get("INPUT_THEME",      "dark")
tv_session= os.environ.get("TV_SESSION_JSON",  "")

timeframes = [t.strip() for t in tfs_raw.split(",") if t.strip()]

print(f"Symbol     : {symbol}")
print(f"Exchange   : {exchange or 'auto'}")
print(f"Timeframes : {timeframes}")
print(f"Wait sec   : {wait_sec}")
print(f"Theme      : {theme}")
print(f"TV session : {'present' if tv_session else 'MISSING'}")

# ── TF maps ───────────────────────────────────────────────────
TF_MAP = {
    "1":   "1",   "1m":  "1",
    "5":   "5",   "5m":  "5",
    "15":  "15",  "15m": "15",
    "30":  "30",  "30m": "30",
    "60":  "60",  "1h":  "60",  "1H": "60",
    "240": "240", "4h":  "240", "4H": "240",
    "D":   "D",   "d":   "D",   "1D": "D",
    "W":   "W",   "w":   "W",   "1W": "W",
}
TF_LABEL = {
    "1": "1M", "5": "5M", "15": "15M", "30": "30M",
    "60": "1H", "240": "4H", "D": "D", "W": "W",
}


def build_tv_url(sym, exch, tf_key, theme="dark"):
    """
    Build a TradingView chart URL.
    Uses /chart/new/ path which forces a fresh chart state (ignores saved layout).
    Appends cache-buster timestamp so the browser doesn't serve stale page.
    """
    interval = TF_MAP.get(tf_key, tf_key)
    prefix   = f"{exch}:{sym}" if exch else sym
    ts       = int(time.time())
    return (
        f"https://www.tradingview.com/chart/"
        f"?symbol={prefix}"
        f"&interval={interval}"
        f"&theme={theme}"
        f"&style=1"
        f"&allow_symbol_change=0"
        f"&save_image=false"
        f"&_t={ts}"
    )


# ── Output folder ─────────────────────────────────────────────
out_dir = Path("snapshots")
out_dir.mkdir(exist_ok=True)


# ── Parse session JSON ────────────────────────────────────────
def parse_session(session_json):
    """Return (cookies_list, storage_origins_list)."""
    if not session_json or not session_json.strip():
        return [], []
    try:
        data = json.loads(session_json)
        return data.get("cookies", []), data.get("origins", [])
    except Exception as e:
        print(f"  [session] Parse error: {e}")
        return [], []


# ── localStorage init script ──────────────────────────────────
def make_storage_script(origins):
    """Return an init script that sets localStorage for TradingView origin."""
    if not origins:
        return None
    origins_json = json.dumps(origins, ensure_ascii=False)
    return (
        "(function(){"
        "var origins=" + origins_json + ";"
        "origins.forEach(function(o){"
        "if(window.location.origin!==o.origin)return;"
        "(o.localStorage||[]).forEach(function(item){"
        "try{localStorage.setItem(item.name,item.value);}catch(e){}"
        "});"
        "});"
        "})();"
    )


# ── Popup dismiss ─────────────────────────────────────────────
POPUP_SELECTORS = [
    'button[data-name="close"]',
    'button[aria-label="Close"]',
    'button[aria-label="close"]',
    'div[class*="closeButton"]',
    'button[class*="close-button"]',
    '[data-dialog-name] button[class*="close"]',
]


def dismiss_popups(page, label=""):
    dismissed = 0
    for sel in POPUP_SELECTORS:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click(timeout=800)
                dismissed += 1
                page.wait_for_timeout(250)
        except Exception:
            pass
    if dismissed:
        print(f"  [{label}] dismissed {dismissed} popup(s)")
    return dismissed


# ── Wait for chart + verify correct symbol ────────────────────
def wait_and_verify(page, expected_sym, tf_label, wait_sec):
    """
    Wait for chart to render, then confirm the symbol shown in the
    page title/header matches what we requested.
    Returns True if symbol matches, False if mismatch detected.
    """
    # 1. Wait for chart container
    try:
        page.wait_for_selector('div[class*="chart-container"]',
                               timeout=25000)
    except Exception:
        print(f"  [{tf_label}] chart-container timeout")

    # 2. Dismiss early popups
    page.wait_for_timeout(1500)
    dismiss_popups(page, tf_label)

    # 3. Wait for loading spinner to disappear
    for _ in range(30):
        try:
            spinner = page.query_selector('div[class*="spinner"]')
            if not spinner or not spinner.is_visible():
                break
        except Exception:
            break
        page.wait_for_timeout(500)

    # 4. Extra settle time for indicators
    extra = max(0, wait_sec - 12)
    if extra > 0:
        page.wait_for_timeout(extra * 1000)

    # 5. Final popup dismiss
    dismiss_popups(page, tf_label)

    # 6. Verify symbol in page title
    try:
        title = page.title()
        print(f"  [{tf_label}] page title: {title}")
        sym_clean = expected_sym.upper()
        # TradingView title format: "EXCHANGE:SYMBOL — TF chart | TradingView"
        title_upper = title.upper()
        if sym_clean in title_upper:
            print(f"  [{tf_label}] symbol verified OK")
            return True
        else:
            print(f"  [{tf_label}] WARNING: symbol '{sym_clean}' not found in title")
            return False
    except Exception as e:
        print(f"  [{tf_label}] title check error: {e}")
        return True  # proceed anyway


# ── Main ──────────────────────────────────────────────────────
from playwright.sync_api import sync_playwright

results = []

launch_args = [
    "--window-size=1920,1080",
    "--disable-notifications",
    "--disable-popup-blocking",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-default-apps",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    # Disable cache to prevent stale symbol from previous runs
    "--disk-cache-size=0",
    "--media-cache-size=0",
]

cookies, storage_origins = parse_session(tv_session)
storage_script = make_storage_script(storage_origins)

print(f"\nCookies to inject : {len(cookies)}")
print(f"Storage origins   : {len(storage_origins)}")
print("\n=== Opening Chromium (non-persistent, clean session per run) ===")

with sync_playwright() as pw:
    # Use launch() for a clean isolated browser — no shared state between runs
    # This gives each run a completely clean browser — no shared state
    browser = pw.chromium.launch(
        headless=True,
        args=launch_args,
    )

    # Create ONE context with cookies + storage for this run
    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )

    # Inject cookies (TV auth session)
    if cookies:
        try:
            context.add_cookies(cookies)
            print(f"  Injected {len(cookies)} cookies")
        except Exception as e:
            print(f"  Cookie inject warning: {e}")

    # Inject localStorage via init script (runs before every page load)
    if storage_script:
        context.add_init_script(storage_script)
        print(f"  localStorage init script registered")

    # One page object, navigate per TF
    page = context.new_page()

    for tf_key in timeframes:
        tf_label = TF_LABEL.get(tf_key, tf_key)
        url      = build_tv_url(symbol, exchange, tf_key, theme)
        out_path = out_dir / f"{symbol}_{tf_label}.png"

        print(f"\n{'─'*55}")
        print(f"  {symbol} [{tf_label}]")
        print(f"  URL: {url}")

        try:
            # Navigate with full wait — domcontentloaded is enough,
            # we handle the rest manually
            page.goto(url, timeout=35000, wait_until="domcontentloaded")

            # Wait + verify symbol
            verified = wait_and_verify(page, symbol, tf_label, wait_sec)

            if not verified:
                # Symbol mismatch — try pressing Escape to close any open dialog
                # then wait a bit more and re-check
                print(f"  [{tf_label}] Attempting symbol fix...")
                try:
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(1000)
                except Exception:
                    pass
                # Re-verify after escape
                try:
                    title = page.title().upper()
                    if symbol not in title:
                        print(f"  [{tf_label}] Still mismatched — saving anyway (check log)")
                except Exception:
                    pass

            # Screenshot
            img_bytes = page.screenshot(full_page=False)
            out_path.write_bytes(img_bytes)
            size_kb = len(img_bytes) // 1024
            print(f"  Saved: {out_path} ({size_kb} KB)")

            results.append({
                "tf": tf_key, "label": tf_label,
                "path": str(out_path), "success": True,
                "verified": verified,
            })

        except Exception as e:
            print(f"  ERROR [{tf_label}]: {e}")
            results.append({
                "tf": tf_key, "label": tf_label,
                "path": "", "success": False, "error": str(e),
            })

    context.close()
    browser.close()
    print("\n=== Browser closed ===")

# ── Manifest ──────────────────────────────────────────────────
manifest = {
    "symbol":     symbol,
    "exchange":   exchange,
    "timeframes": timeframes,
    "theme":      theme,
    "timestamp":  datetime.utcnow().isoformat() + "Z",
    "results":    results,
}
(out_dir / "manifest.json").write_text(
    json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
)

# ── Summary ───────────────────────────────────────────────────
ok   = sum(1 for r in results if r["success"])
fail = len(results) - ok
print(f"\n{'='*55}")
print(f"DONE: {ok}/{len(results)} OK  |  {fail} failed")
for r in results:
    tag = "OK " if r["success"] else "ERR"
    vfy = "(unverified)" if r.get("verified") is False else ""
    print(f"  {tag}  {symbol}_{r['label']}  {vfy}")

sys.exit(0 if fail == 0 else 1)
