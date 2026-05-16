#!/usr/bin/env python3
"""
snapshot_run.py
On-demand TradingView screenshot script.
Called by .github/workflows/snapshot.yml
Reads inputs from environment variables, screenshots each TF, saves to snapshots/
"""
import os
import json
import sys
import time
from pathlib import Path
from datetime import datetime

# ── Read inputs from env ──────────────────────────────────────
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
print(f"TV session : {'present ✓' if tv_session else 'MISSING — running without login'}")

# ── TF → TradingView interval mapping ────────────────────────
TF_MAP = {
    "1":   "1",   "1m":  "1",
    "5":   "5",   "5m":  "5",
    "15":  "15",  "15m": "15",
    "30":  "30",  "30m": "30",
    "60":  "60",  "1h":  "60",  "1H":  "60",
    "240": "240", "4h":  "240", "4H":  "240",
    "D":   "D",   "d":   "D",   "1D":  "D",
    "W":   "W",   "w":   "W",   "1W":  "W",
}

TF_LABEL = {
    "1": "1M", "5": "5M", "15": "15M", "30": "30M",
    "60": "1H", "240": "4H", "D": "D", "W": "W",
}


def build_tv_url(sym, exch, tf_key, theme="dark"):
    interval = TF_MAP.get(tf_key, tf_key)
    prefix   = f"{exch}:{sym}" if exch else sym
    return (
        f"https://www.tradingview.com/chart/"
        f"?symbol={prefix}"
        f"&interval={interval}"
        f"&theme={theme}"
        f"&style=1"
        f"&hide_top_toolbar=0"
        f"&hide_side_toolbar=0"
        f"&allow_symbol_change=0"
        f"&save_image=false"
    )


# ── Output folder ─────────────────────────────────────────────
out_dir = Path("snapshots")
out_dir.mkdir(exist_ok=True)


# ── Inject TV session cookies + localStorage ──────────────────
def inject_session(context, session_json):
    if not session_json or not session_json.strip():
        print("  [session] TV_SESSION_JSON not set — no login")
        return False
    try:
        data    = json.loads(session_json)
        cookies = data.get("cookies", [])
        if cookies:
            context.add_cookies(cookies)
            print(f"  [session] Injected {len(cookies)} cookies")
        origins = data.get("origins", [])
        if origins:
            origins_json = json.dumps(origins, ensure_ascii=False)
            script = (
                "(function(){"
                "var origins=" + origins_json + ";"
                "origins.forEach(function(o){"
                "if(window.location.origin!==o.origin)return;"
                "(o.localStorage||[]).forEach(function(i){"
                "try{localStorage.setItem(i.name,i.value);}catch(e){}"
                "});"
                "});"
                "})();"
            )
            context.add_init_script(script)
            print(f"  [session] Injected localStorage for {len(origins)} origin(s)")
        return True
    except Exception as e:
        print(f"  [session] Warning: {e}")
        return False


# ── Popup dismiss ─────────────────────────────────────────────
POPUP_SELECTORS = [
    'button[data-name="close"]',
    'button[aria-label="Close"]',
    'button[aria-label="close"]',
    'div[class*="closeButton"]',
    'button[class*="close-button"]',
    'div[role="dialog"] button[class*="close"]',
    'button:has-text("x")',
    'button:has-text("X")',
]


def dismiss_popups(page, label=""):
    dismissed = 0
    for sel in POPUP_SELECTORS:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click(timeout=1000)
                dismissed += 1
                page.wait_for_timeout(300)
        except Exception:
            pass
    if dismissed:
        print(f"  [{label}] Dismissed {dismissed} popup(s)")
    return dismissed


def wait_for_chart(page, symbol, timeout_sec=25):
    try:
        page.wait_for_selector(
            'div[class*="chart-container"]',
            timeout=timeout_sec * 1000
        )
    except Exception:
        print(f"  [{symbol}] chart-container timeout — proceeding anyway")
    # Wait for spinner to disappear
    for _ in range(20):
        try:
            spinner = page.query_selector('div[class*="spinner"]')
            if not spinner or not spinner.is_visible():
                break
        except Exception:
            break
        page.wait_for_timeout(500)


# ── Main Playwright run ────────────────────────────────────────
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
]

print("\n=== Opening Chromium ===")

with sync_playwright() as pw:
    context = pw.chromium.launch_persistent_context(
        user_data_dir=f"/tmp/.pw_snap_{os.environ.get('GITHUB_RUN_ID', 'local')}",  # unique per run — no cross-run session conflict
        headless=True,
        args=launch_args,
        viewport={"width": 1920, "height": 1080},
        ignore_default_args=["--enable-automation"],
    )

    inject_session(context, tv_session)
    page = context.new_page()

    for tf_key in timeframes:
        tf_label = TF_LABEL.get(tf_key, tf_key)
        url      = build_tv_url(symbol, exchange, tf_key, theme)
        out_path = out_dir / f"{symbol}_{tf_label}.png"

        print(f"\n--- {symbol} [{tf_label}] ---")
        print(f"  URL: {url}")

        try:
            page.goto(url, timeout=30000, wait_until="domcontentloaded")

            # Early popup dismiss
            page.wait_for_timeout(1500)
            dismiss_popups(page, tf_label)

            # Wait for chart
            wait_for_chart(page, symbol, timeout_sec=wait_sec)

            # Extra settle time for indicators
            extra = max(0, wait_sec - 12)
            if extra > 0:
                page.wait_for_timeout(extra * 1000)

            # Final popup dismiss
            dismiss_popups(page, tf_label)

            # Screenshot
            img_bytes = page.screenshot(full_page=False)
            out_path.write_bytes(img_bytes)

            size_kb = len(img_bytes) // 1024
            print(f"  Saved: {out_path} ({size_kb} KB)")
            results.append({
                "tf":      tf_key,
                "label":   tf_label,
                "path":    str(out_path),
                "success": True
            })

        except Exception as e:
            print(f"  ERROR [{tf_label}]: {e}")
            results.append({
                "tf":      tf_key,
                "label":   tf_label,
                "path":    "",
                "success": False,
                "error":   str(e)
            })

    context.close()
    print("\n=== Browser closed ===")

# ── Write manifest ─────────────────────────────────────────────
manifest = {
    "symbol":     symbol,
    "exchange":   exchange,
    "timeframes": timeframes,
    "theme":      theme,
    "timestamp":  datetime.utcnow().isoformat() + "Z",
    "results":    results,
}
manifest_path = out_dir / "manifest.json"
manifest_path.write_text(
    json.dumps(manifest, indent=2, ensure_ascii=False),
    encoding="utf-8"
)
print(f"\nManifest written: {manifest_path}")

# ── Summary ───────────────────────────────────────────────────
ok   = sum(1 for r in results if r["success"])
fail = len(results) - ok
print(f"\n{'='*50}")
print(f"DONE: {ok}/{len(results)} OK  ({fail} failed)")
for r in results:
    status = "OK " if r["success"] else "ERR"
    info   = r.get("path") or r.get("error", "")
    print(f"  {status}  {symbol}_{r['label']}  {info}")

sys.exit(0 if fail == 0 else 1)
