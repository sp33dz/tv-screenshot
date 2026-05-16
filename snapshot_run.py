#!/usr/bin/env python3
"""
snapshot_run.py  —  On-demand TradingView screenshot
Root-cause fix: TradingView ignores ?symbol= URL param when a session exists.
Solution: navigate to chart, then FORCE symbol change via keyboard shortcut
          (same as user pressing '/' or clicking the symbol search box),
          type the symbol, confirm, wait for chart to reload correctly.
"""
import os
import json
import sys
import time
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
print(f"Session    : {'present' if tv_session else 'MISSING'}")

# ── TF maps ───────────────────────────────────────────────────
TF_MAP = {
    "1":"1","1m":"1","5":"5","5m":"5","15":"15","15m":"15",
    "30":"30","30m":"30","60":"60","1h":"60","1H":"60",
    "240":"240","4h":"240","4H":"240",
    "D":"D","d":"D","1D":"D","W":"W","w":"W","1W":"W",
}
TF_LABEL = {
    "1":"1M","5":"5M","15":"15M","30":"30M",
    "60":"1H","240":"4H","D":"D","W":"W",
}
# TF label → TradingView toolbar button text (for clicking)
TF_BTN = {
    "1":"1m","5":"5m","15":"15m","30":"30m",
    "60":"1h","240":"4h","D":"D","W":"W",
}

def base_tv_url(theme="dark"):
    """Base chart URL — no symbol in URL, let JS set it."""
    return f"https://www.tradingview.com/chart/?theme={theme}&style=1"

def build_tv_url(sym, exch, tf_key, theme="dark"):
    interval = TF_MAP.get(tf_key, tf_key)
    prefix   = f"{exch}:{sym}" if exch else sym
    ts       = int(time.time())
    return (
        f"https://www.tradingview.com/chart/"
        f"?symbol={prefix}&interval={interval}"
        f"&theme={theme}&style=1&save_image=false&_t={ts}"
    )

out_dir = Path("snapshots")
out_dir.mkdir(exist_ok=True)

# ── Session helpers ───────────────────────────────────────────
def parse_session(session_json):
    if not session_json or not session_json.strip():
        return [], []
    try:
        data = json.loads(session_json)
        return data.get("cookies", []), data.get("origins", [])
    except Exception as e:
        print(f"  [session] Parse error: {e}")
        return [], []

def make_storage_script(origins):
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
POPUP_SEL = [
    'button[data-name="close"]',
    'button[aria-label="Close"]',
    'button[aria-label="close"]',
    'div[class*="closeButton"]',
    'button[class*="close-button"]',
]

def dismiss_popups(page, label=""):
    dismissed = 0
    for sel in POPUP_SEL:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click(timeout=800)
                dismissed += 1
                page.wait_for_timeout(200)
        except Exception:
            pass
    return dismissed

# ── Wait for spinner to clear ────────────────────────────────
def wait_spinner_gone(page, max_sec=20):
    for _ in range(max_sec * 2):
        try:
            sp = page.query_selector('div[class*="spinner"]')
            if not sp or not sp.is_visible():
                return
        except Exception:
            return
        page.wait_for_timeout(500)

# ── CORE: Force symbol via keyboard search ────────────────────
def force_symbol(page, sym, exch, label):
    """
    Uses TradingView's built-in symbol search to force the correct symbol.
    Steps:
      1. Press '/' to open symbol search (universal TV shortcut)
      2. Clear and type the full symbol (with exchange prefix if given)
      3. Wait for dropdown, press Enter on first match
      4. Wait for chart to reload with new symbol
      5. Verify page title contains the correct symbol
    """
    search_query = f"{exch}:{sym}" if exch else sym

    print(f"  [{label}] Force symbol via keyboard search: {search_query}")

    # Close any open dialogs first
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)
    except Exception:
        pass

    # Open symbol search with '/'
    try:
        page.keyboard.press("/")
        page.wait_for_timeout(600)
    except Exception:
        pass

    # Check if search box opened — try alternative if not
    search_opened = False
    for sel in [
        'input[data-role="search"]',
        'input[placeholder*="Search"]',
        'input[placeholder*="symbol"]',
        'input[class*="search-"]',
        'div[class*="symbolSearch"] input',
        'div[data-name="symbol-search-items-dialog"] input',
    ]:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                search_opened = True
                break
        except Exception:
            pass

    if not search_opened:
        # Try clicking the symbol name in the top bar instead
        print(f"  [{label}] Keyboard shortcut failed — trying toolbar click")
        for sel in [
            'div[class*="symbolInfo"]',
            'div[class*="symbol-"]',
            'div[data-name="legend-series-item"] span',
            'div[class*="title-"] div[class*="symbol"]',
        ]:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    el.click(timeout=2000)
                    page.wait_for_timeout(600)
                    search_opened = True
                    break
            except Exception:
                pass

    # Type the symbol into whatever input is active
    try:
        # Select-all then type to replace any existing text
        page.keyboard.press("Control+a")
        page.wait_for_timeout(100)
        page.keyboard.type(search_query, delay=60)
        page.wait_for_timeout(1200)
    except Exception as e:
        print(f"  [{label}] Type error: {e}")

    # Wait for search results to appear
    result_appeared = False
    for sel in [
        'div[class*="listItem"]',
        'div[class*="search-item"]',
        'div[data-symbol-item]',
        'li[class*="item-"]',
    ]:
        try:
            page.wait_for_selector(sel, timeout=4000)
            result_appeared = True
            break
        except Exception:
            pass

    # Press Enter to select first result
    try:
        page.keyboard.press("Enter")
        page.wait_for_timeout(500)
        # Sometimes needs a second Enter
        page.keyboard.press("Enter")
    except Exception:
        pass

    # Wait for chart to reload
    page.wait_for_timeout(3000)
    wait_spinner_gone(page, max_sec=15)
    page.wait_for_timeout(1000)

    # Verify
    try:
        title = page.title().upper()
        verified = sym.upper() in title
        print(f"  [{label}] Title after symbol set: {page.title()!r}")
        print(f"  [{label}] Symbol verified: {verified}")
        return verified
    except Exception:
        return False


# ── Change TF via toolbar ─────────────────────────────────────
def set_timeframe(page, tf_key, label):
    """
    Click the correct TF button in the TradingView top toolbar.
    Falls back to keyboard shortcut if button not found.
    """
    tf_btn_text = TF_BTN.get(tf_key, tf_key)

    # Try clicking the TF button in toolbar
    for sel in [
        f'button[data-value="{TF_MAP.get(tf_key,tf_key)}"]',
        f'div[class*="toolbar"] button:has-text("{tf_btn_text}")',
        f'button[class*="button-"][aria-label*="{tf_btn_text}"]',
    ]:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click(timeout=2000)
                page.wait_for_timeout(1500)
                wait_spinner_gone(page, max_sec=10)
                print(f"  [{label}] TF set via toolbar click")
                return True
        except Exception:
            pass

    # Keyboard shortcut fallback (works for common TFs in TV)
    shortcuts = {"60":"Alt+1","240":"Alt+2","D":"Alt+3","W":"Alt+4"}
    sc = shortcuts.get(tf_key)
    if sc:
        try:
            page.keyboard.press(sc)
            page.wait_for_timeout(1500)
            wait_spinner_gone(page, max_sec=10)
            print(f"  [{label}] TF set via keyboard {sc}")
            return True
        except Exception:
            pass

    print(f"  [{label}] TF button not found — relying on URL interval")
    return False


# ── Main ──────────────────────────────────────────────────────
from playwright.sync_api import sync_playwright

results = []

launch_args = [
    "--window-size=1920,1080",
    "--disable-notifications",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-default-apps",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disk-cache-size=0",
    "--media-cache-size=0",
]

cookies, storage_origins = parse_session(tv_session)
storage_script = make_storage_script(storage_origins)

print(f"\nCookies : {len(cookies)}  |  Storage origins: {len(storage_origins)}")
print("=== Launching clean Chromium ===")

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True, args=launch_args)

    context = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )

    if cookies:
        try:
            context.add_cookies(cookies)
            print(f"Injected {len(cookies)} cookies")
        except Exception as e:
            print(f"Cookie warning: {e}")

    if storage_script:
        context.add_init_script(storage_script)
        print("localStorage init script registered")

    page = context.new_page()

    # ── Load chart once, then reuse page for all TFs ──────────
    first_url = build_tv_url(symbol, exchange, timeframes[0], theme)
    print(f"\nInitial navigation: {first_url}")
    page.goto(first_url, timeout=40000, wait_until="domcontentloaded")

    # Wait for chart to be ready
    try:
        page.wait_for_selector('div[class*="chart-container"]', timeout=25000)
    except Exception:
        print("chart-container timeout on initial load")

    page.wait_for_timeout(2000)
    dismiss_popups(page, "init")
    wait_spinner_gone(page, max_sec=20)
    page.wait_for_timeout(1000)

    # Force correct symbol (critical fix)
    verified = force_symbol(page, symbol, exchange, "init")
    if not verified:
        print("WARNING: Could not verify symbol after force — screenshots may be wrong")

    # ── Screenshot each TF ─────────────────────────────────────
    for idx, tf_key in enumerate(timeframes):
        tf_label_str = TF_LABEL.get(tf_key, tf_key)
        out_path     = out_dir / f"{symbol}_{tf_label_str}.png"

        print(f"\n{'─'*55}")
        print(f"  {symbol} [{tf_label_str}]  (TF {idx+1}/{len(timeframes)})")

        try:
            if idx == 0:
                # Already on first TF from initial URL — just verify and screenshot
                # Try to click the TF button to make sure
                set_timeframe(page, tf_key, tf_label_str)
            else:
                # Navigate to new TF URL to change interval
                tf_url = build_tv_url(symbol, exchange, tf_key, theme)
                print(f"  URL: {tf_url}")
                page.goto(tf_url, timeout=35000, wait_until="domcontentloaded")

                page.wait_for_timeout(1500)
                dismiss_popups(page, tf_label_str)
                wait_spinner_gone(page, max_sec=15)

                # Re-verify symbol on each TF navigate
                v = force_symbol(page, symbol, exchange, tf_label_str)
                if not v:
                    print(f"  [{tf_label_str}] Symbol re-verification failed")

            # Extra settle for indicators
            extra = max(0, wait_sec - 12)
            if extra > 0:
                page.wait_for_timeout(extra * 1000)

            dismiss_popups(page, tf_label_str)

            # Screenshot
            img_bytes = page.screenshot(full_page=False)
            out_path.write_bytes(img_bytes)
            size_kb = len(img_bytes) // 1024
            print(f"  Saved: {out_path} ({size_kb} KB)")

            results.append({
                "tf": tf_key, "label": tf_label_str,
                "path": str(out_path), "success": True,
            })

        except Exception as e:
            print(f"  ERROR [{tf_label_str}]: {e}")
            results.append({
                "tf": tf_key, "label": tf_label_str,
                "path": "", "success": False, "error": str(e),
            })

    context.close()
    browser.close()
    print("\n=== Browser closed ===")

# ── Manifest + summary ────────────────────────────────────────
manifest = {
    "symbol": symbol, "exchange": exchange,
    "timeframes": timeframes, "theme": theme,
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "results": results,
}
(out_dir / "manifest.json").write_text(
    json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
)

ok   = sum(1 for r in results if r["success"])
fail = len(results) - ok
print(f"\n{'='*55}")
print(f"DONE: {ok}/{len(results)} OK  |  {fail} failed")
for r in results:
    tag = "OK " if r["success"] else "ERR"
    print(f"  {tag}  {symbol}_{r['label']}  {r.get('path') or r.get('error','')}")

sys.exit(0 if fail == 0 else 1)
