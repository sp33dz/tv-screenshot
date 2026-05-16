#!/usr/bin/env python3
"""
snapshot_run.py  —  On-demand TradingView screenshot
Fix: '/' opens Indicator search in TV, NOT symbol search.
Real symbol search: click the symbol ticker text in top bar directly.
"""
import os, json, sys, time
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
print(f"Session    : {'present' if tv_session else 'MISSING'}")

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
    oj = json.dumps(origins, ensure_ascii=False)
    return (
        "(function(){var origins=" + oj + ";"
        "origins.forEach(function(o){"
        "if(window.location.origin!==o.origin)return;"
        "(o.localStorage||[]).forEach(function(item){"
        "try{localStorage.setItem(item.name,item.value);}catch(e){}"
        "});});})();"
    )

# ── Dismiss ANY open dialog/popup ────────────────────────────
def dismiss_all(page, label=""):
    """Close any open dialog: press Escape, then click close buttons."""
    count = 0
    # Escape closes most TV dialogs
    for _ in range(3):
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(300)
        except Exception:
            pass

    # Also click explicit close buttons
    for sel in [
        'button[data-name="close"]',
        'button[aria-label="Close"]',
        'button[aria-label="close"]',
        '[class*="closeButton"]',
        '[class*="close-button"]',
        '[data-name="close-button"]',
    ]:
        try:
            btns = page.query_selector_all(sel)
            for btn in btns:
                if btn.is_visible():
                    btn.click(timeout=500)
                    count += 1
                    page.wait_for_timeout(200)
        except Exception:
            pass

    if count:
        print(f"  [{label}] closed {count} dialog(s)")
    return count

def wait_spinner_gone(page, max_sec=20):
    for _ in range(max_sec * 2):
        try:
            sp = page.query_selector('div[class*="spinner"]')
            if not sp or not sp.is_visible():
                return
        except Exception:
            return
        page.wait_for_timeout(500)

def wait_chart_ready(page, max_sec=25):
    try:
        page.wait_for_selector('div[class*="chart-container"]', timeout=max_sec*1000)
    except Exception:
        print("  chart-container timeout")
    wait_spinner_gone(page, max_sec=15)

# ── CORE FIX: Force symbol by clicking ticker in top bar ──────
def force_symbol(page, sym, exch, label):
    """
    Click the symbol/ticker text in TradingView's top toolbar to open
    the real Symbol Search dialog (not the indicator search).
    Then type the symbol and confirm with Enter.
    """
    search_query = f"{exch}:{sym}" if exch else sym
    print(f"  [{label}] Forcing symbol: {search_query}")

    # 1. Make sure no dialogs are open
    dismiss_all(page, label)
    page.wait_for_timeout(500)

    # 2. Click the symbol name in the top bar
    # TradingView renders the ticker in these selectors (try each)
    TICKER_SELECTORS = [
        # The main symbol display in the top bar
        '[data-name="legend-series-item"] .tv-screener-table__symbol',
        'div[class*="symbolNameInput"]',
        'div[class*="symbol-"] > div[class*="title"]',
        '[class*="symbolInput"]',
        # Header/toolbar area symbol
        'div[class*="pane-legend"] div[class*="mainTitle"]',
        'div[class*="header-chart-panel"] div[class*="symbol"]',
        # Simpler: any element containing the current symbol text in toolbar area
        'div[class*="toolbarContent"] div[class*="symbol"]',
        'div[class*="chart-toolbar"] span[class*="symbol"]',
        # TradingView 2024+ selectors
        '[data-name="header-toolbar-symbol-search"]',
        'button[id*="header-toolbar-symbol-search"]',
        'div[class*="tickerInput"]',
        'span[class*="tickerSymbol"]',
        # Fallback: the breadcrumb/title at top left of chart
        'div[class*="titleWrapper"] div[class*="title"]',
    ]

    clicked = False
    for sel in TICKER_SELECTORS:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click(timeout=2000)
                page.wait_for_timeout(800)
                # Check if symbol search opened (has an input)
                inp = page.query_selector('input[data-role="search"]') or \
                      page.query_selector('input[class*="search"]')
                if inp and inp.is_visible():
                    clicked = True
                    print(f"  [{label}] Opened symbol search via: {sel}")
                    break
                else:
                    # Might have opened wrong dialog, dismiss and try next
                    dismiss_all(page, label)
                    page.wait_for_timeout(300)
        except Exception:
            pass

    # 3. If click didn't open symbol search, try JS click on the toolbar area
    if not clicked:
        print(f"  [{label}] Selector click failed — trying JS approach")
        try:
            page.evaluate("""
                // Try to find and click the symbol search button
                var candidates = [
                    document.querySelector('[data-name="header-toolbar-symbol-search"]'),
                    document.querySelector('[class*="symbolInput"]'),
                    document.querySelector('[class*="tickerInput"]'),
                    document.querySelector('[class*="titleWrapper"]'),
                ];
                for(var i=0;i<candidates.length;i++){
                    if(candidates[i]){ candidates[i].click(); break; }
                }
            """)
            page.wait_for_timeout(1000)
            inp = page.query_selector('input[data-role="search"]') or \
                  page.query_selector('input[class*="search"]')
            if inp and inp.is_visible():
                clicked = True
                print(f"  [{label}] Opened via JS click")
        except Exception as e:
            print(f"  [{label}] JS click error: {e}")

    # 4. Type symbol into the search box
    if clicked:
        try:
            page.keyboard.press("Control+a")
            page.wait_for_timeout(100)
            page.keyboard.type(search_query, delay=80)
            page.wait_for_timeout(1500)
        except Exception as e:
            print(f"  [{label}] Type error: {e}")
    else:
        # Last resort: try keyboard type directly (maybe search is focused)
        print(f"  [{label}] Could not confirm dialog open — typing anyway")
        try:
            page.keyboard.type(search_query, delay=80)
            page.wait_for_timeout(1500)
        except Exception:
            pass

    # 5. Wait for results then press Enter
    for sel in ['div[class*="listItem"]','div[class*="search-item"]',
                'div[data-symbol-item]','li[class*="item-"]']:
        try:
            page.wait_for_selector(sel, timeout=3000)
            break
        except Exception:
            pass

    try:
        page.keyboard.press("Enter")
        page.wait_for_timeout(600)
        page.keyboard.press("Enter")  # confirm twice
    except Exception:
        pass

    # 6. Wait for chart to reload
    page.wait_for_timeout(3000)
    wait_spinner_gone(page, max_sec=15)
    dismiss_all(page, label)  # dismiss any remaining dialogs AFTER symbol set
    page.wait_for_timeout(1000)

    # 7. Verify
    try:
        title = page.title()
        verified = sym.upper() in title.upper()
        print(f"  [{label}] Title: {title!r}  verified={verified}")
        return verified
    except Exception:
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
print(f"Cookies: {len(cookies)}  Storage origins: {len(storage_origins)}")

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

    page = context.new_page()

    for idx, tf_key in enumerate(timeframes):
        tf_lbl   = TF_LABEL.get(tf_key, tf_key)
        out_path = out_dir / f"{symbol}_{tf_lbl}.png"
        url      = build_tv_url(symbol, exchange, tf_key, theme)

        print(f"\n{'─'*55}")
        print(f"  {symbol} [{tf_lbl}]  ({idx+1}/{len(timeframes)})")
        print(f"  URL: {url}")

        try:
            page.goto(url, timeout=40000, wait_until="domcontentloaded")
            wait_chart_ready(page, max_sec=25)
            page.wait_for_timeout(1500)
            dismiss_all(page, tf_lbl)

            # Force correct symbol every TF
            verified = force_symbol(page, symbol, exchange, tf_lbl)
            if not verified:
                print(f"  [{tf_lbl}] WARNING: symbol unverified")

            # Extra settle for indicators
            extra = max(0, wait_sec - 12)
            if extra > 0:
                page.wait_for_timeout(extra * 1000)

            # Final dismiss before screenshot
            dismiss_all(page, tf_lbl)
            page.wait_for_timeout(500)

            img_bytes = page.screenshot(full_page=False)
            out_path.write_bytes(img_bytes)
            print(f"  Saved: {out_path} ({len(img_bytes)//1024} KB)")

            results.append({"tf": tf_key, "label": tf_lbl,
                            "path": str(out_path), "success": True})

        except Exception as e:
            print(f"  ERROR [{tf_lbl}]: {e}")
            results.append({"tf": tf_key, "label": tf_lbl,
                            "path": "", "success": False, "error": str(e)})

    context.close()
    browser.close()
    print("\n=== Browser closed ===")

# ── Manifest ──────────────────────────────────────────────────
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
