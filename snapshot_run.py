#!/usr/bin/env python3
"""
snapshot_run.py  —  On-demand TradingView screenshot
Strategy: Use TV's chart widget URL with explicit symbol/interval,
then verify symbol from DOM (not page title) and dismiss all dialogs robustly.
"""
import os, json, sys, time
from pathlib import Path
from datetime import datetime

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
TF_LABEL = {"1":"1M","5":"5M","15":"15M","30":"30M","60":"1H","240":"4H","D":"D","W":"W"}

def build_url(sym, exch, tf, theme):
    iv     = TF_MAP.get(tf, tf)
    prefix = f"{exch}:{sym}" if exch else sym
    ts     = int(time.time())
    return (
        f"https://www.tradingview.com/chart/"
        f"?symbol={prefix}&interval={iv}"
        f"&theme={theme}&style=1&save_image=false&_t={ts}"
    )

out_dir = Path("snapshots")
out_dir.mkdir(exist_ok=True)

def parse_session(sj):
    if not sj or not sj.strip():
        return [], []
    try:
        d = json.loads(sj)
        return d.get("cookies",[]), d.get("origins",[])
    except Exception as e:
        print(f"  session parse error: {e}")
        return [], []

def make_ls_script(origins):
    if not origins:
        return None
    oj = json.dumps(origins, ensure_ascii=False)
    return (
        "(function(){var O=" + oj + ";"
        "O.forEach(function(o){"
        "if(window.location.origin!==o.origin)return;"
        "(o.localStorage||[]).forEach(function(i){"
        "try{localStorage.setItem(i.name,i.value);}catch(e){}"
        "});});})();"
    )

# ─────────────────────────────────────────────────────────────
# Dismiss ALL open dialogs/popups robustly
# ─────────────────────────────────────────────────────────────
def dismiss_all(page, label=""):
    count = 0
    # 1. Press Escape 3 times
    for _ in range(3):
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass
    # 2. Click all visible close buttons
    CLOSE_SELS = [
        'button[data-name="close"]',
        '[data-name="close-button"]',
        'button[aria-label="Close"]',
        'button[aria-label="close"]',
        '[class*="closeButton"]',
        '[class*="close-button"]',
        '[class*="CloseButton"]',
        'div[class*="dialog"] button',
    ]
    for sel in CLOSE_SELS:
        try:
            for btn in page.query_selector_all(sel):
                if btn.is_visible():
                    btn.click(timeout=400)
                    count += 1
                    page.wait_for_timeout(150)
        except Exception:
            pass
    if count:
        print(f"  [{label}] closed {count} dialog(s)")
    return count

def wait_spinner(page, max_sec=20):
    for _ in range(max_sec * 2):
        try:
            sp = page.query_selector('div[class*="spinner"]')
            if not sp or not sp.is_visible():
                return
        except Exception:
            return
        page.wait_for_timeout(500)

def wait_chart(page, max_sec=25):
    try:
        page.wait_for_selector('div[class*="chart-container"]', timeout=max_sec*1000)
    except Exception:
        print("  chart-container timeout")
    wait_spinner(page, max_sec=15)

# ─────────────────────────────────────────────────────────────
# Verify correct symbol loaded by reading DOM — more reliable than title
# ─────────────────────────────────────────────────────────────
def verify_symbol_dom(page, sym, label):
    """Read the symbol shown in TV's chart header via DOM."""
    # Try several selectors TV uses for the current symbol display
    SYMBOL_SELS = [
        # Top bar: the symbol name text
        'div[class*="symbolInfo"] div[class*="symbol"]',
        'div[class*="titleWrapper"] div[class*="title"]',
        '[data-name="legend-series-item"] div[class*="title"]',
        # The input that shows current symbol
        'div[class*="tickerInput"]',
        'div[class*="symbolInput"]',
        # Fallback: read page title
    ]
    for sel in SYMBOL_SELS:
        try:
            el = page.query_selector(sel)
            if el:
                text = el.inner_text().strip().upper()
                if text:
                    found = sym.upper() in text
                    print(f"  [{label}] DOM symbol text: {text!r}  match={found}")
                    return found
        except Exception:
            pass

    # Fallback: page title
    try:
        title = page.title().upper()
        found = sym.upper() in title
        print(f"  [{label}] Title fallback: {title!r}  match={found}")
        return found
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────
# Force correct symbol via TV symbol search toolbar button
# ─────────────────────────────────────────────────────────────
def force_symbol(page, sym, exch, label):
    query = f"{exch}:{sym}" if exch else sym
    print(f"  [{label}] Forcing symbol: {query}")

    # Step 1: dismiss anything open
    dismiss_all(page, label)
    page.wait_for_timeout(400)

    # Step 2: click the symbol search button in the toolbar
    # TradingView's header toolbar has a dedicated symbol search button
    opened = False
    TOOLBAR_SELS = [
        '#header-toolbar-symbol-search',
        '[id="header-toolbar-symbol-search"]',
        '[data-name="header-toolbar-symbol-search"]',
        'button[id*="symbol-search"]',
        # The clickable symbol name text in the top-left
        'div[class*="chart-widget"] div[class*="title"] span',
        'div[class*="mainTitle"]',
        'div[class*="symbolTitle"]',
        'div[class*="symbol-title"]',
        'div[class*="title-TVBgYZEO"]',     # TV class (may change)
        # The "W" workspace button area — skip
        # Broader fallback: any element whose text contains current symbol
    ]
    for sel in TOOLBAR_SELS:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click(timeout=1500)
                page.wait_for_timeout(800)
                # Check if symbol search input appeared
                inp = (page.query_selector('input[data-role="search"]') or
                       page.query_selector('input[placeholder*="earch"]') or
                       page.query_selector('div[class*="symbolSearch"] input') or
                       page.query_selector('div[class*="search-"] input'))
                if inp and inp.is_visible():
                    opened = True
                    print(f"  [{label}] Symbol search opened via: {sel}")
                    break
                else:
                    dismiss_all(page, label)
        except Exception:
            pass

    # Step 3: if still not opened, try JS to click toolbar button
    if not opened:
        print(f"  [{label}] Trying JS click on symbol search button")
        try:
            page.evaluate("""
                (function(){
                    var sels = [
                        '#header-toolbar-symbol-search',
                        '[data-name="header-toolbar-symbol-search"]',
                        '[class*="mainTitle"]',
                        '[class*="symbolTitle"]',
                        '[class*="symbol-title"]',
                    ];
                    for(var i=0;i<sels.length;i++){
                        var el = document.querySelector(sels[i]);
                        if(el){ el.click(); return sels[i]; }
                    }
                    return null;
                })()
            """)
            page.wait_for_timeout(900)
            inp = (page.query_selector('input[data-role="search"]') or
                   page.query_selector('input[placeholder*="earch"]'))
            if inp and inp.is_visible():
                opened = True
                print(f"  [{label}] Symbol search opened via JS")
        except Exception as e:
            print(f"  [{label}] JS click error: {e}")

    # Step 4: type the symbol
    if opened:
        try:
            page.keyboard.press("Control+a")
            page.wait_for_timeout(100)
            page.keyboard.type(query, delay=80)
            page.wait_for_timeout(1200)
        except Exception as e:
            print(f"  [{label}] Type error: {e}")
    else:
        print(f"  [{label}] Could not open symbol search — trying keyboard type anyway")
        try:
            page.keyboard.type(query, delay=80)
            page.wait_for_timeout(1200)
        except Exception:
            pass

    # Step 5: wait for dropdown results then Enter
    for sel in ['div[class*="listItem"]','div[class*="search-item"]',
                'div[data-symbol-item]','li[class*="item-"]',
                'div[class*="symbolSearchResult"]']:
        try:
            page.wait_for_selector(sel, timeout=4000)
            break
        except Exception:
            pass

    try:
        page.keyboard.press("Enter")
        page.wait_for_timeout(800)
    except Exception:
        pass

    # Step 6: wait for chart reload
    page.wait_for_timeout(3000)
    wait_spinner(page, max_sec=15)
    dismiss_all(page, label)
    page.wait_for_timeout(800)

    # Step 7: verify
    return verify_symbol_dom(page, sym, label)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
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
ls_script = make_ls_script(storage_origins)
print(f"Cookies: {len(cookies)}  Origins: {len(storage_origins)}")

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
    if ls_script:
        context.add_init_script(ls_script)

    page = context.new_page()

    for idx, tf_key in enumerate(timeframes):
        tf_lbl   = TF_LABEL.get(tf_key, tf_key)
        out_path = out_dir / f"{symbol}_{tf_lbl}.png"
        url      = build_url(symbol, exchange, tf_key, theme)

        print(f"\n{'─'*55}")
        print(f"  {symbol} [{tf_lbl}]  ({idx+1}/{len(timeframes)})")

        try:
            page.goto(url, timeout=40000, wait_until="domcontentloaded")
            wait_chart(page, max_sec=25)
            page.wait_for_timeout(1500)
            dismiss_all(page, tf_lbl)

            # Check symbol first — if wrong, force it
            ok = verify_symbol_dom(page, symbol, tf_lbl)
            if not ok:
                print(f"  [{tf_lbl}] Symbol mismatch — forcing...")
                ok = force_symbol(page, symbol, exchange, tf_lbl)
                if not ok:
                    print(f"  [{tf_lbl}] WARNING: could not verify symbol")
            else:
                print(f"  [{tf_lbl}] Symbol already correct — skipping force")

            # Extra wait for indicators to settle
            extra = max(0, wait_sec - 12)
            if extra > 0:
                page.wait_for_timeout(extra * 1000)

            # Final dismiss
            dismiss_all(page, tf_lbl)
            page.wait_for_timeout(500)

            img_bytes = page.screenshot(full_page=False)
            out_path.write_bytes(img_bytes)
            print(f"  Saved: {out_path} ({len(img_bytes)//1024} KB)")
            results.append({"tf":tf_key,"label":tf_lbl,"path":str(out_path),"success":True})

        except Exception as e:
            print(f"  ERROR [{tf_lbl}]: {e}")
            results.append({"tf":tf_key,"label":tf_lbl,"path":"","success":False,"error":str(e)})

    context.close()
    browser.close()
    print("\n=== Browser closed ===")

manifest = {
    "symbol":symbol,"exchange":exchange,"timeframes":timeframes,
    "theme":theme,"timestamp":datetime.utcnow().isoformat()+"Z","results":results,
}
(out_dir/"manifest.json").write_text(
    json.dumps(manifest,indent=2,ensure_ascii=False),encoding="utf-8")

ok   = sum(1 for r in results if r["success"])
fail = len(results)-ok
print(f"\n{'='*55}\nDONE: {ok}/{len(results)} OK | {fail} failed")
for r in results:
    print(f"  {'OK' if r['success'] else 'ERR'}  {symbol}_{r['label']}  {r.get('path') or r.get('error','')}")

sys.exit(0 if fail==0 else 1)
