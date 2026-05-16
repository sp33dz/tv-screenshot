#!/usr/bin/env python3
"""
snapshot_run.py  —  On-demand TradingView screenshot
Strategy: Use /chart/new/ URL (bypasses saved layout), verify symbol from DOM,
          dismiss all dialogs robustly before screenshotting.
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
    """
    /chart/new/ forces a fresh chart that reads ?symbol= from URL.
    /chart/ loads saved layout and ignores ?symbol= when session exists.
    """
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

def dismiss_all(page, label=""):
    """Press Escape 3 times + click all visible close buttons."""
    for _ in range(3):
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(200)
        except Exception:
            pass
    count = 0
    for sel in [
        'button[data-name="close"]',
        '[data-name="close-button"]',
        'button[aria-label="Close"]',
        'button[aria-label="close"]',
        '[class*="closeButton"]',
        '[class*="close-button"]',
    ]:
        try:
            for btn in page.query_selector_all(sel):
                if btn.is_visible():
                    btn.click(timeout=400)
                    count += 1
                    page.wait_for_timeout(150)
        except Exception:
            pass
    if count:
        print(f"  [{label}] dismissed {count} dialog(s)")

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
    wait_spinner(page, 15)

def read_symbol_from_dom(page):
    """Read the currently displayed TICKER from TradingView DOM.

    Priority order: ticker-specific selectors first, then page title as fallback.
    TradingView shows the ticker (e.g. 'AIXI') separately from the company name
    ('XIAO-I CORPORATION') — we must target ticker elements, not company-name ones.
    """
    # Tier 1: Elements that reliably hold the TICKER (short symbol), not company name
    TICKER_SELS = [
        # Legend item ticker — most reliable
        '[data-name="legend-series-item"] [class*="title"]',
        # Chart header ticker area
        '[class*="priceSources"] [class*="symbol"]',
        '[class*="symbolInput"] input',
        '[data-name="chart-toolbar-symbol-search"] input',
        # Left toolbar / header symbol display
        'div[class*="tickerInput"]',
        '[class*="tickerContainer"] [class*="ticker"]',
        '[class*="symbolChip"]',
        # Generic but higher-priority than company name
        'div[class*="symbolInfo"] [class*="ticker"]',
        'div[class*="symbolInfo"] [class*="symbol"]:first-child',
    ]
    for sel in TICKER_SELS:
        try:
            el = page.query_selector(sel)
            if el:
                txt = (el.get_attribute("value") or el.inner_text() or "").strip().upper()
                # Ticker is short (1–10 chars) and has no spaces
                if txt and 1 <= len(txt) <= 10 and " " not in txt:
                    return txt
        except Exception:
            pass

    # Tier 2: page title — format is usually "TICKER — TradingView" or "TICKER · ..."
    try:
        title = page.title()
        # Try the first token before any separator
        for sep in [" — ", " - ", " · ", "·", "—"]:
            if sep in title:
                part = title.split(sep)[0].strip().upper()
                if part and 1 <= len(part) <= 10 and " " not in part:
                    return part
    except Exception:
        pass

    # Tier 3: fallback — return whatever the first legend title says (may be company name)
    FALLBACK_SELS = [
        '[data-name="legend-series-item"] div[class*="mainTitle"]',
        'div[class*="symbolTitle"]',
        'div[class*="symbol-title"]',
        '[class*="titleWrapper"] [class*="title"]',
    ]
    for sel in FALLBACK_SELS:
        try:
            el = page.query_selector(sel)
            if el:
                txt = el.inner_text().strip().upper()
                if txt:
                    return txt
        except Exception:
            pass
    return ""

def verify_symbol(page, sym, label):
    dom_sym = read_symbol_from_dom(page)
    match = sym.upper() in dom_sym
    print(f"  [{label}] DOM symbol: {dom_sym!r}  expected={sym}  match={match}")
    return match

# Main
from playwright.sync_api import sync_playwright

results   = []
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
    "--no-session-restore",
    "--disable-session-crashed-bubble",
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
        print(f"  URL: {url}")

        try:
            page.goto(url, timeout=40000, wait_until="domcontentloaded")
            wait_chart(page, max_sec=25)
            page.wait_for_timeout(2000)
            dismiss_all(page, tf_lbl)

            ok = verify_symbol(page, symbol, tf_lbl)
            if not ok:
                print(f"  [{tf_lbl}] Symbol mismatch — waiting longer then re-checking")
                page.wait_for_timeout(3000)
                dismiss_all(page, tf_lbl)
                ok = verify_symbol(page, symbol, tf_lbl)
                if not ok:
                    print(f"  [{tf_lbl}] WARNING: symbol still unverified — saving anyway")

            extra = max(0, wait_sec - 12)
            if extra > 0:
                page.wait_for_timeout(extra * 1000)

            dismiss_all(page, tf_lbl)
            page.wait_for_timeout(500)

            img_bytes = page.screenshot(full_page=False)
            out_path.write_bytes(img_bytes)
            print(f"  Saved: {out_path} ({len(img_bytes)//1024} KB)")
            results.append({"tf":tf_key,"label":tf_lbl,"path":str(out_path),"success":True,"verified":ok})

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
    vfy = "(unverified)" if not r.get("verified",True) else ""
    print(f"  {'OK' if r['success'] else 'ERR'}  {symbol}_{r['label']}  {vfy}")

sys.exit(0 if fail==0 else 1)
