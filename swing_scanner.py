"""
TradeFlow - Short-Term Swing Scanner & Telegram Alert Dispatcher (v4.0)
Upgrades for Short-Term NSE Swings (2–10 Trading Days):
  - Dynamic Nifty 200 universe via NSE Archives / NiftyIndices CSV (auto Industry/Sector map) + multi-sector fallback
  - Chunked yfinance batch download (auto_adjust=False, multi_level_index=True) avoiding dividend-distorted resistance & 1-ticker crashes
  - Vectorized indicator engine: EMA 10/20/50, Wilder ATR 5/14/20, Mansfield RS + 20D RS New High
  - Empirical NSE U-shaped intraday volume curve (with 15m Yahoo feed-delay compensation) & session-aware mode (MORNING / BTST / EOD)
  - 5 clean, non-shadowing short-term swing tiers including VCP Tight Coil & fixed RVOL bounce scoring
  - Regime-adaptive trade gating & risk sizing wired directly to Nifty 50 trend
  - Gap-aware structural stop-loss, fee-aware position sizing, 52W-high overhead supply flag, & split targets (1.5R + 10 EMA trail)
  - Concurrent earnings-date guard (shortlist only), reliable FII/DII cascade (no 0.0 sentinel), cross-run state.json tracking, & HTML chunking
"""

import io
import json
import os
import re
import sys
import time
import html
import logging
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timezone, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

import numpy as np
import requests

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("Error: Missing yfinance or pandas. Run: pip install yfinance pandas numpy requests")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

IST = timezone(timedelta(hours=5, minutes=30))
BENCHMARK_SYMBOL = "^NSEI"

# --- Risk & Sizing Parameters (Short-Term NSE Delivery Swings) ---
MAX_LOSS_BUDGET = float(os.environ.get("MAX_LOSS_BUDGET", "700.0"))         # Max risk per trade in INR
MAX_CAPITAL_PER_TRADE = float(os.environ.get("MAX_CAPITAL_PER_TRADE", "40000.0"))  # Max deployed capital per trade
NSE_DELIVERY_FEE_RATE = 0.0022   # ~0.22% round-trip (0.20% STT + stamp + exchange + SEBI + GST)
DP_CHARGE_INR = 16.0             # CDSL/NSDL sell-side depository charge

# Liquidity & Volatility Guardrails (Short-Term Swing Sweet Spot)
MIN_PRICE_INR = 50.0
MIN_MEDIAN_TURNOVER_INR = 2.5e8  # ₹25 Cr median 20D daily turnover
MIN_ATR_PCT = 1.2                # Avoid dead low-beta stocks for 2-10d swings
MAX_ATR_PCT = 6.5                # Avoid erratic circuit-prone names
MAX_CANDIDATES = 7
MAX_PER_SECTOR = 2

# Persistent state path (restored/saved via actions/cache in GitHub Actions)
STATE_FILE = Path(os.environ.get("SCANNER_STATE_PATH", ".scanner_cache/state.json"))

# Dynamic Nifty 200 CSV Endpoints (verified 2026)
NIFTY200_CSV_URLS = [
    "https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv",
    "https://www.niftyindices.com/IndexConstituent/ind_nifty200list.csv",
]

# Diversified fallback universe if both CSV endpoints are unreachable
FALLBACK_UNIVERSE: dict[str, str] = {
    "TATASTEEL": "Metals", "HINDALCO": "Metals", "JINDALSTEL": "Metals", "VEDL": "Metals", "NMDC": "Metals",
    "BEL": "Capital Goods", "HAL": "Capital Goods", "BHEL": "Capital Goods", "LT": "Construction",
    "TATAPOWER": "Power", "NTPC": "Power", "POWERGRID": "Power", "COALINDIA": "Oil Gas & Fuels",
    "ADANIPORTS": "Services", "JSWINFRA": "Services", "INDIGO": "Services", "DELHIVERY": "Services",
    "SBIN": "Financial Services", "ICICIBANK": "Financial Services", "HDFCBANK": "Financial Services",
    "BAJFINANCE": "Financial Services", "POLICYBZR": "Financial Services",
    "RELIANCE": "Oil Gas & Fuels", "BHARTIARTL": "Telecom", "ETERNAL": "Consumer Services",
    "TRENT": "Consumer Services", "M&M": "Automobile", "BAJAJ-AUTO": "Automobile",
    "SUNPHARMA": "Healthcare", "CIPLA": "Healthcare", "LUPIN": "Healthcare",
    "TCS": "IT", "INFY": "IT", "PERSISTENT": "IT", "AMBUJACEM": "Construction Materials",
}

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


# ---------------------------------------------------------------------------
# 1. Session Detection & Empirical NSE Volume Projection
# ---------------------------------------------------------------------------

def detect_session_mode(now: Optional[datetime] = None) -> str:
    """
    Determine scan session mode: MORNING, BTST, or EOD.
    Can be overridden via SCAN_MODE env var for manual workflow_dispatch runs.
    """
    override = os.environ.get("SCAN_MODE", "").strip().upper()
    if override in {"MORNING", "BTST", "EOD"}:
        return override

    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return "EOD"

    mins_from_midnight = now.hour * 60 + now.minute
    if 9 * 60 + 15 <= mins_from_midnight < 13 * 60 + 30:
        return "MORNING"
    if 13 * 60 + 30 <= mins_from_midnight <= 15 * 60 + 35:
        return "BTST"
    return "EOD"


def get_nse_intraday_vol_multiplier(now: Optional[datetime] = None, feed_delay_mins: float = 15.0) -> float:
    """
    Estimate full-session EOD volume multiplier using NSE's empirical U-shaped
    intraday volume curve, compensating for ~15-min Yahoo Finance feed delay.
    Returns 1.0 outside live market hours (or after 15:45 IST when Yahoo EOD bar settles).
    """
    now = now or datetime.now(IST)
    if now.weekday() >= 5:
        return 1.0

    elapsed_wall = (now.hour * 60 + now.minute + now.second / 60.0) - (9 * 60 + 15)
    # Before market open or >15 mins after close (Yahoo final EOD bar available)
    if elapsed_wall <= 0 or elapsed_wall >= (375.0 + feed_delay_mins):
        return 1.0

    # Compensate for Yahoo Finance's ~15-min NSE quote lag during live/post-close settle window
    eff_mins = max(min(elapsed_wall - feed_delay_mins, 375.0), 15.0)
    if eff_mins >= 370.0:
        return 1.0

    # Empirical NSE cumulative volume distribution across 375-minute session (09:15–15:30 IST):
    #   09:15–10:15 (60m):  ~26% (opening surge)
    #   10:15–12:00 (105m): +22% -> 48%
    #   12:00–14:15 (135m): +20% -> 68% (midday lull)
    #   14:15–15:30 (75m):  +32% -> 100% (closing institutional auction/surge)
    if eff_mins <= 60.0:
        cum_frac = 0.26 * (eff_mins / 60.0)
    elif eff_mins <= 165.0:
        cum_frac = 0.26 + 0.22 * ((eff_mins - 60.0) / 105.0)
    elif eff_mins <= 300.0:
        cum_frac = 0.48 + 0.20 * ((eff_mins - 165.0) / 135.0)
    else:
        cum_frac = 0.68 + 0.32 * ((eff_mins - 300.0) / 75.0)

    return round(1.0 / max(cum_frac, 0.08), 3)


# ---------------------------------------------------------------------------
# 2. Dynamic Universe Loader (Nifty 200 CSV -> Auto Sector Map)
# ---------------------------------------------------------------------------

def shorten_sector(industry: str) -> str:
    """Shorten NSE Industry labels for compact Telegram rendering."""
    mapping = {
        "Financial Services": "Financials",
        "Information Technology": "IT",
        "Oil Gas & Consumable Fuels": "Energy/Oil",
        "Automobile and Auto Components": "Auto",
        "Fast Moving Consumer Goods": "FMCG",
        "Construction Materials": "Cement/Mat",
        "Metals & Mining": "Metals",
        "Consumer Durables": "Durables",
        "Consumer Services": "Consumer",
        "Capital Goods": "CapGoods",
        "Telecommunication": "Telecom",
        "Healthcare": "Pharma/HC",
        "Chemicals": "Chemicals",
        "Construction": "Infra",
        "Power": "Power",
        "Realty": "Realty",
        "Services": "Services",
    }
    clean = str(industry).strip()
    return mapping.get(clean, clean[:14] if clean else "Other")


def fetch_nifty200_universe() -> tuple[list[str], dict[str, str], str]:
    """
    Fetch Nifty 200 constituents & official NSE Industry classification.
    Returns: (yahoo_symbols, clean_symbol_to_sector, source_label)
    """
    for url in NIFTY200_CSV_URLS:
        try:
            resp = requests.get(url, headers=BROWSER_HEADERS, timeout=10)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            cols = {c.strip(): c for c in df.columns}
            if "Symbol" not in cols:
                continue

            sym_col = cols["Symbol"]
            ind_col = cols.get("Industry")
            series_col = cols.get("Series")

            if series_col:
                df = df[df[series_col].astype(str).str.strip().str.upper() == "EQ"]

            sector_map: dict[str, str] = {}
            symbols: list[str] = []
            for _, row in df.iterrows():
                raw_sym = str(row[sym_col]).strip().upper()
                if not raw_sym or raw_sym == "NAN" or "DUMMY" in raw_sym:
                    continue
                sector = shorten_sector(row[ind_col]) if ind_col else "Other"
                sector_map[raw_sym] = sector
                symbols.append(f"{raw_sym}.NS")

            if len(symbols) >= 100:
                host = url.split("/")[2]
                logging.info(f"Loaded {len(symbols)} Nifty 200 symbols from {host}")
                return symbols, sector_map, f"Nifty 200 ({host})"
        except Exception as e:
            logging.warning(f"Universe fetch failed from {url}: {e}")

    logging.warning("Falling back to curated 36-stock diversified NSE universe.")
    symbols = [f"{s}.NS" for s in FALLBACK_UNIVERSE]
    return symbols, dict(FALLBACK_UNIVERSE), "Fallback 36"


# ---------------------------------------------------------------------------
# 3. Robust Chunked yfinance Downloader (MultiIndex-Safe, Unadjusted OHLC)
# ---------------------------------------------------------------------------

def _extract_panel(df: pd.DataFrame, field: str, expected_symbols: list[str]) -> pd.DataFrame:
    """Safely extract a [Date x Ticker] DataFrame for `field` regardless of yfinance column structure."""
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        lvl0 = set(df.columns.get_level_values(0))
        lvl1 = set(df.columns.get_level_values(1))
        if field in lvl0:
            sub = df[field]
        elif field in lvl1:
            sub = df.xs(field, axis=1, level=1)
        else:
            return pd.DataFrame()
        if isinstance(sub, pd.Series):
            name = sub.name if sub.name in expected_symbols else expected_symbols[0]
            return sub.to_frame(name=name)
        return sub

    # Single-level fallback (rare when multi_level_index=True, handled defensively)
    if field in df.columns and len(expected_symbols) == 1:
        return df[[field]].rename(columns={field: expected_symbols[0]})
    return pd.DataFrame()


def download_ohlcv_panels(all_symbols: list[str], chunk_size: int = 80) -> dict[str, pd.DataFrame]:
    """
    Download 1y daily OHLCV in chunks with auto_adjust=False (split-adjusted,
    NOT dividend-distorted) and multi_level_index=True.
    Returns dict of aligned DataFrames keyed by Open, High, Low, Close, Volume.
    """
    fields = ("Open", "High", "Low", "Close", "Volume")
    buckets: dict[str, list[pd.DataFrame]] = {f: [] for f in fields}

    chunks = [all_symbols[i : i + chunk_size] for i in range(0, len(all_symbols), chunk_size)]
    for idx, chunk in enumerate(chunks, 1):
        raw = pd.DataFrame()
        for attempt in range(2):
            try:
                raw = yf.download(
                    chunk,
                    period="1y",
                    interval="1d",
                    group_by="column",
                    auto_adjust=False,
                    multi_level_index=True,
                    threads=True,
                    progress=False,
                    timeout=20,
                )
                if raw is not None and not raw.empty:
                    break
            except Exception as e:
                logging.warning(f"Chunk {idx}/{len(chunks)} attempt {attempt + 1} error: {e}")
            time.sleep(1.5)

        if raw is None or raw.empty:
            logging.warning(f"Chunk {idx}/{len(chunks)} returned empty data.")
            continue

        for f in fields:
            panel = _extract_panel(raw, f, chunk)
            if not panel.empty:
                buckets[f].append(panel)

        if idx < len(chunks):
            time.sleep(0.4)  # Gentle throttle protection against Yahoo 429

    panels: dict[str, pd.DataFrame] = {}
    for f in fields:
        if buckets[f]:
            merged = pd.concat(buckets[f], axis=1)
            merged = merged.loc[:, ~merged.columns.duplicated()]
            # Normalize index to timezone-naive midnight dates for clean alignment
            merged.index = pd.to_datetime(merged.index).tz_localize(None).normalize()
            merged = merged.sort_index()
            panels[f] = merged
        else:
            panels[f] = pd.DataFrame()
    return panels


# ---------------------------------------------------------------------------
# 4. Market News & FII/DII Cascade (No 0.0 Sentinel Bug)
# ---------------------------------------------------------------------------

def fetch_market_news(limit: int = 3) -> list[str]:
    """Fetch fresh (<36h) Indian equity market headlines via Google News RSS."""
    rss_url = (
        "https://news.google.com/rss/search?"
        "q=Nifty+Sensex+Indian+stock+market+when:1d&hl=en-IN&gl=IN&ceid=IN:en"
    )
    headlines: list[str] = []
    try:
        resp = requests.get(rss_url, headers={"User-Agent": BROWSER_HEADERS["User-Agent"]}, timeout=10)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for item in root.findall(".//item"):
            title_el = item.find("title")
            if title_el is not None and title_el.text:
                title = title_el.text.strip()
                clean = title.rsplit(" - ", 1)[0] if " - " in title else title
                if clean not in headlines:
                    headlines.append(clean)
            if len(headlines) >= limit:
                break
    except Exception as e:
        logging.warning(f"Market news RSS failed: {e}")
    return headlines or ["Indian equities tracking global macro cues and institutional flows."]


def _parse_fii_dii_from_headline(title: str) -> tuple[Optional[float], Optional[float]]:
    """
    Best-effort regex extraction of net FII/DII Rs crore figures from fresh wire headlines,
    e.g., 'FIIs sell equities worth Rs 1,842 crore; DIIs buy Rs 2,310 crore'.
    """
    fii_net: Optional[float] = None
    dii_net: Optional[float] = None

    pat = re.compile(
        r"\b(FIIs?|FPIs?|DIIs?)\b\s+(?:net\s+)?(buy|bought|purchase|purchased|infuse|infused|sell|sold|offload|offloaded|dump|dumped)"
        r"(?:[^0-9;.,]{0,35}?)(?:Rs\.?|₹|INR)?\s*([\d,]+(?:\.\d+)?)\s*(?:cr\b|crore)",
        re.IGNORECASE,
    )
    for who, verb, num_str in pat.findall(title):
        val = float(num_str.replace(",", ""))
        signed = -val if verb.lower() in {"sell", "sold", "offload", "dump"} else val
        if who.upper().startswith(("FII", "FPI")) and fii_net is None:
            fii_net = signed
        elif who.upper().startswith("DII") and dii_net is None:
            dii_net = signed
    return fii_net, dii_net


def fetch_fii_dii_flow() -> dict:
    """
    Fetch daily FII/DII net cash flow with multi-tier cascade:
      1) Direct NSE API (/api/fiidiiTradeReact)
      2) Fresh (<36h) Google News RSS wire with regex figure extraction
    Uses None (never 0.0) to represent unavailable numbers.
    """
    now_ist = datetime.now(IST)
    flow_data: dict = {
        "fii_net": None,
        "dii_net": None,
        "date": now_ist.strftime("%d %b"),
        "sentiment": None,
        "headline": None,
        "source": "Pending",
    }

    # Attempt 1: Direct NSE session
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": BROWSER_HEADERS["User-Agent"],
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/reports/fii-dii",
        })
        session.get("https://www.nseindia.com/reports/fii-dii", timeout=6)
        resp = session.get("https://www.nseindia.com/api/fiidiiTradeReact", timeout=6)
        if resp.status_code == 200:
            payload = resp.json()
            if isinstance(payload, list):
                for item in payload:
                    cat = str(item.get("category", "")).upper()
                    raw_val = str(item.get("netValue", "") or "").replace(",", "").strip()
                    if raw_val in ("", "-", "None"):
                        continue
                    net_val = float(raw_val)
                    if item.get("date"):
                        flow_data["date"] = str(item["date"])
                    if "FII" in cat or "FPI" in cat:
                        flow_data["fii_net"] = net_val
                    elif "DII" in cat:
                        flow_data["dii_net"] = net_val
                if flow_data["fii_net"] is not None or flow_data["dii_net"] is not None:
                    flow_data["source"] = "NSE Official"
                    logging.info(f"NSE FII/DII fetched ({flow_data['date']}): FII={flow_data['fii_net']}, DII={flow_data['dii_net']}")
    except Exception as e:
        logging.warning(f"Direct NSE FII/DII blocked/failed: {e}")

    # Attempt 2: Fresh (<36h) Google News RSS fallback
    if flow_data["fii_net"] is None and flow_data["dii_net"] is None:
        try:
            feed_url = (
                "https://news.google.com/rss/search?"
                "q=FII+DII+cash+crore+when:1d&hl=en-IN&gl=IN&ceid=IN:en"
            )
            resp = requests.get(feed_url, headers={"User-Agent": BROWSER_HEADERS["User-Agent"]}, timeout=8)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            for it in root.findall(".//item")[:8]:
                title_el = it.find("title")
                pub_el = it.find("pubDate")
                if title_el is None or not title_el.text:
                    continue
                if pub_el is not None and pub_el.text:
                    try:
                        pub_dt = parsedate_to_datetime(pub_el.text)
                        if pub_dt.tzinfo is None:
                            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                        if (datetime.now(timezone.utc) - pub_dt).total_seconds() > 36 * 3600:
                            continue
                    except Exception:
                        pass

                raw_title = title_el.text.strip()
                clean_title = raw_title.rsplit(" - ", 1)[0] if " - " in raw_title else raw_title
                if not ("fii" in clean_title.lower() or "fpi" in clean_title.lower()):
                    continue

                fii_parsed, dii_parsed = _parse_fii_dii_from_headline(clean_title)
                if fii_parsed is not None or dii_parsed is not None:
                    flow_data["fii_net"] = fii_parsed
                    flow_data["dii_net"] = dii_parsed
                    flow_data["source"] = "Wire Extract"
                    flow_data["headline"] = clean_title
                    break
                elif flow_data["headline"] is None and "crore" in clean_title.lower():
                    flow_data["headline"] = clean_title
                    flow_data["source"] = "Financial Wire"
        except Exception as e:
            logging.warning(f"FII/DII RSS fallback failed: {e}")

    # Classify flow sentiment cleanly
    fii = flow_data["fii_net"]
    dii = flow_data["dii_net"]
    if fii is not None and dii is not None:
        if fii > 500 and dii > 0:
            flow_data["sentiment"] = "🟢 Strong Confluence (Both Buying)"
        elif fii > 0 and dii <= 0:
            flow_data["sentiment"] = "🟢 FII Inflow Driven (Bullish)"
        elif fii > 0 and dii > 0:
            flow_data["sentiment"] = "🟢 Net Institutional Inflows"
        elif fii < -1500 and dii > 0:
            flow_data["sentiment"] = "🟡 Choppy (FII Selling, DII Absorbing)"
        elif fii < 0 and dii < 0:
            flow_data["sentiment"] = "🔴 Defensive / Risk-Off (Both Selling)"
        else:
            flow_data["sentiment"] = "🟡 Mixed Institutional Flow"
    elif fii is not None:
        flow_data["sentiment"] = "🟢 FII Net Buyer" if fii > 0 else "🟡 FII Net Seller"
    elif flow_data["headline"]:
        flow_data["sentiment"] = flow_data["headline"][:115]
    else:
        flow_data["sentiment"] = "⚪ Pending Official Release (~6:00–6:45 PM IST)"

    return flow_data


# ---------------------------------------------------------------------------
# 5. Earnings Proximity Guard (Shortlist Only)
# ---------------------------------------------------------------------------

def _fetch_next_earnings_date(yahoo_sym: str) -> Optional[date]:
    """Check yfinance Ticker.calendar for upcoming earnings date within 7 days."""
    try:
        cal = yf.Ticker(yahoo_sym).calendar
        if cal is None:
            return None

        raw_dates = None
        if isinstance(cal, dict):
            raw_dates = cal.get("Earnings Date")
        elif isinstance(cal, pd.DataFrame) and "Earnings Date" in cal.index:
            raw_dates = cal.loc["Earnings Date"].tolist()
        elif isinstance(cal, pd.DataFrame) and "Earnings Date" in cal.columns:
            raw_dates = cal["Earnings Date"].tolist()

        if raw_dates is None:
            return None
        if not isinstance(raw_dates, (list, tuple, pd.Series, np.ndarray)):
            raw_dates = [raw_dates]

        today = datetime.now(IST).date()
        upcoming: list[date] = []
        for item in raw_dates:
            if item is None or (isinstance(item, float) and np.isnan(item)):
                continue
            if isinstance(item, datetime):
                d = item.date()
            elif isinstance(item, date):
                d = item
            else:
                d = pd.to_datetime(item).date()
            if 0 <= (d - today).days <= 7:
                upcoming.append(d)
        return min(upcoming) if upcoming else None
    except Exception:
        return None


def attach_earnings_warnings(candidates: list[dict]) -> None:
    """Concurrently attach earnings warnings ONLY for shortlisted candidates."""
    if not candidates:
        return
    with ThreadPoolExecutor(max_workers=min(6, len(candidates))) as pool:
        future_map = {
            pool.submit(_fetch_next_earnings_date, f"{c['symbol']}.NS"): c
            for c in candidates
        }
        for fut in as_completed(future_map):
            cand = future_map[fut]
            try:
                edate = fut.result()
                cand["earnings_warn"] = edate.strftime("%d %b") if edate else None
            except Exception:
                cand["earnings_warn"] = None


# ---------------------------------------------------------------------------
# 6. Vectorized Short-Term Swing Engine
# ---------------------------------------------------------------------------

def evaluate_nifty_regime(bench_close: pd.Series) -> dict:
    """Classify Nifty 50 regime and return dynamic risk multiplier."""
    bench_close = bench_close.dropna()
    if len(bench_close) < 60:
        return {
            "code": "PULLBACK",
            "risk_scale": 0.75,
            "label": "⚪ Benchmark Incomplete (Nifty — Cautious 0.75x)",
        }

    n_ema20 = float(bench_close.ewm(span=20, adjust=False).mean().iloc[-1])
    n_ema50 = float(bench_close.ewm(span=50, adjust=False).mean().iloc[-1])
    n_price = float(bench_close.iloc[-1])

    if n_price >= n_ema20 and n_ema20 >= n_ema50:
        return {
            "code": "BULLISH",
            "risk_scale": 1.0,
            "label": f"🟢 Bullish Trend (Nifty {n_price:,.0f} > 20/50E | Risk 1.0x)",
        }
    if n_price >= n_ema50:
        return {
            "code": "PULLBACK",
            "risk_scale": 0.75,
            "label": f"🟡 Pullback/Chop (Nifty {n_price:,.0f} > 50E | Breakouts Off | Risk 0.75x)",
        }
    return {
        "code": "RISK_OFF",
        "risk_scale": 0.5,
        "label": f"🔴 Risk-Off (Nifty {n_price:,.0f} < 50E | RS Leaders Only | Risk 0.5x)",
    }


def compute_fee_aware_size(
    entry_price: float,
    stop_loss: float,
    risk_budget: float,
    max_capital: float,
) -> tuple[int, float]:
    """
    Compute max shares such that (stop_distance * shares + statutory delivery fees) <= risk_budget
    AND (entry_price * shares) <= max_capital.
    """
    raw_risk = entry_price - stop_loss
    if raw_risk <= 0 or entry_price <= 0:
        return 0, 0.0

    # Reserve DP charge + percentage round-trip statutory friction
    net_budget = max(risk_budget - DP_CHARGE_INR, 0.0)
    eff_risk_per_share = raw_risk + (entry_price * NSE_DELIVERY_FEE_RATE)
    risk_shares = int(net_budget / eff_risk_per_share)
    cap_shares = int(max_capital / entry_price)
    shares = min(risk_shares, cap_shares)
    return shares, round(raw_risk, 2)


def scan_swing_setups(
    panels: dict[str, pd.DataFrame],
    symbols: list[str],
    sector_map: dict[str, str],
    session_mode: str,
    vol_mult: float,
) -> tuple[list[dict], dict, dict]:
    """
    Vectorized pre-computation + regime-adaptive short-term swing ranking.
    Returns: (top_candidates, regime_info, telemetry_dict)
    """
    close_df = panels.get("Close", pd.DataFrame())
    open_df = panels.get("Open", pd.DataFrame())
    high_df = panels.get("High", pd.DataFrame())
    low_df = panels.get("Low", pd.DataFrame())
    vol_df = panels.get("Volume", pd.DataFrame())

    telemetry = {
        "requested": len(symbols),
        "downloaded": 0,
        "liquid_eligible": 0,
        "qualified": 0,
        "failed_tickers": 0,
        "vol_mult": vol_mult,
        "mode": session_mode,
    }

    if close_df.empty:
        return [], evaluate_nifty_regime(pd.Series(dtype=float)), telemetry

    bench_close = close_df[BENCHMARK_SYMBOL].dropna() if BENCHMARK_SYMBOL in close_df.columns else pd.Series(dtype=float)
    regime = evaluate_nifty_regime(bench_close)
    effective_risk_budget = MAX_LOSS_BUDGET * regime["risk_scale"]

    available = [s for s in symbols if s in close_df.columns]
    telemetry["downloaded"] = len(available)

    # Track latest price of every downloaded ticker (for state.json SL breach checks)
    latest_prices: dict[str, float] = {}
    candidates: list[dict] = []

    for sym in available:
        clean_name = sym.replace(".NS", "")
        try:
            df = pd.DataFrame({
                "Open": open_df[sym],
                "High": high_df[sym],
                "Low": low_df[sym],
                "Close": close_df[sym],
                "Volume": vol_df[sym],
            }).dropna()

            if len(df) < 120:
                continue

            close = df["Close"]
            open_p = df["Open"]
            high = df["High"]
            low = df["Low"]
            vol = df["Volume"]

            curr_price = float(close.iloc[-1])
            latest_prices[clean_name] = round(curr_price, 2)

            if curr_price < MIN_PRICE_INR:
                continue

            # 20D median daily turnover liquidity filter (prior 20 full sessions)
            median_turnover = float((close.iloc[-21:-1] * vol.iloc[-21:-1]).median())
            if pd.isna(median_turnover) or median_turnover < MIN_MEDIAN_TURNOVER_INR:
                continue

            # Wilder's ATR (5, 14, 20)
            prev_close_series = close.shift(1)
            tr = pd.concat([
                high - low,
                (high - prev_close_series).abs(),
                (low - prev_close_series).abs(),
            ], axis=1).max(axis=1)

            atr5 = float(tr.ewm(alpha=1 / 5, adjust=False).mean().iloc[-1])
            atr14 = float(tr.ewm(alpha=1 / 14, adjust=False).mean().iloc[-1])
            atr20 = float(tr.ewm(alpha=1 / 20, adjust=False).mean().iloc[-1])
            if pd.isna(atr14) or atr14 <= 0 or pd.isna(atr20) or atr20 <= 0:
                continue

            atr_pct = (atr14 / curr_price) * 100.0
            if not (MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT):
                continue

            telemetry["liquid_eligible"] += 1

            curr_open = float(open_p.iloc[-1])
            curr_high = float(high.iloc[-1])
            curr_low = float(low.iloc[-1])
            prev_price = float(close.iloc[-2])
            prev_high = float(high.iloc[-2])
            day_change = ((curr_price - prev_price) / prev_price) * 100.0

            # EMAs (10 for short-term momentum, 20 & 50 for swing trend)
            ema10 = float(close.ewm(span=10, adjust=False).mean().iloc[-1])
            ema20 = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
            ema50 = float(close.ewm(span=50, adjust=False).mean().iloc[-1])

            # Intraday-normalized RVOL
            avg_vol20 = float(vol.iloc[-21:-1].mean())
            if avg_vol20 <= 0:
                continue
            proj_vol = float(vol.iloc[-1]) * vol_mult
            rvol = round(proj_vol / avg_vol20, 2)
            prior_3d_rvol = float(vol.iloc[-4:-1].mean()) / avg_vol20

            # Candle geometry (distinct CLV, Body Ratio, Upper Wick)
            is_green = curr_price >= curr_open
            range_span = curr_high - curr_low
            if range_span > 0:
                clv = (curr_price - curr_low) / range_span
                body_ratio = abs(curr_price - curr_open) / range_span
                upper_wick = (curr_high - max(curr_open, curr_price)) / range_span
            else:
                clv, body_ratio, upper_wick = 0.5, 0.0, 0.0

            # Volatility Contraction & Tightness (for VCP Coils)
            atr_compression = atr5 / atr20
            prior_3d_close_spread = (
                (float(close.iloc[-4:-1].max()) - float(close.iloc[-4:-1].min())) / prev_price
            )

            # Multi-Horizon Mansfield RS vs Nifty 50 + 20D RS New High
            mansfield_rs = 0.0
            rs_new_20d_high = False
            rs_10d = 0.0
            common_idx = close.index.intersection(bench_close.index)
            if len(common_idx) >= 55:
                rs_line = close.loc[common_idx] / bench_close.loc[common_idx]
                rs_sma50 = rs_line.rolling(50).mean()
                if not pd.isna(rs_sma50.iloc[-1]) and rs_sma50.iloc[-1] > 0:
                    mansfield_rs = float(((rs_line.iloc[-1] / rs_sma50.iloc[-1]) - 1.0) * 100.0)
                rs_new_20d_high = bool(rs_line.iloc[-1] >= rs_line.iloc[-21:-1].max())
                rs_10d = float(
                    ((close.loc[common_idx[-1]] / close.loc[common_idx[-11]] - 1.0)
                     - (bench_close.loc[common_idx[-1]] / bench_close.loc[common_idx[-11]] - 1.0)) * 100.0
                )

            # HARD DISQUALIFIERS: Distribution, supply rejection wicks, or overextended chases
            dist_to_ema10 = ((curr_price - ema10) / ema10) * 100.0
            dist_to_ema20 = ((curr_price - ema20) / ema20) * 100.0
            dist_to_ema50 = ((curr_price - ema50) / ema50) * 100.0

            if (
                (day_change < -1.0 and rvol > 1.1 and clv < 0.35)
                or (rvol > 1.4 and upper_wick > 0.45 and clv < 0.35)
                or (dist_to_ema10 > 6.5)
                or (dist_to_ema20 > 9.0)
            ):
                continue

            is_uptrend = curr_price >= ema20 and ema20 >= ema50
            is_fast_uptrend = curr_price >= ema10 and ema10 >= ema20 and ema20 >= ema50
            prev_20d_high = float(high.iloc[-21:-1].max())
            high_52w = float(high.max())
            dist_to_52w = ((high_52w - curr_price) / curr_price) * 100.0

            # Session-specific breakout thresholds
            bo_min_rvol = 1.60 if session_mode == "MORNING" else 1.35
            bo_min_clv = 0.70 if session_mode == "MORNING" else 0.65

            setup_type: Optional[str] = None
            setup_priority = 0.0
            rs_score = max(min(mansfield_rs * 1.5, 15.0), -8.0) + (6.0 if rs_new_20d_high else 0.0)

            # --- 5 Clean, Non-Shadowing Short-Term Swing Tiers ---
            # Tier 1: VCP Tight Coil above rising 10/20 EMA (supply dry-up before thrust)
            if (
                is_fast_uptrend
                and (0.0 <= dist_to_ema20 <= 4.0)
                and (atr_compression <= 0.78)
                and (prior_3d_close_spread <= 0.028)
                and (prior_3d_rvol <= 0.78)
                and (clv >= 0.50)
            ):
                setup_type = "VCP Tight Coil"
                setup_priority = 92.0 + (1.0 - atr_compression) * 10.0 + rs_score

            # Tier 2: 20D Structural Breakout (disabled in PULLBACK / RISK_OFF regimes)
            elif (
                regime["code"] == "BULLISH"
                and is_uptrend
                and (curr_price >= prev_20d_high)
                and is_green
                and (rvol >= bo_min_rvol)
                and (clv >= bo_min_clv)
                and (body_ratio >= 0.40)
                and (dist_to_ema20 <= 8.0)
            ):
                setup_type = "20D Structural Breakout"
                setup_priority = 86.0 + min(rvol, 4.0) * 3.5 + rs_score

            # Tier 3: 10/20 EMA Institutional Bounce (high-volume green demand reversal)
            elif (
                is_uptrend
                and (0.0 <= dist_to_ema20 <= 3.5)
                and (curr_low <= ema10 * 1.01 or curr_low <= ema20 * 1.015)
                and is_green
                and (rvol >= 1.0)
                and (clv >= 0.60)
            ):
                setup_type = "20 EMA Inst. Bounce"
                setup_priority = 82.0 + min(rvol, 3.0) * 3.0 + rs_score

            # Tier 4: 20 EMA Dry-Up Pullback (low-vol digestion — never shadowed by Tier 3)
            elif (
                is_uptrend
                and (0.0 <= dist_to_ema20 <= 3.0)
                and (rvol <= 0.80)
                and (clv >= 0.35)
                and (day_change >= -1.8)
            ):
                setup_type = "20 EMA Dry-Up Pullback"
                setup_priority = 74.0 + (1.0 - rvol) * 6.0 + rs_score

            # Tier 5: 50 EMA Support Defense (disabled in RISK_OFF)
            elif (
                regime["code"] != "RISK_OFF"
                and (curr_price >= ema50)
                and (0.0 <= dist_to_ema50 <= 3.0)
                and is_green
                and (clv >= 0.55)
                and (rvol >= 0.90)
                and (mansfield_rs > -2.0)
            ):
                setup_type = "50 EMA Support Defense"
                setup_priority = 66.0 + min(rvol, 2.5) * 2.5 + rs_score

            if not setup_type:
                continue

            # --- Regime-Adaptive Gating ---
            if regime["code"] == "PULLBACK" and mansfield_rs <= 0.0:
                continue
            if regime["code"] == "RISK_OFF" and not (is_fast_uptrend and rs_new_20d_high):
                continue

            # BTST Session Bonus / Filter (3:03 PM IST scan favors strong closes near day high)
            btst_ready = is_green and upper_wick <= 0.18 and day_change >= 1.2 and rvol >= 1.15
            if session_mode == "BTST" and btst_ready:
                setup_priority += 4.0

            # --- Gap-Aware Structural Stop-Loss ---
            is_gap_up = curr_open > prev_high
            if is_gap_up:
                # Anchor below today's intraday demand instead of floating inside unfilled gap
                struct_stop = curr_low - (0.15 * atr14)
                min_stop_dist = 0.8 * atr14
            else:
                # Prior 5-day swing low excluding today, or today's low if tighter structure
                prior_swing_low = float(low.iloc[-6:-1].min())
                struct_stop = min(prior_swing_low, curr_low) - (0.20 * atr14)
                min_stop_dist = 1.0 * atr14

            atr_floor_stop = curr_price - min_stop_dist
            atr_max_stop = curr_price - (2.0 * atr14)
            stop_loss = round(max(min(struct_stop, atr_floor_stop), atr_max_stop), 2)

            # Avoid placing SL above today's low on a normal candle
            if stop_loss >= curr_low and (curr_price - curr_low) <= 2.2 * atr14:
                stop_loss = round(curr_low - (0.10 * atr14), 2)

            shares, risk_per_share = compute_fee_aware_size(
                curr_price, stop_loss, effective_risk_budget, MAX_CAPITAL_PER_TRADE
            )
            if shares < 1 or risk_per_share <= 0:
                continue

            # Split targets tailored for 2-10 day short-term swings
            target_1 = round(curr_price + (risk_per_share * 1.5), 2)
            target_2 = round(curr_price + (risk_per_share * 2.5), 2)

            # Next-day buy-stop trigger for EOD runs (prevents chasing opening gap-ups)
            buy_trigger = round(curr_high + 0.10, 2) if session_mode == "EOD" else None
            overhead_52w = (
                round(high_52w, 2)
                if (0.2 <= dist_to_52w <= 2.5 and setup_type != "20D Structural Breakout")
                else None
            )

            candidates.append({
                "symbol": clean_name,
                "sector": sector_map.get(clean_name, "Other"),
                "price": round(curr_price, 2),
                "day_change": round(day_change, 2),
                "setup": setup_type,
                "priority": round(setup_priority, 2),
                "ema10": round(ema10, 2),
                "ema20": round(ema20, 2),
                "ema50": round(ema50, 2),
                "rvol": rvol,
                "atr_pct": round(atr_pct, 2),
                "mansfield_rs": round(mansfield_rs, 1),
                "rs_10d": round(rs_10d, 1),
                "rs_new_high": rs_new_20d_high,
                "stop_loss": stop_loss,
                "risk_per_share": risk_per_share,
                "t1": target_1,
                "t2": target_2,
                "buy_trigger": buy_trigger,
                "max_shares": shares,
                "btst_ready": btst_ready,
                "overhead_52w": overhead_52w,
                "earnings_warn": None,
                "state_tag": "NEW",
            })
        except Exception as e:
            telemetry["failed_tickers"] += 1
            logging.warning(f"Error scanning {sym}: {e}")

    telemetry["qualified"] = len(candidates)
    candidates.sort(key=lambda x: x["priority"], reverse=True)

    # Sector diversity cap (max 2 stocks per sector)
    final_top: list[dict] = []
    sector_counts: dict[str, int] = {}
    for c in candidates:
        sec = c["sector"]
        if sector_counts.get(sec, 0) < MAX_PER_SECTOR:
            final_top.append(c)
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(final_top) == MAX_CANDIDATES:
            break

    telemetry["latest_prices"] = latest_prices
    return final_top, regime, telemetry


# ---------------------------------------------------------------------------
# 7. Cross-Run State Persistence (MORNING -> BTST -> EOD continuity)
# ---------------------------------------------------------------------------

def load_previous_state() -> dict:
    """Load previous scan state from cache if it belongs to today (IST)."""
    if not STATE_FILE.exists():
        return {}
    try:
        payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        if payload.get("date") == today_str:
            return payload
    except Exception as e:
        logging.warning(f"Could not read prior state file: {e}")
    return {}


def reconcile_and_save_state(
    candidates: list[dict],
    latest_prices: dict[str, float],
    session_mode: str,
) -> list[str]:
    """
    Tag candidates as NEW vs HELD relative to earlier runs today,
    detect any earlier picks that breached their stop-loss, and save updated state.
    """
    prev = load_previous_state()
    prev_picks: dict = prev.get("picks", {})
    sl_breaches: list[str] = []

    now_str = datetime.now(IST).strftime("%H:%M")
    for c in candidates:
        sym = c["symbol"]
        if sym in prev_picks:
            first_seen = prev_picks[sym].get("first_seen", now_str)
            c["state_tag"] = f"HELD since {first_seen}"
        else:
            c["state_tag"] = "NEW"

    # Check if any earlier pick from today breached its stop-loss
    for sym, info in prev_picks.items():
        curr_p = latest_prices.get(sym)
        sl = info.get("stop_loss")
        if curr_p is not None and sl is not None and curr_p <= sl:
            sl_breaches.append(f"{sym} (₹{curr_p} ≤ SL ₹{sl})")

    # Persist merged picks for today
    merged_picks = dict(prev_picks)
    for c in candidates:
        sym = c["symbol"]
        if sym not in merged_picks:
            merged_picks[sym] = {
                "first_seen": now_str,
                "first_mode": session_mode,
                "entry_ref": c["price"],
                "stop_loss": c["stop_loss"],
                "setup": c["setup"],
            }

    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(
            json.dumps(
                {
                    "date": datetime.now(IST).strftime("%Y-%m-%d"),
                    "last_mode": session_mode,
                    "updated_ist": now_str,
                    "picks": merged_picks,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception as e:
        logging.warning(f"Failed to save state file: {e}")

    return sl_breaches


# ---------------------------------------------------------------------------
# 8. Telegram Report Builder & Safe Chunked Dispatcher
# ---------------------------------------------------------------------------

def build_telegram_report(
    candidates: list[dict],
    regime: dict,
    flow: dict,
    news: list[str],
    telemetry: dict,
    universe_src: str,
    sl_breaches: list[str],
) -> str:
    """Build an HTML-safe Telegram report optimized for short-term swing execution."""
    now_ist = datetime.now(IST).strftime("%d %b %Y | %I:%M %p IST")
    mode = telemetry["mode"]
    mode_badge = {
        "MORNING": "☀️ MORNING MOMENTUM",
        "BTST": "⚡ PRE-CLOSE / BTST",
        "EOD": "🌙 EOD CONFIRMED (NEXT-DAY PLAN)",
    }.get(mode, mode)

    msg = f"<b>🚀 TRADEFLOW SWING SCANNER v4.0</b>\n"
    msg += f"<i>{now_ist} | {mode_badge}</i>\n"
    msg += f"<b>📈 REGIME:</b> {html.escape(regime['label'])}\n\n"

    # 1. Institutional Flow
    msg += "<b>🏛️ INSTITUTIONAL FLOW (FII / DII)</b>\n"
    fii = flow["fii_net"]
    dii = flow["dii_net"]
    if fii is not None or dii is not None:
        if fii is not None:
            fii_s = f"+₹{fii:,.1f} Cr" if fii >= 0 else f"-₹{abs(fii):,.1f} Cr"
            msg += f"• <b>FII Net:</b> {fii_s}\n"
        if dii is not None:
            dii_s = f"+₹{dii:,.1f} Cr" if dii >= 0 else f"-₹{abs(dii):,.1f} Cr"
            msg += f"• <b>DII Net:</b> {dii_s}\n"
        msg += (
            f"• <b>Bias:</b> {html.escape(str(flow['sentiment']))} "
            f"<i>({html.escape(str(flow['date']))} · {html.escape(str(flow['source']))})</i>\n\n"
        )
    else:
        msg += f"• <b>Status:</b> {html.escape(str(flow['sentiment']))}\n\n"

    # 2. Earlier-Session SL Breach Alert (if any)
    if sl_breaches:
        msg += "<b>🚨 EARLIER PICKS — SL BREACHED TODAY</b>\n"
        msg += f"• {html.escape(', '.join(sl_breaches))}\n\n"

    # 3. Top Short-Term Swing Candidates
    eff_risk = MAX_LOSS_BUDGET * regime["risk_scale"]
    msg += f"<b>🎯 TOP {len(candidates)} SHORT-TERM SWINGS (2–10D)</b>\n"
    if candidates:
        for i, c in enumerate(candidates, 1):
            sign = "+" if c["day_change"] >= 0 else ""
            cap_needed = int(c["max_shares"] * c["price"])
            tag_icon = "🆕" if c["state_tag"] == "NEW" else "🔄"
            rs_badge = "🔥RS-Hi" if c["rs_new_high"] else f"MRS {c['mansfield_rs']:+.1f}%"
            btst_tag = " | <b>BTST✓</b>" if (mode == "BTST" and c["btst_ready"]) else ""

            msg += (
                f"<b>{i}. {html.escape(c['symbol'])}</b> ({html.escape(c['sector'])}) "
                f"— <b>₹{c['price']}</b> ({sign}{c['day_change']}%) [{tag_icon} {html.escape(c['state_tag'])}]\n"
            )
            msg += (
                f"   ⚡ <i>{html.escape(c['setup'])}</i>{btst_tag} "
                f"[10E: ₹{c['ema10']} | 20E: ₹{c['ema20']}]\n"
            )
            msg += (
                f"   📊 <b>RVOL:</b> {c['rvol']}x | <b>{rs_badge}</b> "
                f"(10D: {c['rs_10d']:+.1f}%) | <b>ATR:</b> {c['atr_pct']}%\n"
            )
            if c["buy_trigger"]:
                msg += f"   🎯 <b>Trigger:</b> Buy > ₹{c['buy_trigger']} (skip if gap &gt;0.75 ATR)\n"
            msg += (
                f"   🛑 <b>SL:</b> ₹{c['stop_loss']} (-₹{c['risk_per_share']}) | "
                f"<b>T1 (1.5R):</b> ₹{c['t1']} | <b>T2:</b> ₹{c['t2']} / 10E Trail\n"
            )
            msg += f"   🛡️ <b>Size:</b> {c['max_shares']} sh (~₹{cap_needed:,}, fee-adj)\n"

            warnings: list[str] = []
            if c.get("earnings_warn"):
                warnings.append(f"Earnings {c['earnings_warn']}")
            if c.get("overhead_52w"):
                warnings.append(f"52W High @ ₹{c['overhead_52w']}")
            if warnings:
                msg += f"   ⚠️ <i>{html.escape(' · '.join(warnings))}</i>\n"
            msg += "\n"
    else:
        msg += "<i>No high-probability short-term setups meet regime + liquidity filters today. Cash is a position!</i>\n\n"

    # 4. Market News
    msg += "<b>📰 KEY MARKET HEADLINES</b>\n"
    for h in news:
        msg += f"• {html.escape(h)}\n"
    msg += "\n"

    # 5. Risk Rules & Health Telemetry
    msg += "<b>⚠️ EXECUTION RULES</b>\n"
    msg += (
        f"• <i>Active Risk: ₹{eff_risk:.0f} (base ₹{MAX_LOSS_BUDGET:.0f} × {regime['risk_scale']}x) "
        f"| Cap: ₹{MAX_CAPITAL_PER_TRADE:,.0f} | Book 50% at T1, move SL to Cost.</i>\n"
    )
    msg += (
        f"<i>⚙️ {html.escape(universe_src)} | OK: {telemetry['downloaded']}/{telemetry['requested']} "
        f"| Liquid: {telemetry['liquid_eligible']} | Qual: {telemetry['qualified']} "
        f"| VolMult: {telemetry['vol_mult']}x</i>"
    )
    return msg


def _split_telegram_html(text: str, max_len: int = 3950) -> list[str]:
    """Split long Telegram messages cleanly along paragraph boundaries."""
    if len(text) <= max_len:
        return [text]
    chunks: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) <= max_len:
            current = candidate
        else:
            if current:
                chunks.append(current)
            current = block[:max_len]
    if current:
        chunks.append(current)
    return chunks


def send_telegram_message(token: str, chat_id: str, text: str) -> bool:
    """Send HTML-formatted message(s) to Telegram with length-safe chunking."""
    chunks = _split_telegram_html(text)
    if not token or not chat_id:
        logging.warning("Telegram Bot Token or Chat ID not provided. Printing preview.")
        for idx, chunk in enumerate(chunks, 1):
            print(f"\n--- TELEGRAM PREVIEW ({idx}/{len(chunks)}) ---\n{chunk}\n------------------------")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    all_ok = True
    for idx, chunk in enumerate(chunks, 1):
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            resp = requests.post(url, json=payload, timeout=15)
            if resp.status_code == 200:
                logging.info(f"Telegram chunk {idx}/{len(chunks)} dispatched.")
            else:
                logging.error(f"Telegram API error ({resp.status_code}): {resp.text}")
                all_ok = False
        except Exception as e:
            logging.error(f"Failed to dispatch Telegram chunk {idx}: {e}")
            all_ok = False
    return all_ok


# ---------------------------------------------------------------------------
# 9. Main Orchestrator
# ---------------------------------------------------------------------------

def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    now_ist = datetime.now(IST)
    session_mode = detect_session_mode(now_ist)
    vol_mult = 1.0 if session_mode == "EOD" else get_nse_intraday_vol_multiplier(now_ist)

    logging.info(f"Starting TradeFlow Swing Scanner v4.0 | Mode={session_mode} | VolMult={vol_mult}x")

    # Fetch universe + external feeds concurrently
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_univ = pool.submit(fetch_nifty200_universe)
        f_flow = pool.submit(fetch_fii_dii_flow)
        f_news = pool.submit(fetch_market_news, 3)

        symbols, sector_map, universe_src = f_univ.result()
        panels = download_ohlcv_panels([BENCHMARK_SYMBOL] + symbols)
        flow = f_flow.result()
        news = f_news.result()

    candidates, regime, telemetry = scan_swing_setups(
        panels=panels,
        symbols=symbols,
        sector_map=sector_map,
        session_mode=session_mode,
        vol_mult=vol_mult,
    )

    # Enrich shortlisted candidates only (fast concurrent earnings check)
    attach_earnings_warnings(candidates)

    # Reconcile cross-run state (NEW vs HELD + SL breaches)
    sl_breaches = reconcile_and_save_state(
        candidates=candidates,
        latest_prices=telemetry.get("latest_prices", {}),
        session_mode=session_mode,
    )

    logging.info(
        f"Scan complete: {len(candidates)} diversified setups "
        f"(qualified={telemetry['qualified']}, failed={telemetry['failed_tickers']})"
    )

    report = build_telegram_report(
        candidates=candidates,
        regime=regime,
        flow=flow,
        news=news,
        telemetry=telemetry,
        universe_src=universe_src,
        sl_breaches=sl_breaches,
    )
    send_telegram_message(token, chat_id, report)


if __name__ == "__main__":
    main()
