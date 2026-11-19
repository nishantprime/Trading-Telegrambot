"""
TradeFlow - Swing Scanner & Telegram Alert Dispatcher (v3.0)
Upgrades:
  - Vectorized batch yfinance download (1y history for converged 50 EMA & Wilder's ATR)
  - Date-aligned, volatility-normalized Relative Strength vs Nifty 50
  - Intraday volume projection when executed during live NSE hours (09:15 - 15:30 IST)
  - Decoupled tier logic + true 20D structural breakout checks (no bull-trap matching)
  - Structural stop-loss bounded to [1.0 ATR, 2.0 ATR] + per-trade capital cap
  - Sector concentration cap (max 2 stocks per sector)
  - Concurrent I/O + HTML-escaped Telegram payloads
"""

import os
import sys
import html
import logging
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("Error: Missing yfinance or pandas. Run: pip install yfinance pandas")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

IST = timezone(timedelta(hours=5, minutes=30))
BENCHMARK_SYMBOL = "^NSEI"

# Risk & Sizing Parameters
MAX_LOSS_BUDGET = 600.0          # Max loss per trade in Rupees (Rule 1)
MAX_CAPITAL_PER_TRADE = 25000.0  # Max capital deployed per trade (prevents micro-stop leverage spikes)

# 29 High-Volume Liquid NSE Leaders
WATCHLIST_SYMBOLS = [
    # Core Watchlist
    "TATASTEEL.NS", "BHEL.NS", "JSWINFRA.NS", "COALINDIA.NS",
    "NMDC.NS", "AMBUJACEM.NS", "PARADEEP.NS", "MOTISONS.NS",
    # Metals & Commodity Cyclicals
    "SAIL.NS", "HINDALCO.NS", "NATIONALUM.NS", "JINDALSTEL.NS", "VEDL.NS",
    # Defense, Infra & Railways
    "BEL.NS", "HAL.NS", "IRFC.NS", "RVNL.NS",
    # Power & Energy Capex
    "TATAPOWER.NS", "NTPC.NS", "POWERGRID.NS", "GMRAIRPORT.NS", "ADANIPORTS.NS",
    # Liquid Momentum & Platform Leaders
    "ETERNAL.NS", "DELHIVERY.NS", "POLICYBZR.NS", "SBIN.NS", "ICICIBANK.NS",
    "RELIANCE.NS", "BHARTIARTL.NS"
]

SECTOR_MAP = {
    "TATASTEEL": "Metals", "SAIL": "Metals", "HINDALCO": "Metals",
    "NATIONALUM": "Metals", "JINDALSTEL": "Metals", "VEDL": "Metals",
    "NMDC": "Metals", "COALINDIA": "Mining/Energy",
    "BHEL": "Capex/PSU", "BEL": "Defense", "HAL": "Defense",
    "IRFC": "Railways", "RVNL": "Railways",
    "TATAPOWER": "Power", "NTPC": "Power", "POWERGRID": "Power",
    "JSWINFRA": "Infra", "GMRAIRPORT": "Infra", "ADANIPORTS": "Infra",
    "AMBUJACEM": "Cement", "PARADEEP": "Chemicals", "MOTISONS": "Consumer",
    "ETERNAL": "Tech", "DELHIVERY": "Logistics", "POLICYBZR": "Fintech",
    "SBIN": "Banking", "ICICIBANK": "Banking",
    "RELIANCE": "Conglomerate", "BHARTIARTL": "Telecom"
}


def get_intraday_vol_multiplier() -> float:
    """Scale partial intraday volume to a full 375-min NSE session if run during live hours."""
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return 1.0
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if market_open <= now < market_close:
        elapsed_mins = max((now - market_open).total_seconds() / 60.0, 15.0)
        return 375.0 / elapsed_mins
    return 1.0


def send_telegram_message(token: str, chat_id: str, text: str) -> bool:
    """Send HTML-formatted message to Telegram."""
    if not token or not chat_id:
        logging.warning("Telegram Bot Token or Chat ID not provided. Skipping dispatch.")
        print("\n--- TELEGRAM PREVIEW ---\n" + text + "\n------------------------")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200:
            logging.info("Successfully dispatched Telegram alert.")
            return True
        logging.error(f"Telegram API returned {resp.status_code}: {resp.text}")
        return False
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")
        return False


def fetch_market_news(limit: int = 3) -> list:
    """Fetch high-impact market headlines using Google News RSS for Indian Equities."""
    rss_url = "https://news.google.com/rss/search?q=Nifty+Indian+stock+market+economy&hl=en-IN&gl=IN&ceid=IN:en"
    headlines = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get(rss_url, headers=headers, timeout=10)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for item in root.findall(".//item")[:limit]:
            title_el = item.find("title")
            if title_el is not None and title_el.text:
                title = title_el.text.strip()
                clean_title = title.rsplit(" - ", 1)[0] if " - " in title else title
                headlines.append(clean_title)
    except Exception as e:
        logging.warning(f"Could not fetch market news via RSS: {e}")
        headlines = ["Indian market tracking global macro cues and institutional capital flows."]
    return headlines


def fetch_fii_dii_flow() -> dict:
    """Fetch daily FII/DII net flow data from NSE India with RSS fallback."""
    flow_data = {
        "fii_net": 0.0,
        "dii_net": 0.0,
        "date": datetime.now(IST).strftime("%d %b"),
        "sentiment": "Neutral",
        "source": "NSE Official"
    }

    # Attempt 1: Direct NSE session scrape
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/reports/fii-dii"
        })
        session.get("https://www.nseindia.com", timeout=6)
        resp = session.get("https://www.nseindia.com/api/fiidiiTradeReact", timeout=6)

        if resp.status_code == 200:
            for item in resp.json():
                cat = item.get("category", "")
                raw_val = str(item.get("netValue", "0") or "0").replace(",", "").strip()
                if raw_val in ("", "-"):
                    continue
                net_val = float(raw_val)
                flow_data["date"] = item.get("date", flow_data["date"])
                if "FII" in cat:
                    flow_data["fii_net"] = net_val
                elif "DII" in cat:
                    flow_data["dii_net"] = net_val
            logging.info(f"NSE FII/DII scraped: {flow_data['date']}")
    except Exception as e:
        logging.warning(f"Direct NSE FII/DII failed: {e}. Trying RSS fallback...")

    # Attempt 2: Google News RSS fallback (e.g., Azure IP blocked or pre-release)
    if flow_data["fii_net"] == 0.0 and flow_data["dii_net"] == 0.0:
        try:
            feed_url = "https://news.google.com/rss/search?q=FII+DII+cash+market+data+crores+NSE&hl=en-IN&gl=IN&ceid=IN:en"
            resp = requests.get(feed_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            for it in root.findall(".//item")[:5]:
                title_el = it.find("title")
                if title_el is not None and title_el.text:
                    t = title_el.text.strip()
                    if "FII" in t and "crore" in t.lower():
                        flow_data["sentiment"] = f"Recent Flow: {t[:95]}..."
                        flow_data["source"] = "Financial Wire"
                        break
        except Exception as e:
            logging.warning(f"FII/DII RSS fallback failed: {e}")

    fii = flow_data["fii_net"]
    dii = flow_data["dii_net"]

    if flow_data["sentiment"] == "Neutral":
        if fii > 500 and dii > 0:
            flow_data["sentiment"] = "🟢 Strong Confluence (Both Buying)"
        elif fii > 0 and dii <= 0:
            flow_data["sentiment"] = "🟢 FII Inflow Driven (Bullish Momentum)"
        elif fii > 0 and dii > 0:
            flow_data["sentiment"] = "🟢 Net Institutional Inflows"
        elif fii < -1500 and dii > 0:
            flow_data["sentiment"] = "🟡 Choppy (FII Dumping, DII Absorbing)"
        elif fii < 0 and dii < 0:
            flow_data["sentiment"] = "🔴 High Risk / Defensive (Both Selling)"
        else:
            flow_data["sentiment"] = "⚪ Neutral / Pending Official Release"

    return flow_data


def scan_swing_setups() -> tuple[list, str]:
    """
    Batch-downloads 1 year of daily NSE data and ranks Top 7 swing setups
    with sector diversification (max 2 per sector).
    Returns: (top_7_candidates, nifty_regime_string)
    """
    all_symbols = [BENCHMARK_SYMBOL] + WATCHLIST_SYMBOLS
    logging.info(f"Batch-downloading 1y daily data for {len(all_symbols)} tickers...")

    data = yf.download(
        all_symbols,
        period="1y",
        interval="1d",
        group_by="ticker",
        threads=True,
        progress=False
    )

    if data.empty:
        logging.error("yfinance returned an empty DataFrame.")
        return [], "Unknown"

    available_tickers = set(data.columns.get_level_values(0))
    bench_close = pd.Series(dtype=float)
    nifty_regime = "Neutral"

    if BENCHMARK_SYMBOL in available_tickers:
        bench_df = data[BENCHMARK_SYMBOL].dropna(subset=["Close"])
        bench_close = bench_df["Close"].squeeze()
        if len(bench_close) >= 60:
            n_ema20 = float(bench_close.ewm(span=20, adjust=False).mean().iloc[-1])
            n_ema50 = float(bench_close.ewm(span=50, adjust=False).mean().iloc[-1])
            n_price = float(bench_close.iloc[-1])
            if n_price >= n_ema20 and n_ema20 >= n_ema50:
                nifty_regime = f"🟢 Bullish Trend (Nifty {n_price:,.0f} > 20/50 EMA)"
            elif n_price >= n_ema50:
                nifty_regime = f"🟡 Pullback / Range (Nifty {n_price:,.0f} > 50 EMA)"
            else:
                nifty_regime = f"🔴 Risk-Off (Nifty {n_price:,.0f} < 50 EMA — Trade Smaller)"

    vol_mult = get_intraday_vol_multiplier()
    candidates = []

    for sym in WATCHLIST_SYMBOLS:
        if sym not in available_tickers:
            continue

        clean_name = sym.replace(".NS", "")
        try:
            df = data[sym].dropna(subset=["Close", "High", "Low", "Open", "Volume"])
            if len(df) < 120:
                continue

            close = df["Close"].squeeze()
            open_p = df["Open"].squeeze()
            high = df["High"].squeeze()
            low = df["Low"].squeeze()
            vol = df["Volume"].squeeze()

            curr_price = float(close.iloc[-1])
            curr_open = float(open_p.iloc[-1])
            prev_price = float(close.iloc[-2])
            day_change = ((curr_price - prev_price) / prev_price) * 100.0

            # Converged EMAs (1y warm-up)
            ema20 = close.ewm(span=20, adjust=False).mean()
            ema50 = close.ewm(span=50, adjust=False).mean()
            curr_ema20 = float(ema20.iloc[-1])
            curr_ema50 = float(ema50.iloc[-1])

            # Intraday-normalized RVOL
            avg_vol20 = float(vol.iloc[-21:-1].mean())
            curr_vol = float(vol.iloc[-1]) * vol_mult
            rvol = round(curr_vol / avg_vol20, 2) if avg_vol20 > 0 else 1.0

            # Candle geometry
            is_green = curr_price >= curr_open
            range_span = float(high.iloc[-1]) - float(low.iloc[-1])
            wick_ratio = (curr_price - float(low.iloc[-1])) / range_span if range_span > 0 else 0.5

            # Wilder's ATR (14)
            tr = pd.concat([
                high - low,
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs()
            ], axis=1).max(axis=1)
            atr14 = float(tr.ewm(alpha=1 / 14, adjust=False).mean().iloc[-1])
            if pd.isna(atr14) or atr14 <= 0:
                continue
            atr_pct = (atr14 / curr_price) * 100.0

            # Date-aligned 5-day Relative Strength vs Nifty
            raw_rs = 0.0
            common_idx = close.index.intersection(bench_close.index)
            if len(common_idx) >= 6:
                s_now, s_prev = float(close.loc[common_idx[-1]]), float(close.loc[common_idx[-6]])
                b_now, b_prev = float(bench_close.loc[common_idx[-1]]), float(bench_close.loc[common_idx[-6]])
                raw_rs = (((s_now - s_prev) / s_prev) - ((b_now - b_prev) / b_prev)) * 100.0
            norm_rs = raw_rs / max(atr_pct, 1.0)

            # HARD FILTER: Block heavy distribution & high-volume upper-wick traps
            if (day_change < -1.0 and rvol > 1.1 and wick_ratio < 0.35) or (rvol > 1.5 and wick_ratio < 0.25):
                logging.info(f"Disqualified {clean_name}: Distribution/Trap (Day: {day_change:.2f}%, RVOL: {rvol}x, Wick: {wick_ratio:.2f})")
                continue

            is_uptrend = curr_price >= curr_ema20 and curr_ema20 >= curr_ema50
            dist_to_ema20 = ((curr_price - curr_ema20) / curr_ema20) * 100.0
            dist_to_ema50 = ((curr_price - curr_ema50) / curr_ema50) * 100.0
            prev_20d_high = float(high.iloc[-21:-1].max())

            setup_type = None
            setup_priority = 0.0

            # Decoupled tier checks (no inner-if shadowing)
            if is_uptrend and (0.0 <= dist_to_ema20 <= 3.5) and (is_green or wick_ratio >= 0.40):
                setup_type = "20 EMA Reversal Bounce"
                setup_priority = 85.0 + (1.0 / max(rvol, 0.3)) * 3.0 + norm_rs * 2.0
            elif is_uptrend and (0.0 <= dist_to_ema20 <= 3.5) and (rvol <= 0.85) and (wick_ratio >= 0.25):
                setup_type = "20 EMA Low-Vol Pullback"
                setup_priority = 75.0 + (1.0 / max(rvol, 0.3)) * 3.0 + norm_rs * 2.0
            elif is_uptrend and rvol >= 1.3 and curr_price >= prev_20d_high and is_green and wick_ratio >= 0.65 and dist_to_ema20 <= 8.0:
                setup_type = "20D Structural Breakout"
                setup_priority = 82.0 + (min(rvol, 4.0) * 4.0) + norm_rs * 2.0
            elif (curr_price >= curr_ema50) and (0.0 <= dist_to_ema50 <= 3.5) and is_green and wick_ratio >= 0.40:
                setup_type = "50 EMA Support Defense"
                setup_priority = 65.0 + norm_rs * 2.0
            elif raw_rs > 3.5 and is_uptrend and (day_change >= -0.5) and (dist_to_ema20 <= 6.0):
                setup_type = "Relative Strength Leader"
                setup_priority = 50.0 + norm_rs * 2.0

            if setup_type:
                # Structural stop below 5-day swing low with 0.25 ATR buffer, bounded to [1.0 ATR, 2.0 ATR]
                swing_low_stop = float(low.iloc[-5:].min()) - (0.25 * atr14)
                atr_floor_stop = curr_price - (1.0 * atr14)
                atr_max_stop = curr_price - (2.0 * atr14)

                raw_stop = min(swing_low_stop, atr_floor_stop)
                stop_loss = round(max(raw_stop, atr_max_stop), 2)
                risk_per_share = round(curr_price - stop_loss, 2)

                if risk_per_share > 0:
                    target_1 = round(curr_price + (risk_per_share * 2.0), 2)
                    risk_shares = int(MAX_LOSS_BUDGET / risk_per_share)
                    cap_shares = int(MAX_CAPITAL_PER_TRADE / curr_price)
                    max_allowed_shares = min(risk_shares, cap_shares)

                    if max_allowed_shares >= 1:
                        candidates.append({
                            "symbol": clean_name,
                            "sector": SECTOR_MAP.get(clean_name, "Other"),
                            "price": round(curr_price, 2),
                            "day_change": round(day_change, 2),
                            "setup": setup_type,
                            "priority": setup_priority,
                            "ema20": round(curr_ema20, 2),
                            "ema50": round(curr_ema50, 2),
                            "rvol": rvol,
                            "rs": round(raw_rs, 1),
                            "stop_loss": stop_loss,
                            "risk_per_share": risk_per_share,
                            "target": target_1,
                            "max_shares": max_allowed_shares
                        })
        except Exception as e:
            logging.debug(f"Error scanning {sym}: {e}")

    candidates.sort(key=lambda x: x["priority"], reverse=True)

    # Sector diversity cap: max 2 stocks per sector
    final_top_7 = []
    sector_counts = {}
    for c in candidates:
        sec = c["sector"]
        if sector_counts.get(sec, 0) < 2:
            final_top_7.append(c)
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(final_top_7) == 7:
            break

    return final_top_7, nifty_regime


def build_telegram_report(candidates: list, nifty_regime: str, flow: dict, news: list) -> str:
    """Build an HTML-safe Telegram report with Top 7 diversified setups."""
    now_ist = datetime.now(IST).strftime("%d %b %Y | %I:%M %p IST")

    fii_str = f"+₹{flow['fii_net']:,.1f} Cr" if flow['fii_net'] >= 0 else f"-₹{abs(flow['fii_net']):,.1f} Cr"
    dii_str = f"+₹{flow['dii_net']:,.1f} Cr" if flow['dii_net'] >= 0 else f"-₹{abs(flow['dii_net']):,.1f} Cr"

    safe_sentiment = html.escape(str(flow["sentiment"]))
    safe_date = html.escape(str(flow["date"]))
    safe_regime = html.escape(nifty_regime)

    msg = "<b>🚀 TRADEFLOW SWING SCANNER v3.0</b>\n"
    msg += f"<i>{now_ist} | Market Radar</i>\n"
    msg += f"<b>📈 INDEX:</b> {safe_regime}\n\n"

    # 1. FII / DII Flow
    msg += "<b>🏛️ INSTITUTIONAL FLOW (FII / DII)</b>\n"
    if flow["fii_net"] != 0.0 or flow["dii_net"] != 0.0:
        msg += f"• <b>FII Net:</b> {fii_str}\n"
        msg += f"• <b>DII Net:</b> {dii_str}\n"
        msg += f"• <b>Regime:</b> {safe_sentiment} <i>({safe_date})</i>\n\n"
    else:
        msg += f"• <b>Status:</b> {safe_sentiment}\n"
        msg += "• <i>(Official cash data updates around 6:00–6:45 PM IST)</i>\n\n"

    # 2. Top 7 Swing Candidates
    msg += "<b>🎯 TOP 7 SWING CANDIDATES</b>\n"
    if candidates:
        for i, c in enumerate(candidates, 1):
            sign = "+" if c["day_change"] >= 0 else ""
            cap_needed = int(c["max_shares"] * c["price"])
            msg += f"<b>{i}. {c['symbol']} ({c['sector']}) — ₹{c['price']}</b> ({sign}{c['day_change']}%)\n"
            msg += f"   ⚡ <i>{c['setup']}</i> [20E: ₹{c['ema20']} | 50E: ₹{c['ema50']}]\n"
            msg += f"   📊 <b>RVOL:</b> {c['rvol']}x | <b>5D RS:</b> {c['rs']:+.1f}%\n"
            msg += f"   🛑 <b>SL:</b> ₹{c['stop_loss']} (Risk: ₹{c['risk_per_share']}) | 🎯 <b>T1:</b> ₹{c['target']}\n"
            msg += f"   🛡️ <b>Max Size:</b> {c['max_shares']} sh (~₹{cap_needed:,})\n\n"
    else:
        msg += "<i>No low-risk setups meeting strict confluence today. Cash is a position!</i>\n\n"

    # 3. Market News (HTML-escaped)
    msg += "<b>📰 KEY MARKET HEADLINES</b>\n"
    for h in news:
        msg += f"• {html.escape(h)}\n"
    msg += "\n"

    # 4. Discipline Reminder
    msg += "<b>⚠️ DISCIPLINE REMINDER</b>\n"
    msg += f"• <i>Max risk ₹{MAX_LOSS_BUDGET:.0f} | Max capital ₹{MAX_CAPITAL_PER_TRADE:,.0f} per trade.</i>\n"
    msg += "• <i>Zero averaging down on red positions.</i>\n"

    return msg


def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    logging.info("Starting TradeFlow Swing Scan v3.0...")

    with ThreadPoolExecutor(max_workers=3) as executor:
        f_flow = executor.submit(fetch_fii_dii_flow)
        f_news = executor.submit(fetch_market_news, 3)
        f_scan = executor.submit(scan_swing_setups)

        flow = f_flow.result()
        news = f_news.result()
        candidates, nifty_regime = f_scan.result()

    logging.info(f"Identified {len(candidates)} diversified swing candidates.")

    report = build_telegram_report(candidates, nifty_regime, flow, news)
    send_telegram_message(token, chat_id, report)
    print("\nScan execution complete.")


if __name__ == "__main__":
    main()
