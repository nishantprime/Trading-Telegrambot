"""
TradeFlow - Upgraded Swing Scanner & Telegram Alert Dispatcher (v2.0)
Features:
  1. Top 7 Liquid Swing Candidates across 28 high-volume NSE leaders
  2. Multi-tier Setup Engine:
     - 20 EMA Pullback Bounce (Golden Swing Entry)
     - High-Volume Momentum Continuation (Breakout)
     - 50 EMA Support Defense (Swing Reversal)
     - Relative Strength Momentum Outperformer (Leading Nifty)
  3. Automatic Position Sizing to enforce the ₹600 max loss rule (Rule 1)
  4. Real-time FII & DII Institutional Flows with cloud fallback
  5. Live Macro Headlines from Indian Financial Markets
Dispatches clean executive summary to Telegram via Bot API.
"""

import os
import sys
import json
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("Error: Missing yfinance or pandas. Run: pip install yfinance pandas")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# 28 High-Volume Liquid Momentum & Swing Leaders on NSE (All Verified)
WATCHLIST_SYMBOLS = [
    # User's Core Watchlist
    "TATASTEEL.NS", "BHEL.NS", "JSWINFRA.NS", "COALINDIA.NS",
    "NMDC.NS", "AMBUJACEM.NS", "PARADEEP.NS", "MOTISONS.NS",
    # Core Metals & Commodity Cyclicals
    "SAIL.NS", "HINDALCO.NS", "NATIONALUM.NS", "JINDALSTEL.NS", "VEDL.NS",
    # High-Beta Defense, Infra & Railways
    "BEL.NS", "HAL.NS", "IRFC.NS", "RVNL.NS",
    # Power & Energy Capex Momentum
    "TATAPOWER.NS", "NTPC.NS", "POWERGRID.NS", "GMRAIRPORT.NS", "ADANIPORTS.NS",
    # Liquid Momentum & Platform Leaders (including ETERNAL / formerly Zomato)
    "ETERNAL.NS", "DELHIVERY.NS", "POLICYBZR.NS", "SBIN.NS", "ICICIBANK.NS",
    "RELIANCE.NS", "BHARTIARTL.NS"
]

BENCHMARK_SYMBOL = "^NSEI"  # Nifty 50 Index

# Max loss per trade budget (Enforces Rule 1)
MAX_LOSS_BUDGET = 600.0  # In Rupees (₹600)


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
        else:
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
        root = ET.fromstring(resp.content)
        items = root.findall(".//item")
        for item in items[:limit]:
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
    """
    Fetch official daily FII / DII net flow data directly from NSE India with robust fallback.
    """
    flow_data = {
        "fii_net": 0.0,
        "dii_net": 0.0,
        "date": datetime.now().strftime("%d %b"),
        "sentiment": "Neutral",
        "source": "NSE Official"
    }

    # Attempt 1: Direct NSE session scrape
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/reports/fii-dii"
        })

        session.get("https://www.nseindia.com", timeout=8)
        resp = session.get("https://www.nseindia.com/api/fiidiiTradeReact", timeout=8)

        if resp.status_code == 200:
            data = resp.json()
            for item in data:
                cat = item.get("category", "")
                net_val = float(item.get("netValue", "0").replace(",", ""))
                dt = item.get("date", flow_data["date"])
                flow_data["date"] = dt
                if "FII" in cat:
                    flow_data["fii_net"] = net_val
                elif "DII" in cat:
                    flow_data["dii_net"] = net_val
            logging.info(f"NSE FII/DII successfully scraped: {flow_data['date']}")
    except Exception as e:
        logging.warning(f"Direct NSE FII/DII failed: {e}. Trying fallback...")

    # Attempt 2: If values are 0 (e.g. weekend or cloud IP blocked), parse financial news feed
    if flow_data["fii_net"] == 0.0 and flow_data["dii_net"] == 0.0:
        try:
            feed_url = "https://news.google.com/rss/search?q=FII+DII+cash+market+data+crores+NSE&hl=en-IN&gl=IN&ceid=IN:en"
            resp = requests.get(feed_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
            root = ET.fromstring(resp.content)
            items = root.findall(".//item")
            for it in items[:3]:
                t = it.find("title").text
                if "FII" in t and "crore" in t.lower():
                    flow_data["sentiment"] = f"Recent Flow: {t[:80]}..."
                    flow_data["source"] = "Financial Wire"
                    break
        except Exception:
            pass

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
            flow_data["sentiment"] = "⚪ Neutral / Weekend Market Flow"

    return flow_data


def scan_swing_setups() -> list:
    """
    Downloads daily data for the 28 liquid stocks and ranks the Top 7 candidates across 4 tiers:
      Tier 1: 20 EMA Pullback Bounce (Golden Swing Entry)
      Tier 2: High-Volume Momentum Continuation (Breakout)
      Tier 3: 50 EMA Support Defense (Swing Reversal)
      Tier 4: Relative Strength Outperformer (Leading Nifty)
    """
    candidates = []

    # Benchmark return
    nifty_5d_return = 0.0
    try:
        bench_df = yf.download(BENCHMARK_SYMBOL, period="1mo", interval="1d", progress=False)
        if len(bench_df) >= 6:
            nifty_close = bench_df["Close"].squeeze()
            nifty_5d_return = ((nifty_close.iloc[-1] - nifty_close.iloc[-6]) / nifty_close.iloc[-6]) * 100.0
    except Exception as e:
        logging.warning(f"Benchmark fetch failed: {e}")

    logging.info(f"Scanning {len(WATCHLIST_SYMBOLS)} liquid momentum candidates...")

    for sym in WATCHLIST_SYMBOLS:
        clean_name = sym.replace(".NS", "")
        try:
            df = yf.download(sym, period="3mo", interval="1d", progress=False)
            if df.empty or len(df) < 50:
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

            # Technical indicators
            ema20 = close.ewm(span=20, adjust=False).mean()
            ema50 = close.ewm(span=50, adjust=False).mean()
            curr_ema20 = float(ema20.iloc[-1])
            curr_ema50 = float(ema50.iloc[-1])

            # Volume average (20 days)
            avg_vol20 = float(vol.iloc[-21:-1].mean())
            curr_vol = float(vol.iloc[-1])
            rvol = round(curr_vol / avg_vol20, 2) if avg_vol20 > 0 else 1.0

            # Intraday Candle Geometry (Rejection wick & green vs red)
            is_green = curr_price >= curr_open
            range_span = float(high.iloc[-1]) - float(low.iloc[-1])
            wick_ratio = (curr_price - float(low.iloc[-1])) / range_span if range_span > 0 else 0.5

            # 5-day Relative Strength vs Nifty
            stock_5d_return = ((curr_price - float(close.iloc[-6])) / float(close.iloc[-6])) * 100.0 if len(close) >= 6 else 0.0
            rs_score = stock_5d_return - nifty_5d_return

            # ATR for Stop-loss sizing (14-day)
            tr = pd.concat([
                high - low,
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs()
            ], axis=1).max(axis=1)
            atr14 = float(tr.rolling(14).mean().iloc[-1])

            # HARD FILTER: Block High-Volume Sell-offs / Knife Falling
            # If a stock is dumping (-1% or worse) with high volume (> 1.1x) and closing on its lows (wick < 0.35),
            # it is Institutional Liquidation/Distribution - NEVER buy into heavy selling!
            if (day_change < -1.0) and (rvol > 1.1) and (wick_ratio < 0.35):
                logging.info(f"Disqualified {clean_name}: Heavy distribution (Day: {day_change:.2f}%, RVOL: {rvol}x)")
                continue

            # Trend & Distances
            is_uptrend = curr_price >= curr_ema20 and curr_ema20 >= curr_ema50
            dist_to_ema20 = ((curr_price - curr_ema20) / curr_ema20) * 100.0
            dist_to_ema50 = ((curr_price - curr_ema50) / curr_ema50) * 100.0

            setup_type = None
            setup_priority = 0

            # Tier 1: 20 EMA Pullback
            if is_uptrend and (0.0 <= dist_to_ema20 <= 3.5):
                if is_green or wick_ratio >= 0.35:
                    setup_type = "20 EMA Reversal Bounce"
                    setup_priority = 85 + (1.0 / max(rvol, 0.2)) * 3 + rs_score
                elif rvol <= 0.85:
                    setup_type = "20 EMA Low-Vol Pullback"
                    setup_priority = 75 + (1.0 / max(rvol, 0.2)) * 3 + rs_score

            # Tier 2: High-Volume Momentum Continuation (Breakout)
            elif is_uptrend and rvol >= 1.2 and (day_change >= 0.4):
                setup_type = "High-Volume Momentum Breakout"
                setup_priority = 80 + (rvol * 5) + rs_score

            # Tier 3: 50 EMA Support Defense
            elif (curr_price >= curr_ema50) and (abs(dist_to_ema50) <= 3.5):
                if is_green or wick_ratio >= 0.35:
                    setup_type = "50 EMA Support Defense"
                    setup_priority = 65 + (1.0 / max(rvol, 0.2)) * 3 + rs_score
                elif rvol <= 0.85:
                    setup_type = "50 EMA Low-Vol Contraction"
                    setup_priority = 55 + (1.0 / max(rvol, 0.2)) * 3 + rs_score

            # Tier 4: Strong Relative Strength Outperformer
            elif rs_score > 3.0 and (curr_price >= curr_ema50) and (day_change >= -0.5 or rvol <= 0.85):
                setup_type = "Relative Strength Leader"
                setup_priority = 45 + rs_score

            if setup_type:
                # Stop loss at recent 4-day low or 1.25x ATR below entry
                recent_swing_low = float(low.iloc[-4:].min())
                stop_loss = round(max(recent_swing_low, curr_price - (1.25 * atr14)), 2)
                risk_per_share = round(curr_price - stop_loss, 2)

                if risk_per_share > 0:
                    target_1 = round(curr_price + (risk_per_share * 2.0), 2)  # 1:2 Risk/Reward
                    max_allowed_shares = int(MAX_LOSS_BUDGET / risk_per_share)

                    candidates.append({
                        "symbol": clean_name,
                        "price": round(curr_price, 2),
                        "day_change": round(day_change, 2),
                        "setup": setup_type,
                        "priority": setup_priority,
                        "ema20": round(curr_ema20, 2),
                        "ema50": round(curr_ema50, 2),
                        "rvol": rvol,
                        "rs": round(rs_score, 1),
                        "stop_loss": stop_loss,
                        "risk_per_share": risk_per_share,
                        "target": target_1,
                        "max_shares": max_allowed_shares
                    })
        except Exception as e:
            logging.debug(f"Error scanning {sym}: {e}")
            continue

    # Sort candidates strictly by priority score
    candidates.sort(key=lambda x: x["priority"], reverse=True)
    return candidates[:7]  # TOP 7 HIGHEST QUALITY SETUPS


def build_telegram_report(candidates: list, flow: dict, news: list) -> str:
    """Build a crisp, executive Telegram HTML message with Top 7 setups."""
    now_ist = datetime.now(timezone.utc).strftime("%d %b %Y | %H:%M UTC")

    fii_str = f"+₹{flow['fii_net']:,.1f} Cr" if flow['fii_net'] >= 0 else f"-₹{abs(flow['fii_net']):,.1f} Cr"
    dii_str = f"+₹{flow['dii_net']:,.1f} Cr" if flow['dii_net'] >= 0 else f"-₹{abs(flow['dii_net']):,.1f} Cr"

    msg = f"<b>🚀 TRADEFLOW SWING SCANNER</b>\n"
    msg += f"<i>{now_ist} | Market Radar</i>\n\n"

    # 1. FII / DII Flow
    msg += f"<b>🏛️ INSTITUTIONAL FLOW (FII / DII)</b>\n"
    if flow['fii_net'] != 0.0 or flow['dii_net'] != 0.0:
        msg += f"• <b>FII Net:</b> {fii_str}\n"
        msg += f"• <b>DII Net:</b> {dii_str}\n"
        msg += f"• <b>Regime:</b> {flow['sentiment']} <i>({flow['date']})</i>\n\n"
    else:
        msg += f"• <b>Status:</b> {flow['sentiment']}\n"
        msg += f"• <i>(Official cash market data updates on trading day evenings)</i>\n\n"

    # 2. Top 7 Swing Candidates
    msg += f"<b>🎯 TOP 7 SWING CANDIDATES</b>\n"
    if candidates:
        for i, c in enumerate(candidates, 1):
            sign = "+" if c['day_change'] >= 0 else ""
            msg += f"<b>{i}. {c['symbol']} — ₹{c['price']}</b> ({sign}{c['day_change']}%)\n"
            msg += f"   ⚡ <i>{c['setup']}</i> [20 EMA: ₹{c['ema20']} | 50 EMA: ₹{c['ema50']}]\n"
            msg += f"   📊 <b>RVOL:</b> {c['rvol']}x | <b>RS:</b> {c['rs']}\n"
            msg += f"   🛑 <b>SL:</b> ₹{c['stop_loss']} (Risk: ₹{c['risk_per_share']}) | 🎯 <b>Target:</b> ₹{c['target']}\n"
            msg += f"   🛡️ <b>Max Size:</b> {c['max_shares']} shares <i>(₹{MAX_LOSS_BUDGET:.0f} max risk rule)</i>\n\n"
    else:
        msg += f"<i>No low-risk setups meeting strict confluence today. Cash is also a position!</i>\n\n"

    # 3. Market News
    msg += f"<b>📰 KEY MARKET HEADLINES</b>\n"
    for h in news:
        msg += f"• {h}\n"
    msg += "\n"

    # 4. Discipline Reminder
    msg += f"<b>⚠️ DISCIPLINE REMINDER</b>\n"
    msg += f"• <i>Never risk > ₹600 per trade.</i>\n"
    msg += f"• <i>Zero averaging down on red positions.</i>\n"

    return msg


def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    logging.info("Starting TradeFlow Upgraded Swing Scan (Top 7 Mode)...")

    flow = fetch_fii_dii_flow()
    news = fetch_market_news(limit=3)
    candidates = scan_swing_setups()

    logging.info(f"Identified {len(candidates)} swing candidates.")

    report = build_telegram_report(candidates, flow, news)
    send_telegram_message(token, chat_id, report)
    print("\nScan execution complete.")


if __name__ == "__main__":
    main()
