import os, time, requests, statistics
from datetime import datetime, timezone

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]
ALPHA_KEY    = os.environ["ALPHA_VANTAGE_KEY"]

PAIRS = [
    "EURUSD","GBPUSD","USDJPY","USDCHF","AUDUSD","NZDUSD","USDCAD",
    "EURJPY","EURGBP","EURCHF","AUDJPY","CHFJPY","GBPJPY","GBPCHF",
    "AUDCAD","EURCAD","AUDNZD"
]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

AV_BASE = "https://www.alphavantage.co/query"

def notion_query_existing():
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    pair_to_page = {}
    payload = {}
    while True:
        r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        for res in data.get("results", []):
            title = res["properties"]["Name"]["title"]
            name = "".join(t["plain_text"] for t in title).strip().upper()
            if name:
                pair_to_page[name] = res["id"]
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]
    return pair_to_page

def notion_upsert(row, page_id=None):
    props = {
        "Name": {"title": [{"text": {"content": row["Name"]}}]},
        "Current Price": {"number": row["Current Price"]},
        "Daily High": {"number": row["Daily High"]},
        "Daily Low": {"number": row["Daily Low"]},
        "10-Day High": {"number": row["10-Day High"]},
        "10-Day Low": {"number": row["10-Day Low"]},
        "BB Upper": {"number": row["BB Upper"]},
        "BB Lower": {"number": row["BB Lower"]},
        "Updated At": {"date": {"start": row["Updated At"]}},
        "Flags": {"multi_select": [{"name": f} for f in row["Flags"]]},
    }
    if page_id:
        url = f"https://api.notion.com/v1/pages/{page_id}"
        requests.patch(url, headers=NOTION_HEADERS, json={"properties": props}, timeout=60).raise_for_status()
    else:
        url = "https://api.notion.com/v1/pages"
        payload = {"parent": {"database_id": NOTION_DB_ID}, "properties": props}
        requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=60).raise_for_status()

def av_call(params):
    r = requests.get(AV_BASE, params=params, timeout=60)
    r.raise_for_status()
    # Alpha Vantage free tier: 5 requests/min — throttle per request
    time.sleep(13)  # ~4.6 req/min; safe
    return r.json()

def realtime_price(pair):
    p = {"function":"CURRENCY_EXCHANGE_RATE","from_currency":pair[:3],"to_currency":pair[3:],"apikey":ALPHA_KEY}
    data = av_call(p)
    return float(data["Realtime Currency Exchange Rate"]["5. Exchange Rate"])

def intraday_today_highlow(pair):
    p = {"function":"FX_INTRADAY","from_symbol":pair[:3],"to_symbol":pair[3:],"interval":"5min","outputsize":"compact","apikey":ALPHA_KEY}
    data = av_call(p).get("Time Series FX (5min)", {})
    if not data:
        return None, None
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    highs, lows = [], []
    for ts, bar in data.items():
        if ts.startswith(today):
            highs.append(float(bar["2. high"])); lows.append(float(bar["3. low"]))
    if not highs:  # if no bars yet today, fallback to recent
        for bar in data.values():
            highs.append(float(bar["2. high"])); lows.append(float(bar["3. low"]))
    return max(highs), min(lows)

def daily_series(pair, limit=30):
    p = {"function":"FX_DAILY","from_symbol":pair[:3],"to_symbol":pair[3:],"outputsize":"compact","apikey":ALPHA_KEY}
    data = av_call(p).get("Time Series FX (Daily)", {})
    # newest first in API; we keep the most recent 'limit'
    ordered = list(sorted(data.items(), reverse=True))[:limit]
    closes = [float(v["4. close"]) for _, v in ordered]
    highs  = [float(v["2. high"])  for _, v in ordered]
    lows   = [float(v["3. low"])   for _, v in ordered]
    return closes, highs, lows

def bollinger(closes, period=20, mult=2.0):
    if len(closes) < period: return None, None
    window = list(reversed(closes[:period]))
    mean = sum(window)/period
    # population std dev
    var = sum((x-mean)**2 for x in window)/period
    std = var**0.5
    return round(mean + mult*std, 6), round(mean - mult*std, 6)

def main():
    existing = notion_query_existing()
    for pair in PAIRS:
        try:
            price = realtime_price(pair)
            d_hi, d_lo = intraday_today_highlow(pair)
            closes, highs, lows = daily_series(pair, 30)
            ten_hi, ten_lo = max(highs[:10]), min(lows[:10])
            bb_u, bb_l = bollinger(closes, period=20, mult=2.0)

            flags = []
            if bb_u and price >= bb_u: flags.append("At/Above Upper BB")
            if bb_l and price <= bb_l: flags.append("At/Below Lower BB")
            if abs(price - ten_hi)/max(ten_hi,1e-9) < 0.001: flags.append("Near 10-Day High")
            if abs(price - ten_lo)/max(ten_lo,1e-9) < 0.001: flags.append("Near 10-Day Low")

            row = {
                "Name": pair,
                "Current Price": round(price, 6),
                "Daily High": round(d_hi, 6) if d_hi else None,
                "Daily Low": round(d_lo, 6) if d_lo else None,
                "10-Day High": round(ten_hi, 6),
                "10-Day Low": round(ten_lo, 6),
                "BB Upper": bb_u,
                "BB Lower": bb_l,
                "Updated At": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
                "Flags": flags,
            }
            notion_upsert(row, existing.get(pair))
        except Exception as e:
            print(f"[{pair}] ERROR:", e)

if __name__ == "__main__":
    main()
