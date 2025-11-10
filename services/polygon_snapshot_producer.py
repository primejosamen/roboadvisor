#!/usr/bin/env python
"""
Produce snapshots de precio diferido (15 min) para cada ticker y los publica
en 'market_snapshots_raw'.
"""
import os, time, json, requests
from confluent_kafka import Producer

KEY     = os.environ["POLYGON_KEY"]
BROKER  = os.getenv("KAFKA_BROKER", "kafka:9092")
TICKERS = os.getenv("TICKERS","AAPL,MSFT,TSLA,KO").split(",")
URL     = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{}"

producer = Producer({"bootstrap.servers": BROKER})

while True:
    try:
        for t in TICKERS:
            resp = requests.get(URL.format(t), params={"apiKey": KEY}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            snap = data.get("ticker")
            if not snap:
                print(f"❌ Sin campo 'ticker' en snapshot para {t}: {data}")
                continue

            # extrae los campos con .get() para no KeyError
            last_trade = snap.get("lastTrade", {})
            last_quote = snap.get("lastQuote", {})
            day_info   = snap.get("day", {})
            min_info   = snap.get("min", {})

            price = last_trade.get("p")
            if price is None:
                print(f"❌ Sin lastTrade.p para {t}: {snap}")
                continue

            bid = last_quote.get("p")
            ask = last_quote.get("p")  # o None si no existe
            volume = day_info.get("v")
            ts = min_info.get("t")

            payload = {
                "ticker": t,
                "ts":      ts,
                "price":   price,
                "bid":     bid,
                "ask":     ask,
                "volume":  volume
            }
            producer.produce("market_snapshots_raw",
                             value=json.dumps(payload).encode())
            print(f"▶ Snap: {t} price={price} bid={bid} ask={ask} vol={volume}")
        producer.flush()
        time.sleep(60)
    except KeyboardInterrupt:
        break
    except Exception as e:
        print("❌ Error en snapshot producer:", e)
        time.sleep(30)
