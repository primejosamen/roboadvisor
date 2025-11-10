#!/usr/bin/env python
"""
Cada 70 s llama a /v2/aggs/ticker/{TICKER}/prev (endpoint FREE)
y publica el OHLC en el topic 'market_prices_raw'.
"""
import os, time, json, datetime, requests
from confluent_kafka import Producer

API   = "https://api.polygon.io/v2/aggs/ticker/{tkr}/range/1/minute/{frm}/{to}"
KEY   = os.environ["POLYGON_KEY"]
TICKS = os.getenv("TICKERS", "AAPL,MSFT,KO").split(",")
BROKER= os.getenv("KAFKA_BROKER", "kafka:9092")
producer = Producer({"bootstrap.servers": BROKER})

def to_ms(iso):
    dt = datetime.datetime.fromisoformat(iso.replace("Z","+00:00"))
    return int(dt.timestamp()*1000)

while True:
    try:
        today = datetime.date.today().strftime("%Y-%m-%d")
        for tkr in TICKS:
            # Aquí le decimos exactamente qué es cada cosa
            url = API.format(
                tkr = tkr,
                frm = today,
                to  = today
            )
            resp = requests.get(
                url,
                params={
                    "adjusted": "true",
                    "sort":     "desc",
                    "limit":    1,
                    "apiKey":   KEY
                },
                timeout=10
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                # fallback al cierre diario
                prev = requests.get(
                    f"https://api.polygon.io/v2/aggs/ticker/{tkr}/prev",
                    params={"adjusted":"true","apiKey":KEY},
                    timeout=10
                )
                prev.raise_for_status()
                bar = prev.json()["results"][0]
            else:
                bar = results[0]

            payload = {
                "ticker": tkr,
                "ts":      bar["t"],
                "open":    bar["o"],
                "high":    bar["h"],
                "low":     bar["l"],
                "close":   bar["c"],
                "volume":  bar["v"],
            }
            producer.produce(
                "market_prices_raw",
                value=json.dumps(payload).encode()
            )
            print("▶ Price:", tkr, payload["close"])
        producer.flush()
        time.sleep(70)
    except KeyboardInterrupt:
        break
    except Exception as e:
        print("❌", e)
        time.sleep(30)
