#!/usr/bin/env python
"""
Cada 5 min (o el intervalo que prefieras) llama a /v3/reference/dividends
y publica los últimos dividendos en el topic 'market_dividends_raw'.
"""
import os, time, json, requests
from confluent_kafka import Producer

KEY     = os.environ["POLYGON_KEY"]
BROKER  = os.getenv("KAFKA_BROKER", "kafka:9092")
TICKERS = os.getenv("TICKERS", "AAPL,MSFT,TSLA,AMZN,GOOGL,KO").split(",")
URL     = "https://api.polygon.io/v3/reference/dividends"

producer = Producer({"bootstrap.servers": BROKER})

def fetch_dividends(tkr: str, limit: int = 10):
    params = {
        "ticker": tkr,
        "limit":  limit,
        "apiKey": KEY
    }
    r = requests.get(URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json().get("results", [])

while True:
    try:
        for tkr in TICKERS:
            for div in fetch_dividends(tkr, limit=5):
                payload = {
                    "ticker":             div.get("ticker"),
                    "ex_dividend_date":   div.get("ex_dividend_date"),
                    "payment_date":       div.get("payment_date"),
                    "record_date":        div.get("record_date"),
                    "declaration_date":   div.get("declaration_date"),
                    "cash_amount":        div.get("cash_amount"),
                    "frequency":          div.get("frequency"),
                }
                producer.produce(
                    "market_dividends_raw",
                    value=json.dumps(payload).encode("utf-8"),
                )
            producer.flush()
        time.sleep(300)  # 5 minutos
    except KeyboardInterrupt:
        break
    except Exception as e:
        print("❌ Dividend producer error:", e)
        time.sleep(60)
