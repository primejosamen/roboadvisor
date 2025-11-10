#!/usr/bin/env python
"""
Lanza cada 90 s una llamada al endpoint FREE /v2/reference/news de Polygon
y publica las noticias nuevas en el topic Kafka 'market_news_raw'.
El formato JSON es el mismo que espera tu start_news_indexer().
"""
import os, json, time, requests, datetime
from confluent_kafka import Producer

API_KEY  = os.environ["POLYGON_KEY"]
BROKER   = os.getenv("KAFKA_BROKER", "kafka:9092")
TICKERS  = os.getenv("TICKERS", "AAPL,MSFT,KO").split(",")

producer = Producer({"bootstrap.servers": BROKER})

# Guarda el id del último artículo por ticker para no duplicar
last_seen = {t: None for t in TICKERS}

def iso_to_epoch_ms(iso_ts: str) -> int:
    dt = datetime.datetime.fromisoformat(iso_ts.replace("Z","+00:00"))
    return int(dt.timestamp() * 1000)

def fetch_news(ticker: str):
    url = f"https://api.polygon.io/v2/reference/news"
    params = {"ticker": ticker, "order": "desc",
              "limit": 10, "apiKey": API_KEY}
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("results", [])

def produce(article: dict):
    payload = {
        "headline": article["title"],
        "summary":  article.get("description", ""),
        "datetime": iso_to_epoch_ms(article["published_utc"]),
        "related":  ",".join(article.get("tickers", [])),
        "url":      article["article_url"]
    }
    producer.produce(
        topic="market_news_raw",
        value=json.dumps(payload).encode("utf-8"),
    )

if __name__ == "__main__":
    print("⚡ Polygon producer iniciado…")
    while True:
        try:
            for ticker in TICKERS:
                for art in fetch_news(ticker):
                    if art["id"] == last_seen.get(ticker):
                        break  # ya llegamos a noticias antiguas
                    produce(art)
                # regístralo aunque no lleguen news nuevas
                if fetch_news(ticker):
                    last_seen[ticker] = fetch_news(ticker)[0]["id"]
            producer.flush()
            time.sleep(90)               # 90 s ⇒ < 5 peticiones/min
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("❌  Error:", e)
            time.sleep(30)
