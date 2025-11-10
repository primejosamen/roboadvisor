import os, time, json, requests
from confluent_kafka import Producer

API = os.environ["FINNHUB_KEY"]
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
URL = "https://finnhub.io/api/v1/news"

p = Producer({"bootstrap.servers": BROKER})

def delivery(err, msg):
    if err: print("❌", err)

while True:
    try:
        arts = requests.get(URL, params={"category":"general","token":API},
                            timeout=10).json()
        for a in arts:
            key = str(a["id"])
            p.produce("market_news_raw", key=key.encode(),
                      value=json.dumps(a).encode(), callback=delivery)
        p.flush()
        print(f"▲  enviados {len(arts)} artículos")
    except Exception as e:
        print("Error Finnhub:", e)
    time.sleep(3600)           # cada hora
