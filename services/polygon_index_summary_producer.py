#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Produce el resumen diario (open, high, low, close) de uno o varios índices
y los publica en el topic Kafka 'market_indices_summary_raw'.
"""
import os, time, json, datetime, requests
from confluent_kafka import Producer

KEY     = os.environ["POLYGON_KEY"]
BROKER  = os.getenv("KAFKA_BROKER", "kafka:9092")
# Lista de índices, p.ej. NDX, SPX
INDICES = os.getenv("INDICES", "NDX,SPX").split(",")

producer = Producer({"bootstrap.servers": BROKER})

while True:
    # Fecha de hoy en formato YYYY-MM-DD
    today = datetime.date.today().strftime("%Y-%m-%d")
    for idx in INDICES:
        # ¡OJO! Hay que anteponer "I:" al símbolo de índice
        symbol = idx if idx.startswith("I:") else f"I:{idx.upper()}"
        url = f"https://api.polygon.io/v1/open-close/{symbol}/{today}"
        params = {"adjusted": "true", "apiKey": KEY}
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            payload = {
                "ticker": idx.upper(),
                "date":   today,
                "open":   data.get("open"),
                "high":   data.get("high"),
                "low":    data.get("low"),
                "close":  data.get("close"),
                # opcional: after_hours si quieres
                "after_hours": data.get("afterHours")
            }
            producer.produce("market_indices_summary_raw",
                             value=json.dumps(payload).encode("utf-8"))
            print(f"▶ IndexSummary: {idx} → O:{payload['open']} H:{payload['high']} L:{payload['low']} C:{payload['close']}")
        except Exception as e:
            print("❌", e)
    producer.flush()
    # Ejecuta una vez al día (86400 s). Ajusta si quieres frecuencia distinta.
    time.sleep(86400)
