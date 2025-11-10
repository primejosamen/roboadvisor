import os, time, requests, functools

API = "https://api.polygon.io/v3/reference/tickers/"
KEY = os.getenv("POLYGON_API_KEY")
TTL = 24 * 3600                          # 24 h

_cache: dict[str, tuple[float, dict]] = {}

def polygon_get(symbol: str) -> dict:
    sym = symbol.upper()

    # ① caché en memoria
    if sym in _cache and time.time() - _cache[sym][0] < TTL:
        return _cache[sym][1]

    # ② llamada HTTP
    url = f"{API}{sym}?apiKey={KEY}"
    r = requests.get(url, timeout=5)
    r.raise_for_status()
    data = r.json()["results"]

    # ③ guarda en caché
    _cache[sym] = (time.time(), data)
    return data
