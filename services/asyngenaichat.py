#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
asyngenaichat.py  –  RAG + OpenAI Function-Calling
"""
# ───────────── IMPORTS ─────────────
from typing import Optional
import os
import re
import json
import datetime as _dt
from collections import defaultdict
from datetime import date, timedelta
from typing import List, Tuple
import sys
import numpy as np
import pandas as pd
import requests
from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.admin import AdminClient, NewTopic
from elasticsearch import Elasticsearch
from langchain.chains import RetrievalQA
from langchain.prompts import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
)
from langchain.retrievers import EnsembleRetriever
from langchain_elasticsearch import ElasticsearchStore
from langchain_openai import ChatOpenAI
from langchain_openai.embeddings import OpenAIEmbeddings
from langchain.schema import BaseRetriever
from typing import Any
from pydantic import Field
import duckdb, pandas as pd, math, json, requests

import ccloud_lib

# ───────────── CONFIG ─────────────
TZ             = os.getenv("TZ", "America/Guayaquil")   # para manipular zonas horarios puede estar o no
ELASTIC_URL    = os.getenv("ELASTIC_URL", "http://localhost:9200")
POLYGON_KEY    = os.environ["POLYGON_KEY"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
LAST_SOURCES   = []

# ─────────── FUNCIONES DE PRE-FLIGHT ───────────
def ensure_es_index(es: Elasticsearch, index: str, body: dict):
    if es.indices.exists(index=index):
        current = es.indices.get_mapping(index=index)[index]["mappings"]
        desired = body["mappings"]
        if current != desired:
            print(f"Índice '{index}' existe con mapping diferente.")
        else:
            print(f"Índice '{index}' ya existe correctamente mapeado.")
    else:
        es.indices.create(index=index, body=body)
        print(f" Índice '{index}' creado.")

def ensure_kafka_topics(admin_conf: dict, topics: list[NewTopic]):
    admin = AdminClient(admin_conf)
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [t for t in topics if t.topic not in existing]
    if to_create:
        fs = admin.create_topics(to_create, request_timeout=15)
        for topic, f in fs.items():
            try:
                f.result()
                print(f" Topic Kafka '{topic}' creado.")
            except Exception as e:
                print(f" No se pudo crear topic '{topic}': {e}")
    else:
        print("✅ Todos los topics Kafka ya existían.")


try:
    EMBEDDINGS = OpenAIEmbeddings(model="text-embedding-3-small", dimensions=512)
except Exception as e:
    print("⚠️  Error creando OpenAIEmbeddings:", e)
    raise

_SESSION_MEMORY: dict[str, dict] = {}

from langchain.schema import HumanMessage, AIMessage, SystemMessage, BaseMessage

MAX_TURNS = 10

def _state(session_id: str) -> dict:
    """Devuelve (y crea si no existe) el contenedor de estado de la sesión."""
    return _SESSION_MEMORY.setdefault(session_id, {"history": [], "scratch": {}})


def _mk_msg(role: str, content: str) -> BaseMessage:
    if role == "user":
        return HumanMessage(content=content)
    elif role == "assistant":
        return AIMessage(content=content)
    else:           # system / tool
        return SystemMessage(content=content)

def push_history(session_id: str, role: str, content: str):
    msg = _mk_msg(role, content)
    st  = _state(session_id)
    st["history"].append(msg)
    st["history"] = st["history"][-MAX_TURNS*2:]        # recorta

def get_history(session_id: str):
    return _state(session_id)["history"]

# -------------- score threshold ---------------
class ScoreFilterRetriever(BaseRetriever):
    base: BaseRetriever = Field(...)   # ① campo obligatorio
    threshold: float    = 0.15         # ② campo con default


    def _get_relevant_documents(self, query: str):
        docs = self.base.get_relevant_documents(query)
        return [d for d in docs if d.metadata.get("score", 1.0) >= self.threshold]

    async def _aget_relevant_documents(self, query: str):
        docs = await self.base.aget_relevant_documents(query)
        return [d for d in docs if d.metadata.get("score", 1.0) >= self.threshold]

    class Config:                      # ③ deja que Pydantic acepte otros attrs
        arbitrary_types_allowed = True
# ------------------------------------

# ───────────────────── 3. BOOTSTRAP / FACTORY FNS ───────────────────

def build_elasticsearch() -> Elasticsearch:
    try:
        es = Elasticsearch(hosts=[ELASTIC_URL])
        es.info()                     # sanity-check
        return es
    except Exception as e:
        print("⚠️  No se pudo conectar a Elasticsearch:", e)
        raise

def build_tools() -> list[dict]:

    return [
        {
            "type": "function",
            "function": {
                "name": "get_correlation",
                "description": (
                    "Devuelve la correlación de Pearson entre dos tickers usando "
                    "retornos diarios ajustados.  Rango por defecto: último año."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker1":    { "type": "string", "description": "Primer ticker, p.ej. AAPL" },
                        "ticker2":    { "type": "string", "description": "Segundo ticker, p.ej. SPY" },
                        "start_date": { "type": "string", "description": "YYYY-MM-DD (opcional)" },
                        "end_date":   { "type": "string", "description": "YYYY-MM-DD (opcional)" }
                    },
                    "required": ["ticker1", "ticker2"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_price_on_date",
                "description": "Precio de cierre ajustado de un ticker en una fecha (YYYY-MM-DD).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": { "type": "string", "description": "Ticker, p.ej. AAPL" },
                        "date":   { "type": "string", "description": "Fecha ISO, p.ej. 2025-07-01" }
                    },
                    "required": ["ticker", "date"]
                }
            }
        },

        {
            "type": "function",
            "function": {
                "name": "get_price",
                "description": "Devuelve el último precio (delay 15 min) de una acción estadounidense.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {
                            "type": "string",
                            "description": "Ticker de la acción, p. ej. TSLA",
                        }
                    },
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_dividends",
                "description": "Devuelve los dividendos históricos de un ticker.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string", "description": "Ticker, p.ej. AAPL"},
                        "limit": {
                            "type": "integer",
                            "description": "Cuántos registros traer (default 5)",
                        },
                    },
                    "required": ["ticker"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_index_summary",
                "description": "Devuelve el resumen OHLC diario de un índice en una fecha dada.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {
                            "type": "string",
                            "description": "Ticker del índice, p.ej. SPX o NDX",
                        },
                        "date": {
                            "type": "string",
                            "description": "Fecha en formato YYYY-MM-DD",
                        },
                    },
                    "required": ["ticker", "date"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_top_movers",
                "description": "Devuelve los tickers que más subieron y más cayeron en un rango de fechas.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "start_date": {
                            "type": "string",
                            "description": "Fecha inicio YYYY-MM-DD",
                        },
                        "end_date": {"type": "string", "description": "Fecha fin   YYYY-MM-DD"},
                        "top_n": {
                            "type": "integer",
                            "description": "Cuántos top movers devolver",
                        },
                    },
                    "required": ["start_date", "end_date"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_top_volume",
                "description": "Devuelve los tickers con mayor volumen en un rango de fechas.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "start_date": {
                            "type": "string",
                            "description": "Fecha inicio YYYY-MM-DD",
                        },
                        "end_date": {"type": "string", "description": "Fecha fin   YYYY-MM-DD"},
                        "top_n": {
                            "type": "integer",
                            "description": "Cuántos devolver",
                            "default": 5,
                        },
                    },
                    "required": ["start_date", "end_date"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_portfolio_beta",
                "description": "Calcula la beta de un portafolio dado sus tickers, pesos y rango de fechas.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "tickers": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Lista de tickers, p.ej. ['AAPL','MSFT']",
                        },
                        "weights": {
                            "type": "array",
                            "items": {"type": "number"},
                            "description": "Lista de pesos (% normalizados a 1), p.ej. [0.2,0.3]",
                        },
                        "start_date": {
                            "type": "string",
                            "description": "Fecha inicio en formato YYYY-MM-DD",
                        },
                        "end_date": {
                            "type": "string",
                            "description": "Fecha fin en formato YYYY-MM-DD",
                        },
                    },
                    "required": ["tickers", "weights", "start_date", "end_date"],
                },
            },
        },

    ]
import unicodedata

CLASES = {
    "total", "subtotal",
    "rotación anual", "rotacion anual",
    "rv", "rf", "real assets",
    "multi estrategia", "private equity",
    "private debt",
    "líquido", "liquido",
    "renta fija", "renta variable",
    "alternativo", "mercados privados"
}

_HEADER  = re.compile(r"Inventario\s*%", re.I)

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return s.lower().strip()

_line_pct = re.compile(
    r"""^[\s•\-–\*]* 
        (?P<nombre>.+?)\s+                 
        (?P<pct>\d{1,3}(?:[.,]\d{1,2})?)\s*%  
        .*?$                               
    """,
    re.I | re.X
)

def extrae_porcentaje_por_activo(chunks: list[str]) -> dict[str, float]:
    out = {}
    for ch in chunks:
        inside = False
        for ln in ch.splitlines():
            if _HEADER.search(ln):
                inside = True          # comienza la tabla
                continue
            if inside and not ln.strip():
                break                  # línea en blanco → fin de tabla
            if not inside:
                continue

            m = _line_pct.match(ln)
            if not m:
                continue

            nombre = re.sub(r"\s+\$.*", "", m.group("nombre")).strip()
            if (_norm(nombre) in CLASES or                   # clases excluidas
                    re.match(r"^[\d$]", nombre)):                # líneas rotas
                continue

            pct = float(m.group("pct").replace(",", "."))
            if 0 < pct <= 100:
                out[nombre] = pct       # sobrescribe si aparece de nuevo
    return out

def build_llm(tools: list[dict]) -> ChatOpenAI:
    try:
        return ChatOpenAI(
            api_key=OPENAI_API_KEY,
            model="gpt-4o",
            temperature=0.75,
            max_tokens   = 1024,
            model_kwargs={"tools": tools, "tool_choice": "auto"},
            verbose=True,
        )
    except Exception as e:
        print("⚠️  Error inicializando ChatOpenAI:", e)
        raise


def build_prompt() -> ChatPromptTemplate:
    system = SystemMessagePromptTemplate.from_template(
        """Eres un analista financiero.
        
        • Si la pregunta implica comparar meses, sumar columnas o calcular máximos
          de la tabla normalizada, Llama a la función `query_table`
          con la sintaxis SQL adecuada y espera su respuesta.
        • En otro caso responde con los documentos recuperados.
        Hoy es {today}.
        """
    )
    human = HumanMessagePromptTemplate.from_template("{input}")
    return ChatPromptTemplate.from_messages([system, human])


def build_store(index: str) -> ElasticsearchStore:
    return ElasticsearchStore(
        es_url=ELASTIC_URL,
        index_name=index,
        embedding=EMBEDDINGS,
        query_field="content",
        vector_query_field="vector",
        distance_strategy="COSINE",
    )


def build_retrievers(llm: ChatOpenAI) -> tuple[RetrievalQA, RetrievalQA, RetrievalQA]:
    # --- stores base --------------------------------------------------

    vec_pdf = build_store("docs_chatbotres_v3")   # PDFs / chunks
    vec_ext = build_store("ext_docs_v1")          # noticias

    es = build_elasticsearch()
    try:
        tbl_count = es.count(index="tbl_docs_v1")["count"]
    except Exception:
        tbl_count = 0

    vec_tbl: Optional[ElasticsearchStore] = None
    if tbl_count > 0:
        vec_tbl = build_store("tbl_docs_v1")

    pdf_ret = vec_pdf.as_retriever(search_type="similarity",search_kwargs={"k":15, "fetch_k":30})
    ext_ret = vec_ext.as_retriever(k=10)

    tbl_ret = None
    if vec_tbl:
        tbl_ret = vec_tbl.as_retriever(search_type="similarity",search_kwargs={"k":40, "fetch_k":40})

    pdf_ret = ScoreFilterRetriever(base=pdf_ret, threshold=0.18)
    if tbl_ret:
        tbl_ret = ScoreFilterRetriever(base=tbl_ret, threshold=0.18)

    ens_retrievers = [pdf_ret, ext_ret]
    ens_weights    = [0.8   , 0.2   ]          # sin tabla: PDF domina

    if tbl_ret:
        ens_retrievers.insert(1, tbl_ret)      # pdf, tbl, ext
        ens_weights    = [0.35, 0.55, 0.10]    # la tabla vuelve a pesar más

    rag_fin = RetrievalQA.from_chain_type(
        llm=llm,
        retriever=EnsembleRetriever(
            retrievers=ens_retrievers,
            weights=ens_weights,
        ),
        return_source_documents=True,
    )

    if tbl_ret:
        rag_port = RetrievalQA.from_chain_type(llm=llm,retriever=tbl_ret,return_source_documents=True)
    else:
        rag_port = RetrievalQA.from_chain_type(llm=llm,retriever=pdf_ret,return_source_documents=True)



    #  ⬅️  devolvemos los mismos tres objetos que ya esperabas
    return rag_fin, rag_port, vec_pdf
def _set_ctx(items, source="computed"):
    """
    items: dict | list[dict|str] | str  -> se serializa a texto
    source: etiqueta (índice, API, etc.)
    """
    if not isinstance(items, list):
        items = [items]
    ctx = []
    for it in items:
        if isinstance(it, dict):
            text = json.dumps(it, ensure_ascii=False)
        else:
            text = str(it)
        ctx.append({"text": text[:2000], "source": source, "page": None})
    globals()["LAST_SOURCES"] = ctx
# ───────────────────────── 4. DOMAIN HELPERS ───────────────────────

def _es_latest(index: str, ticker: str):
    body = {
        "size": 1,
        "sort": [{"ts": {"order": "desc"}}],
        "query": {"term": {"ticker.keyword": ticker.upper()}},
    }
    r = requests.get(f"{ELASTIC_URL}/{index}/_search", json=body, timeout=5)
    hits = r.json().get("hits", {}).get("hits", [])
    return hits[0]["_source"] if hits else None


def get_price(ticker: str) -> str:  # expuesto al LLM
    snap = _es_latest("prices_snap_v1", ticker)
    if snap:
        dt_local = _dt.datetime.fromtimestamp(snap["ts"] / 1000)
        _set_ctx(snap, "prices_snap_v1")
        return f"Precio diferido 15 min de {ticker.upper()}: {snap['price']} USD a las {dt_local:%H:%M:%S}."

    bar = _es_latest("prices_daily_v1", ticker)
    if bar:
        d_local = _dt.datetime.fromtimestamp(bar["ts"] / 1000).date()
        _set_ctx(bar, "prices_daily_v1")
        return f"El último cierre de {ticker.upper()} fue {bar['close']} USD el {d_local}."
    _set_ctx({"ticker": ticker.upper(), "status": "not_found"}, "prices_daily_v1")
    return f"No encuentro datos de {ticker}."

def get_dividends(ticker: str, limit: int = 5) -> str:
    """
    Devuelve los últimos <limit> dividendos de <ticker>.
    Si no hay fecha de pago, omite el paréntesis.
    """
    body = {
        "size": limit,
        "sort": [{"ex_dividend_date": {"order": "desc"}}],
        "query": {"term": {"ticker.keyword": ticker.upper()}},
        "_source": ["ex_dividend_date", "cash_amount", "payment_date"]
    }
    r = requests.get(f"{ELASTIC_URL}/prices_dividends_v1/_search",
                     json=body, timeout=5)
    hits = r.json().get("hits", {}).get("hits", [])

    if not hits:
        _set_ctx({"ticker": ticker.upper(), "dividends": []}, "prices_dividends_v1")
        return f"No encontré dividendos para {ticker.upper()}."
    rows = [h["_source"] for h in hits]
    _set_ctx(rows, "prices_dividends_v1")

    lines = [f"Últimos {len(hits)} dividendos de {ticker.upper()}:"]

    for h in hits:
        d = h["_source"]
        _set_ctx(rows, "prices_dividends_v1")
        ex   = d.get("ex_dividend_date", "—")
        cash = d.get("cash_amount",      "—")
        pay  = d.get("payment_date")         # puede ser None

        if pay:
            lines.append(f" • Ex-dividend: {ex}  →  {cash} USD  (pago: {pay})")
        else:
            lines.append(f" • Ex-dividend: {ex}  →  {cash} USD")

    return "\n".join(lines)


def get_index_summary(ticker: str, date: str) -> str:
    """
    Consulta Elasticsearch en indices_summary_v1 por ID (ticker_date).
    Si no existe, llama al endpoint REST de Polygon para esa fecha.
    """
    # Intento Elasticsearch por ID
    doc_id = f"{ticker.upper()}_{date}"
    es_url = f"{ELASTIC_URL}/indices_summary_v1/_doc/{doc_id}"
    resp = requests.get(es_url, timeout=5)

    if resp.status_code == 200 and "error" not in resp.json():
        s = resp.json().get("_source", {})
        _set_ctx(s, "indices_summary_v1")
        return (
            f"Resumen {ticker.upper()} {date}:\n"
            f" • Apertura:    {s.get('open','—')}\n"
            f" • Máximo:      {s.get('high','—')}\n"
            f" • Mínimo:      {s.get('low','—')}\n"
            f" • Cierre:      {s.get('close','—')}\n"
            f" • After-hours: {s.get('after_hours','—')}"
        )

    # Fallback a Polygon REST
    POLY_KEY = os.environ["POLYGON_KEY"]
    sym = ticker.upper()
    if not (sym.startswith("I:") or sym.startswith("^")):
        sym = f"I:{sym}"
    try:
        r = requests.get(
            f"https://api.polygon.io/v1/open-close/{sym}/{date}",
            params={"adjusted": "true", "apiKey": POLY_KEY},
            timeout=10
        )
        r.raise_for_status()
        d = r.json()
        _set_ctx(d, "polygon:v1/open-close")
        return (
            f"Resumen {ticker.upper()} {date} (API Polygon):\n"
            f" • Apertura:    {d.get('open','—')}\n"
            f" • Máximo:      {d.get('high','—')}\n"
            f" • Mínimo:      {d.get('low','—')}\n"
            f" • Cierre:      {d.get('close','—')}\n"
            f" • After-hours: {d.get('afterHours','—')}"
        )
    except:
        return f"No encontré resumen para {ticker.upper()} el {date}."


def fetch_daily_close(symbol: str, start: str, end: str) -> pd.Series:
    """Trae cierre diario de Polygon y devuelve serie de pandas indexed por fecha."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
    params = {"adjusted":"true","sort":"asc","limit":5000,"apiKey":os.environ["POLYGON_KEY"]}
    r = requests.get(url, params=params, timeout=10); r.raise_for_status()
    results = r.json().get("results", [])
    df = pd.DataFrame(results)
    df['t'] = pd.to_datetime(df['t'], unit='ms').dt.date
    df.set_index('t', inplace=True)
    return df['c']  # columna close

def get_portfolio_beta(tickers: list[str], weights: list[float],
                       start_date: str, end_date: str) -> str:
    # 1) Descargar series de retornos
    prices = {}
    for t in tickers:
        prices[t] = fetch_daily_close(t, start_date, end_date)
    # SPX como benchmark (símbolo I:SPX)
    benchmark = fetch_daily_close("I:SPX", start_date, end_date)

    # 2) Construir DataFrame de retornos logarítmicos
    df = pd.DataFrame(prices).pct_change().dropna()
    mkt = benchmark.pct_change().dropna().reindex(df.index)

    betas = {}
    for t in tickers:
        cov = np.cov(df[t], mkt)[0,1]
        var = np.var(mkt)
        betas[t] = cov/var if var>0 else np.nan

    # 3) Beta de portafolio
    # normalizar pesos si no suman a 1
    w = np.array(weights)/sum(weights)
    beta_p = float(np.dot(w, [betas[t] for t in tickers]))


    # 4) Formatear respuesta
    lines = [f"Beta individual:"]
    for t in tickers:
        lines.append(f" • {t}: {betas[t]:.3f}")
    lines.append(f"\nBeta ponderada del portafolio: {beta_p:.3f}")
    _set_ctx({
        "tickers": tickers, "weights": list(map(float, w)),
        "betas": {k: float(v) for k, v in betas.items()},
        "beta_portafolio": float(beta_p),
        "start": start_date, "end": end_date
    }, "computed:beta")
    return "\n".join(lines)


def get_top_volume(start_date: str, end_date: str, top_n: int = 5) -> str:
    # Convertimos fechas ISO a milisegundos
    start_ms = int(_dt.datetime.fromisoformat(start_date).timestamp() * 1000)
    end_ms   = int(_dt.datetime.fromisoformat(end_date).timestamp() * 1000)

    body = {
        "size": 10000,
        "_source": ["ticker", "ts", "volume"],
        "query": {
            "range": {
                "ts": {"gte": start_ms, "lte": end_ms}
            }
        }
    }
    r = requests.get(f"{ELASTIC_URL}/prices_daily_v1/_search", json=body, timeout=10)
    hits = r.json().get("hits", {}).get("hits", [])


    vol_by_ticker = defaultdict(float)
    for h in hits:
        src = h["_source"]
        vol_by_ticker[src["ticker"]] += src.get("volume", 0)

    sorted_vol = sorted(vol_by_ticker.items(), key=lambda x: x[1], reverse=True)[:top_n]
    _set_ctx([{"ticker": t, "volume": float(v)} for t, v in sorted_vol], "prices_daily_v1")

    lines = [f"Top {top_n} tickers por volumen ({start_date} → {end_date}):"]
    for t, v in sorted_vol:
        lines.append(f" • {t}: {int(v):,} unidades")
    return "\n".join(lines)


def get_top_movers(start_date: str, end_date: str, top_n: int = 5) -> str:
    # convertimos fechas a milisegundos
    start_ms = int(_dt.datetime.fromisoformat(start_date).timestamp() * 1000)
    end_ms   = int(_dt.datetime.fromisoformat(end_date).timestamp() * 1000)

    body = {
        "size": 10000,
        "_source": ["ticker", "ts", "close"],
        "query": {
            "range": {
                "ts": {"gte": start_ms, "lte": end_ms}
            }
        },
        "sort": [{"ticker.keyword": "asc"}, {"ts": "asc"}]
    }
    r = requests.get(f"{ELASTIC_URL}/prices_daily_v1/_search", json=body, timeout=10)
    hits = r.json().get("hits", {}).get("hits", [])


    prices = defaultdict(list)
    for h in hits:
        src = h["_source"]
        # convertimos ts de vuelta a fecha para agrupar cronológicamente
        date = _dt.datetime.fromtimestamp(src["ts"] / 1000).date()
        prices[src["ticker"]].append((date, src["close"]))

    movers = []
    for t, lst in prices.items():
        lst_sorted = sorted(lst, key=lambda x: x[0])
        if len(lst_sorted) < 2:
            continue
        start_close = lst_sorted[0][1]
        end_close   = lst_sorted[-1][1]
        pct = (end_close - start_close) / start_close * 100
        movers.append((t, pct))

    movers.sort(key=lambda x: x[1], reverse=True)
    tops    = movers[:top_n]
    bottoms = movers[-top_n:]

    lines = ["Top ↑ movers:"]
    for t, p in tops:
        lines.append(f" • {t}: +{p:.1f}%")
    lines.append("\nTop ↓ movers:")
    for t, p in bottoms:
        lines.append(f" • {t}: {p:.1f}%")
    _set_ctx([
        {"ticker": t, "pct_change": float(p)} for t, p in (tops + bottoms)
    ], "prices_daily_v1")
    return "\n".join(lines)

def get_price_on_date(ticker: str, date: str) -> str:
    """
    Devuelve el cierre ajustado de ticker>en la fecha ISO YYYY-MM-DD.
    Si no existe en ES lo pide a Polygon y lo almacena en prices_daily_v1.
    """
    tkr = ticker.upper()
    es_id = f"{tkr}_{date.replace('-', '')}"

    # ① ¿Ya lo tengo cacheado?
    es_url = f"{ELASTIC_URL}/prices_daily_v1/_doc/{es_id}"
    res = requests.get(es_url, timeout=5)
    if res.status_code == 200 and "_source" in res.json():
        close = res.json()["_source"]["close"]
        _set_ctx(res.json()["_source"], "prices_daily_v1")
        return f"El cierre de {tkr} el {date} fue **{close:.2f} USD**."

    # ② Polygon custom-bars (1 día)
    url = f"https://api.polygon.io/v2/aggs/ticker/{tkr}/range/1/day/{date}/{date}"
    params = {"adjusted": "true", "limit": 1, "apiKey": os.environ["POLYGON_KEY"]}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results:
        return f"No encontré precio para {tkr} el {date}."

    bar = results[0]
    close = bar["c"]

    # ③ Cachea en ES (opcional pero recomendable)
    doc = {
        "ticker": tkr,
        "ts": int(bar["t"]),          # epoch ms
        "open":  bar["o"],
        "high":  bar["h"],
        "low":   bar["l"],
        "close": close,
        "volume": bar["v"],
    }
    requests.put(es_url, json=doc, timeout=5)   # upsert
    _set_ctx(doc, "prices_daily_v1")

    return f"El cierre de {tkr} el {date} fue **{close:.2f} USD**."

def get_correlation(ticker1: str,
                    ticker2: str,
                    start_date: str | None = None,
                    end_date:   str | None = None) -> str:
    """
    Calcula la correlación de Pearson entre los retornos diarios de
    ticker1 y ticker2 en el rango [start_date, end_date].
    Si no se indica rango se usa el último año .
    """
    end   = _dt.date.fromisoformat(end_date)   if end_date   else _dt.date.today()
    start = _dt.date.fromisoformat(start_date) if start_date else end - timedelta(days=365)

    # 1) series de cierre ajustado
    s1 = fetch_daily_close(ticker1, start.isoformat(), end.isoformat())
    s2 = fetch_daily_close(ticker2, start.isoformat(), end.isoformat())

    df = pd.concat([s1, s2], axis=1).dropna()
    if df.empty:
        return f"No encontré precios suficientes para {ticker1} y {ticker2}."

    df = df.pct_change().dropna()           # retornos %
    corr = df.corr().iloc[0, 1]
    _set_ctx({
        "ticker1": ticker1.upper(), "ticker2": ticker2.upper(),
        "start": start.isoformat(), "end": end.isoformat(),
        "n_obs": int(len(df)), "pearson": float(corr)
    }, "polygon:v2/aggs")

    return (f"Correlación (Pearson) de {ticker1.upper()} y {ticker2.upper()} "
            f"entre {start} y {end}: **{corr:.3f}**")

NAME2TICKER = {
    "apple":      "AAPL",
    "appl":       "AAPL",
    "microsoft":  "MSFT",
    "google":     "GOOGL",
    "alphabet":   "GOOGL",
    "amazon":     "AMZN",
    "nvidia":     "NVDA",

}

def normalize_ticker(token: str) -> str:

    tok_low = token.lower()
    if tok_low in NAME2TICKER:
        return NAME2TICKER[tok_low]

    if re.fullmatch(r"[A-Za-z]{1,5}", token):
        return token.upper()

    # ---- intento dinámico----------
    url = "https://api.polygon.io/v3/reference/tickers"
    r = requests.get(url, params={
        "search": token,
        "active": "true",
        "limit":  1,
        "sort":   "market_cap",
        "order":  "desc",
        "apiKey": POLYGON_KEY
    }, timeout=10)
    if r.ok and (results := r.json().get("results")):
        tkr = results[0]["ticker"]
        NAME2TICKER[tok_low] = tkr       # cachea para la próxima vez
        return tkr

    return token.upper()

def get_last_stock_split(ticker: str) -> str:
    url = "https://api.polygon.io/v3/reference/splits"
    params = {
        "ticker": ticker.upper(),
        "reverse_split": "false",
        "limit": 1,
        "sort": "execution_date",
        "order": "desc",
        "apiKey": os.environ["POLYGON_KEY"],
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
    except Exception as e:
        return f"Error consultando Polygon: {e}"

    if not results:
        return f"No encontré splits para {ticker.upper()}."

    s = results[0]

    # Polygon v3 -> split_to / split_from
    to_   = s.get("split_to")   or s.get("numerator")
    from_ = s.get("split_from") or s.get("denominator")

    if to_ is None or from_ is None:
        return f"Split encontrado pero falta la proporción (raw={s})."

    ratio = f"{to_}:{from_}"
    fecha = s["execution_date"]
    return f"El último split de {ticker.upper()} fue el {fecha} con proporción {ratio}."

# ───────────────────── 5. MEMORY (SESSION STATE) ───────────────────

def memory_set(session_id: str, key: str, value):
    _state(session_id)["scratch"][key] = value

def memory_get(session_id: str, key: str, default=None):
    return _state(session_id)["scratch"].get(key, default)

def remember_ticker(session_id: str, ticker: str):
    memory_set(session_id, "last_ticker", ticker.upper())

def recall_ticker(session_id: str) -> Optional[str]:
    return memory_get(session_id, "last_ticker")
# ───────────────────── 6. QUESTION HANDLER CORE ────────────────────

APOLOGIES = ("lo siento", "i'm sorry", "no tengo")


def handle_question(
    q: str,
    session_id: str,
    rag_fin: RetrievalQA,
    rag_port: RetrievalQA,
    llm: ChatOpenAI,
    chat_prompt: ChatPromptTemplate,
) -> str:

    #Función pura que enruta la consulta del usuario y devuelve la respuesta como cadena

    q_low = q.lower()

    m = re.search(r"(?:últimos?|ultimos?)\s*(\d+)?\s*dividendos?", q_low)
    if m and not re.search(r"\bde\b", q_low):
        n = int(m.group(1)) if m.group(1) else 5
        last = recall_ticker(session_id)
        if last:
            ans = get_dividends(ticker=last, limit=n)
            push_history(session_id, "user", q)
            push_history(session_id, "assistant", ans)
            return ans

    m = re.search(
        r"(?:últimos?|ultimos?)\s*(\d+)?\s*dividendos?\s+de\s+(\w+)",
        q_low
    )
    if m:
        n   = int(m.group(1)) if m.group(1) else 5
        tkr = normalize_ticker(m.group(2))
        ans = get_dividends(ticker=tkr, limit=n)
        push_history(session_id, "user", q)
        push_history(session_id, "assistant", ans)
        return ans

    if re.search(r"\bpor\s+activo\b", q_low):
        docs = rag_fin.invoke({"query": '"Inventario %"'})["source_documents"]
        activos = extrae_porcentaje_por_activo([d.page_content for d in docs])
        if not activos:
            return "No encontré porcentajes por activo."

        activos = dict(sorted(activos.items(), key=lambda kv: kv[1], reverse=True))
        return "\n".join(f"• **{nombre}**: {pct:.2f} %"
                         for nombre, pct in activos.items()
                         )


    m = re.search(r"correlaci[oó]n .*? (\w+) .*? (\w+)", q_low)
    if m:
        t1 = normalize_ticker(m.group(1))
        t2 = normalize_ticker(m.group(2))
        ans = get_correlation(t1, t2)
        push_history(session_id, "user", q)
        push_history(session_id, "assistant", ans)
        return ans

    m = re.search(r"(?:cu[aá]ndo|cuando)\s+fue\s+(?:el\s+)?stock\s+split\s+de\s+(\w+)", q_low)
    if m:
        tkr = normalize_ticker(m.group(1))
        ans = get_last_stock_split(tkr)
        push_history(session_id, "user", q)
        push_history(session_id, "assistant", ans)
        return ans


    m = re.search(r"precio de cierre de (\w+) de la semana pasada", q_low)
    if m:
        tkr = normalize_ticker(m.group(1))
        today = date.today()
        last_monday = today - timedelta(days=today.weekday()+7)   # lunes de la semana pasada
        last_friday = last_monday + timedelta(days=4)             # viernes
         # descarga precios (Polygon o ES) día por día
        lines = [f"Precios de cierre ajustados de {tkr} (semana pasada):"]
        d = last_monday
        while d <= last_friday:
            resp = get_price_on_date(tkr, d.isoformat())
            lines.append(f"• {d}: {resp.split()[-2]} USD")        # extrae sólo el precio
            d += timedelta(days=1)
        ans = "\n".join(lines)
        push_history(session_id, "user", q)
        push_history(session_id, "assistant", ans)
        remember_ticker(session_id, tkr)
        return ans


    m = re.search(r"cu[aá]nt[ao] val[ií]a (\w+) el (\d{1,2}) de (\w+)(?: de (\d{4}))?", q_low)
    if m:
        tkr, day, month_name = m.group(1).upper(), int(m.group(2)), m.group(3).lower()
        year = int(m.group(4)) if m.group(4) else date.today().year
        meses = {
        "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
        "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12
        }
        month = meses.get(month_name, 0)
        if month == 0:
            return "No entendí el mes."
        fecha = f"{year:04d}-{month:02d}-{day:02d}"
        return get_price_on_date(tkr, fecha)




    #------------ag------


    # --- # atajos rápidos basados en reglas shortcuts ----------------------------------
    if "último trimestre" in q_low or "ultimo trimestre" in q_low:
        end = date.today(); start = end - timedelta(days=90)
        return get_top_movers(start_date=start.isoformat(), end_date=end.isoformat(), top_n=5)

    if "volumen" in q_low:
        end = date.today(); start = end - timedelta(days=7)
        return get_top_volume(start_date=start.isoformat(), end_date=end.isoformat(), top_n=5)

    # rango de fechas explícito
    m = re.search(r"subieron.*del\s*([\d]{4}-[\d]{2}-[\d]{2})\s*al\s*([\d]{4}-[\d]{2}-[\d]{2})", q, re.IGNORECASE)
    if m:
        return get_top_movers(start_date=m.group(1), end_date=m.group(2), top_n=5)

   # último escenario beta usa memoria
    m = re.search(r"cae un\s*(\d+(?:\.\d+)?)%", q, re.IGNORECASE)
    last_beta = memory_get(session_id, "last_beta")
    if m and last_beta is not None:
        caída = float(m.group(1)) / 100.0
        pérdida = last_beta * caída * 100
        return f"Si el S&P 500 cae un {caída*100:.1f}%, tu portafolio (β={last_beta:.3f}) perdería aproximadamente {pérdida:.2f}% de su valor."

    # --- portafolio‑ RAG ----------------------------------------
    if re.search(r"\bmi\s+(portafolio|cartera)\b", q_low):
        ans_port = rag_port.invoke({"query": q})["result"]
        if ans_port.strip() and not ans_port.lower().startswith(APOLOGIES):
            return ans_port

    # --- RAG fallback ----------------------------------------
    rag = rag_fin.invoke({"query": q})
    #print("🔍 Retrieved:", [doc.metadata for doc in result['source_documents']])
    answer = rag["result"]
    globals()["LAST_SOURCES"] = rag.get("source_documents", [])
    if answer.strip() and not answer.lower().startswith(APOLOGIES):
        return answer

    # --- LLM tool‑calling pipeline -----------------------------------
    history = get_history(session_id)
    prompt_msgs = chat_prompt.format_messages(today=str(date.today()), input=q)
    resp = llm.invoke(history + prompt_msgs)
    tool_calls = resp.additional_kwargs.get("tool_calls", [])

    if not tool_calls:
        return resp.content

    # enriquecimeinto del mensaje con tool response
    messages_with_tools = [*prompt_msgs, resp]
    for call in tool_calls:
        fn_name = call["function"]["name"]
        args    = json.loads(call["function"]["arguments"])


        if fn_name == "get_price":
            result = get_price(**args)
        elif fn_name == "get_top_movers":
            result = get_top_movers(**args)
        elif fn_name == "get_dividends":
            result = get_dividends(**args)
        elif fn_name == "get_correlation":
            result = get_correlation(**args)
        elif fn_name == "get_index_summary":
            result = get_index_summary(**args)
        elif fn_name == "get_price_on_date":
            result = get_price_on_date(**args)
        elif fn_name == "get_portfolio_beta":
            result = get_portfolio_beta(**args)
            m_beta = re.search(r"Beta ponderada del portafolio:\s*([\d\.]+)", result)
            if m_beta:
                memory_set(session_id, "last_beta", float(m_beta.group(1)))
        elif fn_name == "query_table":
            result = query_table(**args)
        else:
            result = f"Función desconocida: {fn_name}"

        messages_with_tools.append({
            "role": "tool",
            "tool_call_id": call["id"],
            "content": result,
        })

    final = llm.invoke(messages_with_tools)
    return final.content


# ───────────────────── 7. KAFKA CONSUME‑PRODUCE LOOP ───────────────

def kafka_loop(
    args, rag_fin, rag_port, llm, chat_prompt, es: Elasticsearch
):
    """Bucle continuo de consumir-procesar-producir. Llamada bloqueante."""

    # ───────────────────────────────────────────────────────
    # 0) CONFIG-FILE CHECK
    # ───────────────────────────────────────────────────────
    print(f"[BOOT] config_file      = {args.config_file!r}", file=sys.stderr)
    print(f"[BOOT] chatbotreqtopic  = {args.chatbotreqtopic}", file=sys.stderr)
    print(f"[BOOT] chatbotrestopic  = {args.chatbotrestopic}", file=sys.stderr)
    print(f"[BOOT] chatbotresfinal  = {args.chatbotrestopicfinal}", file=sys.stderr)

    # ───────────────────────────────────────────────────────
    # 1) LOAD CONFLUENT PROPERTIES
    # ───────────────────────────────────────────────────────
    try:
        conf_consumer = ccloud_lib.read_ccloud_config(args.config_file)
        conf_producer = ccloud_lib.read_ccloud_config(args.config_file)
        print("[BOOT] ✅ client.properties leídos OK", file=sys.stderr)
    except Exception as e:
        print("[BOOT] ❌ read_ccloud_config:", e, file=sys.stderr)
        raise

    # ───────────────────────────────────────────────────────
    # 2) SCHEMA REGISTRY
    # ───────────────────────────────────────────────────────
    try:
        schema_registry = SchemaRegistryClient(
            {"url": conf_consumer['schema.registry.url']}
        )
        print(f"[BOOT] ✅ schema-registry = {conf_consumer['schema.registry.url']}", file=sys.stderr)
    except Exception as e:
        print("[BOOT] ❌ SchemaRegistry:", e, file=sys.stderr)
        raise

    # ------------- DESERIALIZER / CONSUMER -----------------
    try:
        chatbotreq_des = AvroDeserializer(
            schema_registry_client=schema_registry,
            schema_str=ccloud_lib.chatbotreq_schema,
            from_dict=ccloud_lib.Chatbotreq.dict_to_chatbotreq,
        )
        cons_conf = ccloud_lib.pop_schema_registry_params_from_config(conf_consumer)
        cons_conf.update({
            "value.deserializer": chatbotreq_des,
            "group.id": "asyngenaichat",
            "auto.offset.reset": "earliest",
        })
        consumer = DeserializingConsumer(cons_conf)
        consumer.subscribe([args.chatbotreqtopic])
        print("[BOOT] ✅ Kafka consumer creado y suscrito", file=sys.stderr)
    except Exception as e:
        print("[BOOT] ❌ creando consumer:", e, file=sys.stderr)
        raise

    # ------------- SERIALIZER / PRODUCER ------------------
    try:
        prod_conf = ccloud_lib.pop_schema_registry_params_from_config(conf_producer)
        chatbotres_val_ser = AvroSerializer(
            schema_registry_client=schema_registry,
            schema_str=ccloud_lib.chatbotres_final_value_schema,
            to_dict=ccloud_lib.Chatbotresfinalvalue.chatbotresfinalvalue_to_dict,
        )
        chatbotres_key_ser = AvroSerializer(
            schema_registry_client=schema_registry,
            schema_str=ccloud_lib.chatbotres_final_key_schema,
            to_dict=ccloud_lib.Chatbotresfinalkey.chatbotresfinalkey_to_dict,
        )
        prod_conf["value.serializer"] = chatbotres_val_ser
        prod_conf["key.serializer"] = chatbotres_key_ser
        producer = SerializingProducer(prod_conf)
        print("[BOOT] ✅ Kafka producer creado", file=sys.stderr)
    except Exception as e:
        print("[BOOT] ❌ creando producer:", e, file=sys.stderr)
        raise

    print("asyngenaichat ⚡ listo – entrando en bucle …", file=sys.stderr)

    idle = 0
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                if idle % 30 == 0:
                    print(f"{idle} s sin mensajes …")
                continue
            if msg.error():
                print("Kafka error:", msg.error())
                continue

            req  = msg.value(); ques = req.query
            print("▶", ques)

            sid = getattr(req, "session_id", None) or "default"
            ans = handle_question(ques, sid, rag_fin, rag_port, llm, chat_prompt)
            print("⮕", ans[:110].replace("\n", " "))

            doc_contexts = []
            for d in (globals().get("LAST_SOURCES") or [])[:5]:
                try:
                    if isinstance(d, dict):
                        text = d.get("text") or d.get("page_content") or str(d)
                        meta = d.get("metadata", {})
                        source = d.get("source", meta.get("source"))
                        page   = d.get("page",   meta.get("page"))
                    else:
                        text = getattr(d, "page_content", str(d))
                        meta = getattr(d, "metadata", {}) or {}
                        source = meta.get("source")
                        page   = meta.get("page")

                    doc_contexts.append({
                        "text": str(text)[:2000],
                        "source": source,
                        "page": page,
                    })
                except Exception:
                    continue


            # --- index Q&A for retrieval ------------------------------------------------
            try:
                es.index(
                    index="cdocs_demo_v1",
                    document={
                        "reqid": req.reqid,
                        "login": req.loginname,
                        "role": "AskDoc",
                        "query": ques,
                        "answer": ans,
                        "contexts": doc_contexts,
                        "ts": int(_dt.datetime.utcnow().timestamp() * 1000),
                    },
                )
            except Exception as e:
                print("⚠️  No se pudo indexar en ES:", e)

            # --- emitir respuesta a kafka -------------------------------------------------
            key = ccloud_lib.Chatbotresfinalkey(
                session_id=getattr(req, "session_id", None),
                reqid=getattr(req, "reqid", None),
            )
            val = ccloud_lib.Chatbotresfinalvalue(
                loginname=req.loginname, query=ques, answer=ans
            )
            producer.produce(args.chatbotrestopicfinal, key=key, value=val)
            producer.poll(0)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print("❌ Error:", e)

    consumer.close(); producer.flush()


# ───────────────────────────── 8. MAIN ─────────────────────────────

def main():
    args = ccloud_lib.parse_args()

    # ---- NEW: canonical defaults (env → code) -----------------
    args.chatbotreqtopic      = os.getenv("CHATBOT_REQ_TOPIC",
                                          "docs_chatbotreq_v1")
    args.chatbotrestopic      = os.getenv("CHATBOT_RES_TOPIC",
                                          "docs_chatbotres_step_1")
    args.chatbotrestopicfinal = os.getenv("CHATBOT_RESFINAL_TOPIC",
                                          "docs_chatbotres_step_final_v1")
    # ------------------------------------------------------------

    es          = build_elasticsearch()

    ensure_es_index(es, "tbl_docs_v1", {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "content":        { "type": "text" },
                "vector":         { "type": "dense_vector", "dims": 512, "index": True, "similarity": "cosine" },

                "row_header":     { "type": "keyword" },
                "nivel":          { "type": "keyword" },

                "rentabilidad":    { "type": "double"  },
                "benchmark":       { "type": "double"  },
                "delta_benchmark": { "type": "double"  },
                "contribucion":    { "type": "double"  },
                "peso_portafolio": { "type": "double"  },
                "variacion_usd":   { "type": "double"  },
                "delta_usd":       { "type": "double"  },
                "delta_pct":       { "type": "double"  },

                "ticker":          { "type": "keyword"  },
                "asset_class":     { "type": "keyword"  },
                "sector":          { "type": "keyword"  },

                "source":          { "type": "keyword" },
                "page":            { "type": "integer" }
            }
        }
    })

    ensure_es_index(es, "docs_chatbotres_v3", {
        "settings": {
            "number_of_shards":   1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "metadata": {
                    "properties": {
                        "page":   { "type": "long"    },
                        "source": { "type": "keyword" }
                    }
                },
                "content": {
                    "type": "text",
                    "analyzer": "standard"
                },
                "vector": {
                    "type":       "dense_vector",
                    "dims":        512,
                    "index":       True,
                    "similarity": "cosine"
                }
            }
        }
    })
    ensure_es_index(es, "ext_docs_v1", {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "metadata": {
                    "properties": {
                        "page":    { "type": "long"    },
                        "source":  { "type": "keyword" },
                        "tickers": { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
                        "ts":      { "type": "long"    }
                    }
                },
                "text":   { "type": "text" },
                "vector": { "type": "dense_vector", "dims": 512, "index": True, "similarity": "cosine" }
            }
        }
    })
    # cdocs_demo_v1
    ensure_es_index(es, "cdocs_demo_v1", {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "answer": { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
                "login":  { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
                "query":  { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
                "reqid":  { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
                "role":   { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
                "ts":     { "type": "long" }
            }
        }
    })
    # prices_snap_v1
    ensure_es_index(es, "prices_snap_v1", {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "price":  { "type": "long" },
                "ticker": { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
                "ts":     { "type": "long" },
                "volume": { "type": "long" }
            }
        }
    })
    # prices_daily_v1
    ensure_es_index(es, "prices_daily_v1", {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "close":  { "type": "float" },
                "high":   { "type": "float" },
                "low":    { "type": "float" },
                "open":   { "type": "float" },
                "ticker": { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
                "ts":     { "type": "long" },
                "volume": { "type": "float" }
            }
        }
    })
    # prices_dividends_v1
    ensure_es_index(es, "prices_dividends_v1", {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "cash_amount":       { "type": "float" },
                "declaration_date":  { "type": "date"  },
                "ex_dividend_date":  { "type": "date"  },
                "frequency":         { "type": "long"  },
                "record_date":       { "type": "date"  },
                "ticker":            { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } }
            }
        }
    })
    # indices_summary_v1
    ensure_es_index(es, "indices_summary_v1", {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "after_hours": { "type": "float" },
                "close":       { "type": "float" },
                "date":        { "type": "date"  },
                "high":        { "type": "float" },
                "low":         { "type": "float" },
                "open":        { "type": "float" },
                "ticker":      { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } }
            }
        }
    })

    # 2) Kafka Pre-flight
    admin_conf = ccloud_lib.read_ccloud_config(args.config_file)
    admin_conf = {
        "bootstrap.servers": admin_conf["bootstrap.servers"],
        "security.protocol": admin_conf.get("security.protocol","PLAINTEXT"),
    }
    topics = [
        NewTopic("source_docs_v3",      num_partitions=3, replication_factor=1),
        NewTopic("source_tables_v1",    num_partitions=1, replication_factor=1),
        NewTopic(args.chatbotreqtopic,    num_partitions=1, replication_factor=1),
        NewTopic(args.chatbotrestopic,    num_partitions=1, replication_factor=1),
        NewTopic(args.chatbotrestopicfinal, num_partitions=1, replication_factor=1),
    ]
    ensure_kafka_topics(admin_conf, topics)

    llm         = build_llm(build_tools())
    chat_prompt = build_prompt()
    rag_fin, rag_port, _ = build_retrievers(llm)

    kafka_loop(args, rag_fin, rag_port, llm, chat_prompt, es)

if __name__ == "__main__":
    main()
