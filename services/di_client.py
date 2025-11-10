# di_client.py – ingestión optimizada (v4-ready)

from pdf_to_df import rows_to_df
from elasticsearch import Elasticsearch, helpers
import tempfile
import math
from utils_polygon import polygon_get

import os, json, re, sys

from decimal import Decimal, InvalidOperation
from typing import Union
from uuid import uuid4
from typing import List, Dict, Any
from collections import defaultdict
from confluent_kafka import SerializingProducer
import ccloud_lib
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult
from azure.core.credentials import AzureKeyCredential

# --------------- CONFIG ----------------------------------------------------

SYMBOL_RE = re.compile(r"\b[A-Z]{1,5}\b")

ELASTIC_URL    = os.getenv("ELASTIC_URL", "http://localhost:9200")
es = Elasticsearch(hosts=[ELASTIC_URL])
AZURE_INTELLIGENCES_ENDPOINT = os.environ["AZURE_INTELLIGENCES_ENDPOINT"]
AZURE_INTELLIGENCES_KEY      = os.environ["AZURE_INTELLIGENCES_KEY"]

_client = DocumentIntelligenceClient(
    AZURE_INTELLIGENCES_ENDPOINT,
    AzureKeyCredential(AZURE_INTELLIGENCES_KEY)
)

SCHEMA_REG_URL   = os.environ["SCHEMA_REGISTRY_URL"]
BROKER         = os.getenv("KAFKA_BROKER", "127.0.0.1:29092")
SEC_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")


producer_conf_tbl: Dict[str, str] = {
    "bootstrap.servers":   BROKER,
    "security.protocol":  SEC_PROTOCOL,

    "schema.registry.url": SCHEMA_REG_URL,
}

schema_registry_client_tbl = SchemaRegistryClient({"url": SCHEMA_REG_URL})
producer_conf_tbl = ccloud_lib.pop_schema_registry_params_from_config(producer_conf_tbl)


tablerow_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client_tbl,
    schema_str=ccloud_lib.tablerow_schema,
    to_dict=ccloud_lib.TableRow.tablerow_to_dict,
)
producer_conf_tbl["value.serializer"] = tablerow_serializer
_TABLE_PRODUCER = SerializingProducer(producer_conf_tbl)

_NUM_PATTERN = re.compile(r"""
    (?P<neg>\()?             # ( opcional para indicar negativo
    [^\d\-]*                 # símbolos monetarios u otros delante
    (?P<num>[\d.,\s]+)       # dígitos con separadores
    [^\d%]*                  # texto intermedio (ej. espacio antes del %)
    (?P<pct>%?)              # % opcional
    \)?                      # ) opcional para negativo
""", re.VERBOSE)


def normalize_number(txt: str) -> Union[float, None]:
    """
    Convierte '$1,234.56', '-12,3 %', '(1.000,25)', '—' → float
    Devuelve None si no se puede parsear.
    """
    if not txt:
        return None

    m = _NUM_PATTERN.search(txt)
    if not m:
        return None

    raw = m.group('num')
    pct = bool(m.group('pct'))
    neg = bool(m.group('neg')) or txt.strip().startswith('-')

    # 1. Quitar espacios de millares (frances/español usan espacio no-break)
    raw = raw.replace('\u202f', '').replace(' ', '')

    # 2. Detectar formato decimal
    if raw.count(',') > 1 and raw.count('.') == 0:     # 1.234.567,89 (EU)
        raw = raw.replace('.', '').replace(',', '.')
    elif raw.count('.') > 1 and raw.count(',') == 0:   # 1,234,567.89 (US con , millares)
        raw = raw.replace(',', '')
    elif raw.count(',') == 1 and raw.count('.') == 1:
        # Dos separadores → asume último es decimal
        if raw.rfind(',') > raw.rfind('.'):
            raw = raw.replace('.', '').replace(',', '.')
        else:
            raw = raw.replace(',', '')

    try:
        val = Decimal(raw)
        if pct:
            val = val / 100
        if neg:
            val = -val
        return float(val)
    except InvalidOperation:
        return None



# --------------- HELPERS ---------------------------------------------------
def _group_lines(lines, max_len: int = 400) -> List[str]:
    """Une líneas sueltas en trozos ≲ max_len caracteres."""
    buf, chunk = [], ""
    for ln in lines:
        if len(chunk) + len(ln) + 1 > max_len:
            buf.append(chunk.strip())
            chunk = ln
        else:
            chunk += (" " if chunk else "") + ln
    if chunk:
        buf.append(chunk.strip())
    return buf


def _build_matrix(tbl):
    """Crea una matriz 2-D respetando row_span / col_span (v3 y v4)."""
    # v4 → row_count / column_count  |  v3 → rows / columns
    rows = getattr(tbl, "row_count", getattr(tbl, "rows", None))
    cols = getattr(tbl, "column_count", getattr(tbl, "columns", None))
    if rows is None or cols is None:
        rows = 1 + max(c.row_index for c in tbl.cells)
        cols = 1 + max(c.column_index for c in tbl.cells)

    mtx = [[None] * cols for _ in range(rows)]
    for cell in tbl.cells:
        r, c = cell.row_index, cell.column_index
        rs, cs = cell.row_span or 1, cell.column_span or 1
        for i in range(rs):
            for j in range(cs):
                mtx[r + i][c + j] = cell
    return mtx


def _headers_from_matrix(mtx):
    """Detecta encabezados de columna y fila."""
    col_h, row_h = {}, {}
    for r, row in enumerate(mtx):
        for c, cell in enumerate(row):
            if not cell:
                continue
            if cell.kind in ("columnHeader", "mergedHeader"):
                col_h[c] = cell.content.strip()
            if cell.kind == "rowHeader":
                row_h[r] = cell.content.strip()
    return col_h, row_h
# ─────────── normalizador numérico ────────────

# --------------- MAIN ------------------------------------------------------
def analyze_local(path: str,
                  *,
                  model_id: str = "prebuilt-layout",
                  max_chunk: int = 400) -> List[Dict[str, Any]]:
    """
    Devuelve bloques normalizados con párrafos + filas de tabla.
    Cada bloque: {id,type,page,content,…}
    """
    with open(path, "rb") as f:
        data = f.read()

        poller = _client.begin_analyze_document(
            model_id=model_id,
            body=data,                        # (v4)
            # requiere recurso v4
        )
    result: AnalyzeResult = poller.result()
    blocks: List[Dict[str, Any]] = []

    # ---- 1. Párrafos ------------------------------------------------------
    for p in result.paragraphs or []:
        txt = p.content.strip()
        if not txt:
            continue
        pg = p.bounding_regions[0].page_number if p.bounding_regions else -1
        blocks.append({
            "id": str(uuid4()),
            "type": "paragraph",
            "page": pg,
            "content": txt
        })

    # ---- 2. Líneas sueltas (fallback v3 / restos) -------------------------
    for page in result.pages:
        orphan = [
            ln.content.strip()
            for ln in page.lines
            if not getattr(ln, "roles", None)    # roles solo existe en v4
        ]
        for txt in _group_lines(orphan, max_chunk):
            blocks.append({
                "id": str(uuid4()),
                "type": "paragraph",
                "page": page.page_number,
                "content": txt
            })

    rows_json: list[dict] = []
    # ---- 3. Tablas mejoradas ---------------------------------------------
    for t_idx, tbl in enumerate(result.tables or []):
        mtx = _build_matrix(tbl)
        col_h, row_h = _headers_from_matrix(mtx)

        for r, row in enumerate(mtx):
            record = defaultdict(str)
            if r in row_h:                      # header de fila
                record["row_header"] = row_h[r]

            if record.get("row_header") and normalize_number(record["row_header"]) is not None:
                record.pop("row_header")

            for c, cell in enumerate(row):
                if cell is None:
                    continue
                header = col_h.get(c, f"col{c}")
                record[header] += (" " + cell.content.strip())

            if not record.get("row_header"):
                for k in ("Activo", "Instrumento", "Nombre", "Descripción"):
                    if record.get(k):
                        record["row_header"] = record[k].strip()
                        break

            if not record.get("row_header"):
                for cell in row:
                    if cell and normalize_number(cell.content) is None:
                        record["row_header"] = cell.content.strip()
                        break

            if not record.get("row_header") \
                    or normalize_number(record["row_header"]) is not None:
                continue
            #---------------ag____
            valor_clase = (record.get("Clase de activo") or
                           record.get("Clase de Activo") or "").strip()

            es_subtotal = record["row_header"].upper() in ("SUBTOTAL", "TOTAL","TOTAL GENERAL")
            if es_subtotal:
                continue
            if valor_clase and valor_clase == record["row_header"]:
                record["nivel"] = "clase"
            else:
                record["nivel"] = "activo"

            aliases = {
                "rentabilidad":        record.get("Rentabilidad %")    or record.get("Rentabilidad")
                                       or record.get("Rent. %")        or record.get("Rent. Portafolio"),
                "benchmark":           record.get("Rent. Benchmark %") or record.get("Bench. %")
                                       or record.get("Benchmark"),
                "delta_benchmark":     record.get("Diff. %")           or record.get("Diferencia %"),
                "contribucion":        record.get("Contribución %")    or record.get("Contr. Portafolio"),
                "peso_portafolio":     record.get("Inventario %"),
                "variacion_usd":       record.get("Var USD"),
                "rentabilidad_mensual":record.get("Rent. Mensual %"),
                "rentabilidad_anual":  record.get("Rent. Anual %"),
                "tir":                 record.get("TIR %"),
            }
            record.update({k:v for k,v in aliases.items() if v is not None})


            num_record = {k: normalize_number(v)
                          for k, v in record.items()
                          #------ag-----
                          if k not in ("row_header", "nivel")
                          }
            tablerow = ccloud_lib.TableRow(
                source = os.path.basename(path),
                page   = tbl.bounding_regions[0].page_number,
                row_json = json.dumps(record, ensure_ascii=False),
                numeric  = json.dumps(num_record, separators=(",",":"))
            )
            _TABLE_PRODUCER.produce("source_tables_v1", value=tablerow)
            rows_json.append({**record, **num_record})
            _TABLE_PRODUCER.poll(0)


            blocks.append({
                "id": str(uuid4()),
                "type": "table_row",
                "page": tbl.bounding_regions[0].page_number,
                "table": t_idx,
                "row": r,
                "content": json.dumps(record, ensure_ascii=False),
                "numeric": num_record
            })

    if rows_json:
        df = rows_to_df(rows_json)

        tmp_path = tempfile.gettempdir() + "/last_ingest.parquet"
        df.to_parquet(tmp_path, index=False)

        def _nan_to_none(row: dict) -> dict:
            return {k: (None if (isinstance(v, float) and math.isnan(v)) else v)
                    for k, v in row.items()}

        helpers.bulk(
            es,
            (
                {"_index": "tbl_docs_v1", **_nan_to_none(row)}
                for row in df.to_dict(orient="records")
            ),

        )

    _TABLE_PRODUCER.flush(5)




    return blocks