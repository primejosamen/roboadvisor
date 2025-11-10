#genaidocsexplorer.py

from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
import traceback
import confluent_kafka
import uuid
from uuid import uuid4
import PyPDF2
from langchain_openai.embeddings import OpenAIEmbeddings
#from langchain.vectorstores import ElasticsearchStore
from langchain.schema import Document
from langchain_community.document_loaders import PyPDFLoader
from confluent_kafka import DeserializingConsumer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.avro import AvroSerializer
#from confluent_kafka.schema_registry.avro import SerializerError
from confluent_kafka.avro.serializer import SerializerError
#from langchain.utilities import GoogleSearchAPIWrapper
import ccloud_lib
import pymongo
from langchain.agents import initialize_agent
from langchain.agents import AgentType
from langchain import PromptTemplate
from elasticsearch import Elasticsearch
from flask import Flask
from flask_socketio import SocketIO
import requests
from flask import Flask, request, jsonify, json
from langchain_experimental.text_splitter import SemanticChunker
from langchain_text_splitters import CharacterTextSplitter
from langchain.vectorstores import MongoDBAtlasVectorSearch
import threading, time
from confluent_kafka import DeserializingConsumer, KafkaException
from langchain_elasticsearch import ElasticsearchStore
from langchain.schema import Document
from langchain_community.vectorstores import ElasticsearchStore
from langchain_community.utilities import GoogleSearchAPIWrapper
# AI
# General
import json
import os
from bson import json_util
import pandas as pd
from flask import jsonify
import sys

BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:29092")
ELASTIC_URL = os.environ.get("ELASTIC_URL", "http://localhost:9200")
# BROKER      = os.getenv("KAFKA_BROKER", "kafka:9092")
# ELASTIC_URL = os.getenv("ELASTIC_URL", "http://elasticsearch-coordinating:9200")
# DOCS_WS_URL = os.getenv("DOCS_WS_URL", "http://docsexplorer:5000")
SPLITTER = RecursiveCharacterTextSplitter(chunk_size=2000, chunk_overlap=200)


# global & reutilizable

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

@socketio.on('connect')
def on_connect():
    print("Cliente conectado por WebSocket")
@socketio.on("data")
def forward_data(payload):
    """
    payload viene de asyngenaichatre.py vía sio.emit("data", {...})
    Lo enviamos de vuelta a los navegadores.
    """
    # broadcast=True => lo reciben todos los clientes
    socketio.emit("data", payload)
@app.errorhandler(Exception)
def handle_all_errors(e):
    # Opcional: loguear e en app.logger.error(...)
    response = jsonify({ "error": str(e) })
    response.status_code = 500
    return response

try:
    path = os.path.dirname(os.path.abspath(__file__))
    upload_folder=os.path.join(
        path.replace("/file_folder",""),"tmp")
    os.makedirs(upload_folder, exist_ok=True)
    app.config['upload_folder'] = upload_folder
except Exception as e:
    app.logger.info("An error occurred while creating temp folder")
    app.logger.error("Exception occurred : {}".format(e))

args = ccloud_lib.parse_args()
config_file = args.config_file
# Same defaulting logic as in asyngenaichat.py
chatbotreqtopic = "docs_chatbotreq_v1"

confproducer = ccloud_lib.read_ccloud_config(config_file)
confconsumer = ccloud_lib.read_ccloud_config(config_file)
schema_registry_conf = {
    "url": confconsumer["schema.registry.url"],
    #"basic.auth.user.info": confconsumer["basic.auth.user.info"],
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
# chatbotreq
chatbotreq_avro_serializer = AvroSerializer(
    schema_registry_client = schema_registry_client,
    schema_str =  ccloud_lib.chatbotreq_schema,
    to_dict = ccloud_lib.Chatbotreq.chatbotreq_to_dict)
# uploaddoc serializer
uploaddoc_avro_serializer = AvroSerializer(
    schema_registry_client = schema_registry_client,
    schema_str =  ccloud_lib.uploaddoc_schema,
    to_dict = ccloud_lib.Uploaddoc.uploaddoc_to_dict)

tablerow_avro_deserializer = AvroDeserializer(
    schema_registry_client = schema_registry_client,
    schema_str = ccloud_lib.tablerow_schema,
    from_dict  = ccloud_lib.TableRow.dict_to_tablerow
)

embeddings = OpenAIEmbeddings(
    model="text-embedding-3-small",      # 512 dims
    dimensions=512                       # explícito por si OpenAI cambia defaults
)



#ELASTIC_CLOUD_PASSWORD = os.environ["ELASTIC_CLOUD_PASSWORD"]

# Found in the 'Manage Deployment' page

client = Elasticsearch(hosts=[ELASTIC_URL])

message_count = 0
waiting_count = 0
# Subscribe to topic

# producer

producer_conf = ccloud_lib.pop_schema_registry_params_from_config(confproducer)
producer_conf["value.serializer"] = uploaddoc_avro_serializer
_PRODUCER_U = SerializingProducer(producer_conf)
delivered_records = 0
# query doc

embeddings = OpenAIEmbeddings(model="text-embedding-3-small",
                              dimensions=512)
def start_table_indexer():                                           # >>> MOD
    conf = ccloud_lib.pop_schema_registry_params_from_config(confconsumer)
    conf.update({"group.id": "indexer_table", "auto.offset.reset": "earliest"})
    conf["value.deserializer"] = tablerow_avro_deserializer
    consumer = DeserializingConsumer(conf)
    consumer.subscribe(["source_tables_v1"])

    es = Elasticsearch(hosts=[ELASTIC_URL])

    while True:
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue
        row = m.value()
        if row is None:
            continue

        # ---------- 1️⃣  Reconstruimos los datos originales ----------
        full_row = json.loads(row.row_json) if row.row_json else {}
        nums     = json.loads(row.numeric)  if row.numeric  else {}

        # ---------- 2️⃣  Texto para el embedding ----------
        text_parts = [full_row.get("row_header", "")]
        # opcional: añade algunos campos descriptivos legibles
        for k, v in full_row.items():
            if k not in ("row_header", "nivel") and isinstance(v, str):
                text_parts.append(f"{k}: {v}")

        content_txt = " | ".join(text_parts)[:4096]         # corte defensivo
        vector      = embeddings.embed_query(content_txt)   # ← aquí se genera

        # ---------- 3️⃣  Documento final ----------
        doc = {**full_row, **nums,
               "source":  row.source,
               "page":    row.page,
               "content": content_txt,   # ← nuevo
               "vector":  vector}        # ← nuevo

        es.index(index="tbl_docs_v1", document=doc)
        consumer.commit(m)


# ----------  INDEXADOR EN BACKGROUND ----------
def start_indexer():
    """
    Hilo que consume source_docs_v1 y guarda embeddings en Elasticsearch.
    """
    # ---- Configuración del consumer Kafka -------------------------------
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(confconsumer)
    consumer_conf.update({
        "group.id": "indexer",
        "auto.offset.reset": "earliest"
    })

    uploaddoc_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=ccloud_lib.uploaddoc_schema,
        from_dict=ccloud_lib.Uploaddoc.dict_to_uploaddoc,
    )
    consumer_conf["value.deserializer"] = uploaddoc_deserializer
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(["source_docs_v3"])

    # ---- Elasticsearch + embeddings -------------------------------------
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small", dimensions=512)
    vector_search = ElasticsearchStore(
        es_url=ELASTIC_URL,
        index_name="docs_chatbotres_v3",
        embedding=embeddings,
        vector_query_field="vector",
        query_field="content",


    )

    print("[Indexer] Iniciado; esperando documentos …")

    batch = []
    batch_msgs    = []
    BATCH_SIZE = 32
    FLUSH_SECS = 5
    last_event = time.time()

    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                if batch and (time.time() - last_event) > FLUSH_SECS:
                    vector_search.add_documents(batch, refresh=False)
                    consumer.commit(batch_msgs[-1], asynchronous=False)
                    batch.clear(); batch_msgs.clear()
                continue

            page = msg.value()              # objeto Uploaddoc
            if page is None:
                continue

            doc = Document(
                page_content=page.text,
                metadata={"source": page.source, "page": page.page, "chunk_id": page.chunk_id}
            )
            batch.append(doc)

            batch_msgs.append(msg)
            last_event = time.time()

            if len(batch) >= BATCH_SIZE:
                try:
                    ids = [d.metadata["chunk_id"] for d in batch]
                    vector_search.add_documents(batch, refresh=False)
                    consumer.commit(batch_msgs[-1], asynchronous=False)
                except Exception as e:
                    print("[Indexer] ❌  Error indexando:", e)
                finally:
                    batch.clear(); batch_msgs.clear()



        ## print(f"[Indexer] ➜ {page.source} pag {page.page}")

        except KeyboardInterrupt:
            break
        except KafkaException as e:
            print("[Indexer] Kafka error:", e)
        except Exception as e:
            print("[Indexer] Error general:", e)
            print(json.dumps(e.errors, indent=2))
            traceback.print_exc()
            time.sleep(5)   # pequeño back‑off
    if batch:
        vector_search.add_documents(batch)


# ----------------------------------------------

def start_news_indexer():
    conf = {"bootstrap.servers": BROKER,
            "group.id":"indexer_news","auto.offset.reset":"earliest"}
    c = confluent_kafka.Consumer(conf)
    c.subscribe(["market_news_raw"])

    es = ElasticsearchStore(
        embedding=embeddings,
        es_url=ELASTIC_URL,
        index_name="ext_docs_v1",
        query_field="text",
        vector_query_field="vector"
    )

    while True:
        m = c.poll(1.0)
        if m is None or m.error(): continue
        art = json.loads(m.value().decode())
        doc = Document(
            page_content=f"{art['headline']}. {art.get('summary','')}",
            metadata={"tickers": art.get("related","").split(","),
                      "source": art.get("url"),
                      "ts": art["datetime"]}
        )
        es.add_documents([doc])
        c.commit(m)
        # print("[News] indexed:", art["headline"][:60])

def start_prices_indexer():
    conf = {"bootstrap.servers": BROKER,
            "group.id":"indexer_prices",
            "auto.offset.reset":"earliest"}
    c = confluent_kafka.Consumer(conf)
    c.subscribe(["market_prices_raw"])

    # Si solo necesitas exponer precios al frontend NO hace falta embeddings.
    # Guardémoslos como documentos sin vector:
    from elasticsearch import Elasticsearch
    es = Elasticsearch(hosts=[ELASTIC_URL])

    while True:
        m = c.poll(1.0)
        if m is None or m.error(): continue
        price = json.loads(m.value().decode())
        es.index(index="prices_daily_v1", document=price)
        c.commit(m)
        # print("[Price] indexed:", price["ticker"], price["close"])

def start_snapshots_indexer():
    """
    Hilo que consume market_snapshots_raw y guarda el snapshot
    más reciente de cada ticker en prices_snap_v1.
    """
    from confluent_kafka import Consumer
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "indexer_snap",
        "auto.offset.reset": "earliest"
    })
    c.subscribe(["market_snapshots_raw"])

    from elasticsearch import Elasticsearch
    es = Elasticsearch(hosts=[ELASTIC_URL])

    while True:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        snap = json.loads(m.value().decode())
        # Upsert por ticker para quedarnos SOLO con el último
        es.index(
            index="prices_snap_v1",
            id=snap["ticker"],
            document=snap
        )
        c.commit(m)
        # print(f"[Snap] indexed: {snap['ticker']} → {snap['price']}")

def start_dividends_indexer():
    """
    Consume market_dividends_raw y almacena cada registro
    en Elasticsearch en el índice prices_dividends_v1.
    """
    from confluent_kafka import Consumer
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "indexer_div",
        "auto.offset.reset": "earliest"
    })
    c.subscribe(["market_dividends_raw"])

    from elasticsearch import Elasticsearch
    es = Elasticsearch(hosts=[ELASTIC_URL])

    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        div = json.loads(msg.value().decode())
        # Guardamos con id único ticker+fecha para evitar duplicados
        doc_id = f"{div['ticker']}_{div['ex_dividend_date']}"
        es.index(
            index="prices_dividends_v1",
            id=doc_id,
            document=div
        )
        c.commit(msg)
        # print(f"[Div] indexed: {doc_id}")

def start_index_summary_indexer():
    from confluent_kafka import Consumer
    c = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "indexer_indices_summary",
        "auto.offset.reset": "earliest"
    })
    c.subscribe(["market_indices_summary_raw"])
    from elasticsearch import Elasticsearch
    es = Elasticsearch(hosts=[ELASTIC_URL])

    while True:
        m = c.poll(1.0)
        if m is None or m.error(): continue
        summary = json.loads(m.value().decode())
        # Usamos 'symbol:date' como id para solo guardar uno por día
        doc_id = f"{summary['ticker']}_{summary['date']}"
        es.index(index="indices_summary_v1", id=doc_id, document=summary)
        c.commit(m)
        print(f"[IndexSummary] indexed: {doc_id}")

def ensure_es_index(es: Elasticsearch, index: str, body: dict):
    if es.indices.exists(index=index):
        current = es.indices.get_mapping(index=index)[index]["mappings"]
        desired = body["mappings"]
        if current != desired:
            es.indices.delete(index=index)
            es.indices.create(index=index, body=body)
    else:
        es.indices.create(index=index, body=body)

def build_elasticsearch() -> Elasticsearch:
    return Elasticsearch(hosts=[ELASTIC_URL])

# Mapeos “maestros” (cópialos de tu sección de pre-flight)
INDEX_DEFINITIONS = {
    "tbl_docs_v1": {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "content":        { "type": "text" },
                "vector":         { "type": "dense_vector", "dims": 512, "index": True, "similarity": "cosine" },
                "row_header":     { "type": "keyword" },
                "nivel":          { "type": "keyword" },
                "rentabilidad":   { "type": "double" },
                "benchmark":      { "type": "double" },
                "delta_benchmark":{ "type": "double" },
                "contribucion":   { "type": "double" },
                "peso_portafolio":{ "type": "double" },
                "variacion_usd":  { "type": "double" },
                "delta_usd":      { "type": "double" },
                "delta_pct":      { "type": "double" },
                "ticker":         { "type": "keyword" },
                "asset_class":    { "type": "keyword" },
                "sector":         { "type": "keyword" },
                "source":         { "type": "keyword" },
                "page":           { "type": "integer" }
            }
        }
    },
    "docs_chatbotres_v3": {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "metadata": {
                    "properties": {
                        "page":   { "type": "long" },
                        "source": { "type": "keyword" }
                    }
                },
                "content": {
                    "type": "text",
                    "analyzer": "standard"
                },
                "vector": {
                    "type":       "dense_vector",
                    "dims":       512,
                    "index":      True,
                    "similarity": "cosine"
                }
            }
        }
    },
    "ext_docs_v1": {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "metadata": {
                    "properties": {
                        "page":    { "type": "long"    },
                        "source":  { "type": "keyword" },
                        "tickers": { "type": "text", "fields": { "keyword": { "type": "keyword","ignore_above":256 } } },
                        "ts":      { "type": "long"    }
                    }
                },
                "text":   { "type": "text" },
                "vector": { "type": "dense_vector", "dims":512, "index":True, "similarity":"cosine" }
            }
        }
    },
    "cdocs_demo_v1": {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "answer": { "type":"text", "fields":{ "keyword":{ "type":"keyword","ignore_above":256 } } },
                "login":  { "type":"text", "fields":{ "keyword":{ "type":"keyword","ignore_above":256 } } },
                "query":  { "type":"text", "fields":{ "keyword":{ "type":"keyword","ignore_above":256 } } },
                "reqid":  { "type":"text", "fields":{ "keyword":{ "type":"keyword","ignore_above":256 } } },
                "role":   { "type":"text", "fields":{ "keyword":{ "type":"keyword","ignore_above":256 } } },
                "ts":     { "type":"long" }
            }
        }
    },
    "prices_snap_v1": {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "price":  { "type":"long" },
                "ticker": { "type":"text", "fields":{ "keyword":{ "type":"keyword","ignore_above":256 } } },
                "ts":     { "type":"long" },
                "volume": { "type":"long" }
            }
        }
    },
    "prices_daily_v1": {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "close":  { "type":"float" },
                "high":   { "type":"float" },
                "low":    { "type":"float" },
                "open":   { "type":"float" },
                "ticker": { "type":"text", "fields":{ "keyword":{ "type":"keyword","ignore_above":256 } } },
                "ts":     { "type":"long" },
                "volume": { "type":"float" }
            }
        }
    },
    "prices_dividends_v1": {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "cash_amount":      { "type":"float" },
                "declaration_date": { "type":"date"  },
                "ex_dividend_date": { "type":"date"  },
                "frequency":        { "type":"long"  },
                "record_date":      { "type":"date"  },
                "ticker":           { "type":"text", "fields":{ "keyword":{ "type":"keyword","ignore_above":256 } } }
            }
        }
    },
    "indices_summary_v1": {
        "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
        "mappings": {
            "properties": {
                "after_hours": { "type":"float" },
                "close":       { "type":"float" },
                "date":        { "type":"date"  },
                "high":        { "type":"float" },
                "low":         { "type":"float" },
                "open":        { "type":"float" },
                "ticker":      { "type":"text", "fields":{ "keyword":{ "type":"keyword","ignore_above":256 } } }
            }
        }
    }
}



@app.route("/admin/reset_indices", methods=["POST"])
def reset_indices():

    es = build_elasticsearch()
    errores = []
    for idx, body in INDEX_DEFINITIONS.items():
        try:
            ensure_es_index(es, idx, body)
        except Exception as e:
            errores.append(f"{idx}: {e}")
    if errores:
        return jsonify({"status": "error", "details": errores}), 500
    return jsonify({"status": "ok", "reset": list(INDEX_DEFINITIONS.keys())}), 200

@app.route("/jobportal/querydoc", methods=["POST"])
def query_docs():
    try:
        # --- 1. Parseo seguro del JSON recibido -----------------------------
        try:
            data = request.get_json(force=True)  # fuerza JSON o lanza excepción
        except Exception as e:
            print(f"Error al parsear JSON: {e}", file=sys.stderr)
            return jsonify({"error": "invalid JSON payload"}), 400

        # --- 2. Validaciones de campos requeridos ---------------------------
        required = ("sessionid", "searchquery", "context")
        missing  = [k for k in required if k not in data]
        if missing:
            return jsonify({"error": f"missing fields: {', '.join(missing)}"}), 400

        # --- 3. Extracción de variables -------------------------------------
        userid      = request.args.get("login")          # opcional
        session_id  = data["sessionid"]
        question    = data["searchquery"]
        context     = str(data["context"])
        reqid       = str(uuid.uuid4())

        # --- 4. Lógica principal --------------------------------------------
        print(f"Session: {session_id} | Question: {question}", file=sys.stderr)

        publish_chatbotreq(
            question, userid, reqid, context, session_id, "not yet formed"
        )

        # Devolvemos 202 porque la respuesta real se obtendrá más adelante
        return json_util.dumps({"reqid": reqid}), 202

    except Exception as e:
        # Cualquier error inesperado llega aquí
        print(f"Error interno en /jobportal/querydoc: {e}", file=sys.stderr)
        return jsonify({"error": "internal server error"}), 500


@app.route("/jobportal/recentqueries", methods=["GET"])
def recent_queries():
    login = request.args.get("login")
    role  = request.args.get("role", "AskDoc")
    N     = 3

    # 1️⃣ Buscamos en el índice donde guardas las respuestas finales.
    # Supongamos que en Elasticsearch indexaste cada res con:
    #   { "login": ..., "role": ..., "query": ..., "answer": ..., "ts": epoch_ms }
    body = {
        "size": N,
        "sort": [{ "ts": { "order": "desc" }}],
        "query": {
            "bool": {
                "filter": [
                    { "term": { "login.keyword": login }},
                    { "term": { "role.keyword":  role  }}
                ]
            }
        },
        "_source": ["query", "answer"]   # solo necesitamos estos campos
    }

    try:
        res = client.search(index="cdocs_demo_v1", body=body)
        hits = res.get("hits", {}).get("hits", [])
        items = [h["_source"] for h in hits]          # list(dict)

    except Exception as e:
        # loguea e si quieres
        items = []

    return jsonify({"items": items}), 200

#----proyectito----
@app.route("/jobportal/answer_full")
def get_answer_full():
    reqid = request.args.get("reqid")
    if not reqid:
        return jsonify({"error": "reqid is required"}), 400

    body = {
        "size": 1,
        "query": {"term": {"reqid.keyword": reqid}},
        "_source": ["query", "answer", "contexts"]
    }
    try:
        res = client.search(index="cdocs_demo_v1", body=body)
        hits = res.get("hits", {}).get("hits", [])
    except Exception as e:
        print(f"Error al buscar reqid={reqid}: {e}", file=sys.stderr)
        return jsonify({"error": "internal server error"}), 500

    if not hits:
        return jsonify({"status": "pending"}), 202

    src = hits[0]["_source"]
    return jsonify({
        "query": src.get("query"),
        "answer": src.get("answer"),
        "contexts": src.get("contexts", [])
    }), 200

@app.route("/jobportal/answer")
def get_answer():
    reqid = request.args.get("reqid")
    if not reqid:
        return jsonify({"error": "reqid is required"}), 400

    body = {
        "size": 1,
        "query": {"term": {"reqid.keyword": reqid}},
        "_source": ["answer"],
    }

    try:
        res = client.search(index="cdocs_demo_v1", body=body)
        hits = res.get("hits", {}).get("hits", [])
    except Exception as e:
        # Imprime el error en los logs (stderr) y devuelve un 500
        print(f"Error al buscar reqid={reqid}: {e}", file=sys.stderr)
        return jsonify({"error": "internal server error"}), 500

    if not hits:
        return jsonify({"status": "pending"}), 202   # todavía no está

    return jsonify({"answer": hits[0]["_source"]["answer"]}), 200

@app.route("/jobportal/docs", methods=["DELETE"])
def delete_user_docs():
    """
    Elimina TODO lo cargado por un usuario:
    • Chunks de documentos  -> docs_chatbotres_v3
    • Respuestas grabadas   -> cdocs_demo_v1
    """
    userid = request.args.get("userid")
    if not userid:
        return jsonify({"error": "userid is required"}), 400

    indices = [ "cdocs_demo_v1"]      # añade más si hiciste otros
    deleted_total = 0

    for idx in indices:
        try:
            resp = client.delete_by_query(
                index     = idx,
                body      = { "query": { "term": { "login.keyword": userid } } },
                conflicts = "proceed",
                refresh   = True                 # fuerza refresh inmediato
            )
            deleted_total += resp["deleted"]
        except NotFoundError:
            # El índice aún no existe; lo ignoramos
            continue
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return jsonify({
        "status": "ok",
        "userid": userid,
        "deleted_docs": deleted_total
    }), 200


@app.route("/jobportal/uploaddoc", methods=['POST'])
def upload_doc():
    t0 = time.time()                      # ─┐ 1) wall-clock timing
    req_id = str(uuid.uuid4())            # ─┘    simple correlation id

    def log(msg, **kv):
        """Structured-ish print; everything goes to stderr."""
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        ctx   = " ".join(f"{k}={v}" for k, v in kv.items())
        print(f"[{stamp}] req={req_id} {msg} {ctx}", file=sys.stderr, flush=True)

    try:
        file   = request.files.get("file")
        userid = request.form.get("userid")
        if not file or not userid:
            log("missing file or userid", userid=userid, has_file=bool(file))
            return jsonify({"error": "Missing file or userid"}), 400

        doc_name  = file.filename
        save_path = os.path.join(app.config['upload_folder'], doc_name)
        _, ext    = os.path.splitext(doc_name.lower())

        log("received upload",
            userid=userid, doc=doc_name, ext=ext, size=request.content_length)

        # ---------- persist the file ----------
        try:
            file.save(save_path)
            log("file saved", path=save_path)
        except Exception as e:
            log("failed to save file", error=e)
            raise                                           # bubble up to outer except

        # ---------- branch by extension ----------
        if ext == ".pdf":
            try:
                loader = PyPDFLoader(save_path)
                pages  = loader.load_and_split()
                log("pdf loaded", pages=len(pages))

                for pg in pages:
                    page_no = pg.metadata.get("page", -1)
                    text    = pg.page_content

                    for chunk in SPLITTER.split_text(text):
                        publish_uploaddoc(
                            doc_name,
                            userid,
                            page_no,
                            chunk,
                            str(uuid.uuid4())
                        )

                log("publish_uploaddoc completed", total_pages=len(pages))

            except Exception as e:

                log("PDF processing error", error=e)
                log(traceback.format_exc())
                return jsonify({"error": "Failed to process PDF"}), 500

        elif ext == ".csv":
            try:
                df = pd.read_csv(save_path)
                log("csv loaded", rows=len(df))
                for idx, row in df.iterrows():
                    text = row.to_json(orient="records")
                    publish_uploaddoc(doc_name, userid, idx, text, str(uuid.uuid4()))
            except Exception as e:
                log("CSV processing error", error=e)
                log(traceback.format_exc())
                return jsonify({"error": "Failed to process CSV"}), 500

        elif ext in (".xls", ".xlsx"):
            try:
                sheets = pd.read_excel(save_path, sheet_name=None)
                log("excel loaded", sheets=len(sheets))
                for sheet_name, df in sheets.items():
                    log("processing sheet", sheet=sheet_name, rows=len(df))
                    for idx, row in df.iterrows():
                        text = row.to_json(orient="records")
                        publish_uploaddoc(f"{doc_name}::{sheet_name}", userid, idx, text, str(uuid.uuid4()))
            except Exception as e:
                log("Excel processing error", error=e)
                log(traceback.format_exc())
                return jsonify({"error": "Failed to process Excel file"}), 500

        else:
            log("unsupported format", ext=ext)
            return jsonify({"error": f"Formato no soportado: {ext}"}), 400

        # ---------- success ----------
        _PRODUCER_U.flush(10)

        dt = int((time.time() - t0) * 1000)
        log("upload complete", ms=dt)
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        # *Anything* not caught above lands here
        log("unexpected error", error=e)
        log(traceback.format_exc())
        return jsonify({"error": "Unexpected error"}), 500

# publish upload doc
def publish_uploaddoc(filename,userid,pagenum,content,chunk_id):
    try:
        def acked(err, msg):
            global delivered_records
            """
                Delivery report handler called on successful or failed delivery of message
            """
            if err is not None:
                print("Failed to deliver message: {}".format(err))
            else:
                delivered_records += 1
                # print("Produced record to topic {} partition [{}] @ offset {}"
                #    .format(msg.topic(), msg.partition(), msg.offset()))
        producer = _PRODUCER_U
        uploaddoc_object = ccloud_lib.Uploaddoc()
        uploaddoc_object.source = filename
        uploaddoc_object.loginname = userid
        uploaddoc_object.page = pagenum
        uploaddoc_object.text = content
        uploaddoc_object.chunk_id = chunk_id
        producer.produce(topic="source_docs_v3", value=uploaddoc_object, on_delivery=acked)
        producer.poll(0)

        ## remaining = producer.flush(10)      # keep this line – it still blocks ≤10 s
        ## print(f"↘︎ FLUSH-DONE      outq={remaining}")   # before: _PRODUCER.outq_len()
    except Exception as e:
        print("An error occured:", e)
# publish upload doc


# ------------- NEW publish_chatbotreq ---------------------------------
def publish_chatbotreq(query, userid, reqid, context, session_id, answer):
    """
    Build the Avro object and ship it to Kafka, writing **un-buffered**
    diagnostics to STDERR so they are always visible in kubectl logs.
    """

    # -------- delivery report (runs in librdkafka thread) --------------
    def delivery_cb(err, msg):
        if err:
            print(f"[PRODUCE❌] {reqid} → {err}", file=sys.stderr, flush=True)
        else:
            print(f"[PRODUCE✅] {reqid} written to "
                  f"{msg.topic()}[{msg.partition()}]@{msg.offset()}",
                  file=sys.stderr, flush=True)

    try:
        # 1️⃣  Build the Avro object
        obj = ccloud_lib.Chatbotreq()
        obj.query       = query
        obj.loginname   = userid
        obj.reqid       = reqid
        obj.context     = context
        obj.session_id  = session_id
        obj.answer      = answer

        # 2️⃣  Lazily create a single producer for the whole process
        global _PRODUCER
        if "_PRODUCER" not in globals():
            conf = ccloud_lib.pop_schema_registry_params_from_config(confproducer)
            conf["value.serializer"] = chatbotreq_avro_serializer
            _PRODUCER = SerializingProducer(conf)
            # print(f"🚀 PRODUCER-READY   broker={conf['bootstrap.servers']} "
            #      f"topic={chatbotreqtopic}",
            #      file=sys.stderr, flush=True)

        # 3️⃣  Produce & poll (no outq_len)
        ## print(f" → PRODUCING        reqid={reqid}",
        ##       file=sys.stderr, flush=True)
        _PRODUCER.produce(
            topic=chatbotreqtopic,
            value=obj,
            on_delivery=delivery_cb
        )

        # Instead of outq_len, just always poll (or optionally flush at checkpoints)
        _PRODUCER.poll(0)

    except Exception as e:
        print(f"💥 PRODUCE-ERROR   {type(e).__name__}: {e}",
              file=sys.stderr, flush=True)
# ----------------------------------------------------------------------


if __name__ == "__main__":
    threading.Thread(target=start_indexer, daemon=True).start()
    threading.Thread(target=start_news_indexer, daemon=True).start()
    threading.Thread(target=start_prices_indexer, daemon=True).start()
    threading.Thread(target=start_snapshots_indexer, daemon=True).start()
    threading.Thread(target=start_dividends_indexer, daemon=True).start()
    threading.Thread(target=start_index_summary_indexer, daemon=True).start()
    #threading.Thread(target=start_table_indexer, daemon=True).start()
    socketio.run(app, debug=True, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)
    #app.run(debug=True,host='0.0.0.0')
    # socket_io.run(app,debug=True,port=5001)
    # app.run(debug=True,port=5001)