#!/usr/bin/env python
"""
Indexer:
  • Consume chunks del topic source_docs_v1 (Avro Uploaddoc)
  • Calcula embeddings text‑embedding‑ada‑002
  • Indexa {text, vector, metadata} en docs_chatbotres_step_final_v1
"""

import logging
import os
import sys
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError
from langchain_openai import OpenAIEmbeddings
from langchain_elasticsearch import ElasticsearchStore

import ccloud_lib

# ------------------------- parámetros fijos -------------------------------
INDEX_NAME = "docs_chatbotres_step_final_v1"
TEXT_FIELD = "text"
VECTOR_FIELD = "vector"
VECTOR_DIMS = 1536

# ------------------------------ logging -----------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s | %(message)s",
)
log = logging.getLogger("indexer")

# ----------------------------- helpers ------------------------------------
def ensure_index(es: Elasticsearch) -> None:
    """Crea el índice (o añade el campo text) si aún no está listo."""
    if es.indices.exists(index=INDEX_NAME):
        mapping = es.indices.get_mapping(index=INDEX_NAME)[INDEX_NAME]["mappings"]
        if TEXT_FIELD not in mapping["properties"]:
            es.indices.put_mapping(index=INDEX_NAME, body={
                "properties": {TEXT_FIELD: {"type": "text"}}
            })
            log.info("Campo %s agregado al índice %s", TEXT_FIELD, INDEX_NAME)
        return

    body = {
        "mappings": {
            "properties": {
                TEXT_FIELD: {"type": "text"},
                VECTOR_FIELD: {
                    "type": "dense_vector",
                    "dims": VECTOR_DIMS,
                    "index": True,
                    "similarity": "cosine"
                },
                "source": {"type": "keyword"},
                "page":   {"type": "integer"},
                "chunk":  {"type": "keyword"}
            }
        }
    }
    es.indices.create(index=INDEX_NAME, body=body)
    log.info("Índice %s creado", INDEX_NAME)

# ------------------------------ main --------------------------------------
def main() -> None:
    # --------- argumentos ----------
    args = ccloud_lib.parse_args()
    topic = args.resumereq or "source_docs_v1"   # flag en ccloud_lib
    conf  = ccloud_lib.read_ccloud_config(args.config_file)

    # --------- Elasticsearch + embeddings ----------
    es = Elasticsearch("http://localhost:9200")
    ensure_index(es)

    emb = OpenAIEmbeddings(model="text-embedding-ada-002")
    store = ElasticsearchStore(
        es_url="http://localhost:9200",
        index_name=INDEX_NAME,
        embedding=emb,
        query_field=TEXT_FIELD,
        vector_query_field=VECTOR_FIELD,
        distance_strategy="COSINE"
    )

    # --------- Kafka consumer ----------
    sr = SchemaRegistryClient({"url": conf["schema.registry.url"]})
    deser = AvroDeserializer(
        sr,
        ccloud_lib.uploaddoc_schema,
        ccloud_lib.Uploaddoc.dict_to_uploaddoc
    )

    cons_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    cons_conf.update({
        "value.deserializer": deser,
        "group.id": "rag-indexer",
        "auto.offset.reset": "earliest"
    })
    consumer = DeserializingConsumer(cons_conf)
    consumer.subscribe([topic])
    log.info("🟢 Escuchando %s", topic)

    # --------- bucle principal ----------
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            doc = msg.value()      # objeto Uploaddoc
            if not doc or not doc.text:
                continue

            try:
                store.add_texts(
                    [doc.text],
                    metadatas=[{
                        "source": doc.source,
                        "page":   doc.page,
                        "chunk":  doc.chunk_id
                    }]
                )
                log.info("✓ Página %s indexada (offset %s)", doc.page, msg.offset())

            except BulkIndexError as be:
                err = be.errors[0]["index"]["error"]
                log.error("BulkIndexError: %s", err["reason"])
                raise
    except KeyboardInterrupt:
        log.info("⏹  Interrumpido por usuario")
    finally:
        consumer.close()

# --------------------------- ejecución ------------------------------------
if __name__ == "__main__":
    if "OPENAI_API_KEY" not in os.environ:
        log.error("Falta la variable de entorno OPENAI_API_KEY")
        sys.exit(1)
    main()
