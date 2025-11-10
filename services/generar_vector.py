from langchain_openai.embeddings import OpenAIEmbeddings
import os
import json

# Asegúrate de que la variable de entorno esté disponible
os.environ["OPENAI_API_KEY"] = "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"

# Inicializa el modelo
embeddings = OpenAIEmbeddings(model="text-embedding-ada-002")

# Genera el vector
vector = embeddings.embed_query("Este es un ejemplo para generar un vector.")

# Verifica
print("Dimensión:", len(vector))
print("Primeros 10 valores:", vector[:10])

# Guarda el vector como JSON para copiar fácilmente al curl
with open("vector_1536.json", "w") as f:
    json.dump(vector, f)

print("✅ Vector guardado en vector_1536.json")
