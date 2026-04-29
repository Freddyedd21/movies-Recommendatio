#!/usr/bin/env python3
"""api_recommendation.py

API de recomendaciones de películas (FastAPI).

Contexto
--------
Este archivo implementa una API ligera para *consultar* recomendaciones ya
calculadas previamente por un proceso batch (por ejemplo, el pipeline en Spark
de `spark-kmeans.py`).

Idea principal
--------------
- El clustering + cálculo de recomendaciones es costoso (batch).
- La API solo carga un JSON con recomendaciones y expone endpoints para
  consultarlas (tiempo real).

Entrada
-------
- Archivo `recommendations_api.json` (lista de usuarios con su cluster y lista
  de películas recomendadas).

Nota
----
Esta API carga todo el JSON en memoria al iniciar (diccionario por `user_id`).
Es simple y rápida para consultas, pero depende del tamaño del JSON.
"""

import json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List


# -----------------------------------------------------------------------------
# Modelos Pydantic (esquemas de entrada/salida)
# -----------------------------------------------------------------------------

class Recommendation(BaseModel):
    """Representa una recomendación individual.

    - movie_id: id de la película
    - movie_title: título de la película
    - score: score calculado por el proceso batch (ej. Spark)
    """
    movie_id: int
    movie_title: str
    score: float


class UserRecommendations(BaseModel):
    """Representa el paquete de recomendaciones de un usuario.

    - user_id: id del usuario
    - cluster: cluster asignado al usuario en el modelo (K-Means)
    - recommendations: lista de `Recommendation`
    """
    user_id: int
    cluster: int
    recommendations: List[Recommendation]


# -----------------------------------------------------------------------------
# Inicialización de la app FastAPI
# -----------------------------------------------------------------------------

app = FastAPI(title="Movie Recommendations API")

# CORS habilitado para permitir consumo desde cualquier origen (por ejemplo,
# una app web o un cliente en otra máquina). Para producción, normalmente se
# restringe `allow_origins` a dominios específicos.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------------------------
# Carga del “dataset” de recomendaciones (JSON) en memoria
# -----------------------------------------------------------------------------

# El JSON debe ser una lista de elementos, cada uno con:
# - user_id
# - cluster
# - recommendations: lista de {movie_id, movie_title, score}

with open("recommendations_api.json", "r") as f:
    data = json.load(f)

# `db` actúa como una base de datos en memoria:
# - key: user_id
# - value: UserRecommendations
# Esto permite responder /recommendations/{user_id} en tiempo O(1).
db = {}
for item in data:
    db[item["user_id"]] = UserRecommendations(
        user_id=item["user_id"],
        cluster=item["cluster"],
        recommendations=[Recommendation(**r) for r in item["recommendations"]]
    )

# Mensaje de diagnóstico al iniciar (se verá en consola/terminal).
print(f"Cargados {len(db)} usuarios")


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------

@app.get("/")
def root():
    """Endpoint raíz.

    Útil para verificar rápidamente que la API está corriendo y ver cuántos
    usuarios fueron cargados desde el JSON.
    """
    return {"api": "Movie Recommendations", "total_users": len(db)}


@app.get("/recommendations")
def get_all(limit: int = 100, offset: int = 0):
    """Devuelve recomendaciones para múltiples usuarios.

    Implementa paginado simple:
    - limit: cuántos usuarios devolver
    - offset: desde qué posición comenzar

    Nota: retorna una lista de usuarios serializados a dict.
    """
    users = list(db.values())
    return {
        "total": len(users),
        "limit": limit,
        "offset": offset,
        "users": [u.dict() for u in users[offset:offset+limit]]
    }


@app.get("/recommendations/{user_id}")
def get_user(user_id: int):
    """Devuelve recomendaciones para un usuario específico.

    Manejo de error:
    - Si el `user_id` no existe en el JSON cargado, retorna HTTP 404.
    """
    if user_id not in db:
        # `HTTPException` hace que FastAPI responda con el status code indicado.
        raise HTTPException(404, f"Usuario {user_id} no encontrado")
    return db[user_id]


@app.get("/health")
def health():
    """Endpoint de salud (health check).

    Se usa para monitoreo básico o para pruebas automáticas de disponibilidad.
    """
    return {"status": "ok", "users_loaded": len(db)}


# -----------------------------------------------------------------------------
# Ejecución directa del módulo
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # Permite levantar la API ejecutando:
    #   python api_recommendation.py
    # Aunque también es común correrlo con:
    #   uvicorn api_recommendation:app --host 0.0.0.0 --port 8000
    import uvicorn
    uvicorn.run("api_recommendation:app", host="0.0.0.0", port=8000)