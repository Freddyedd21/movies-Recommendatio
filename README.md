# MovieRecs — Sistema de recomendación de películas (Spark + FastAPI)

Este repositorio implementa un sistema de recomendación de películas basado en **clustering de usuarios (K-Means)** con **Apache Spark** (batch) y una **API en FastAPI** para consultar recomendaciones (tiempo real). Incluye un **frontend estático** (HTML/CSS/JS) para explorar las recomendaciones.

## Arquitectura (alto nivel)

1. **Batch (Spark)**: `spark-kmeans.py`
   - Carga MovieLens 1M (`ratings.dat`, `movies.dat`).
   - Genera features por usuario (actividad + promedio de rating).
   - Entrena K-Means para varios valores de `K`.
   - Recomienda Top-N por usuario basado en su cluster.
   - Guarda resultados en JSON (carpetas por `k=`).

2. **Serving (API)**: `api_recommendation.py`
   - Carga un archivo **`recommendations_api.json`** en memoria.
   - Expone endpoints para consultar recomendaciones por usuario y listar usuarios.

3. **Frontend**: `frontend/`
   - UI para buscar un usuario, ver recomendaciones y listar usuarios.

## Estructura del repo

- `spark-kmeans.py`: pipeline de clustering + recomendaciones + métricas.
- `api_recommendation.py`: API (FastAPI) que sirve recomendaciones desde `recommendations_api.json`.
- `frontend/index.html`: UI web estática.
- `frontend/js/app.js`: lógica del frontend (consume la API).
- `frontend/css/style.css`: estilos.

## Requisitos

### Para el pipeline Spark
- Python 3.9+ (recomendado 3.10+)
- Java (requerido por Spark)
- PySpark
- Dataset **MovieLens 1M** (carpeta `ml-1m/` con `ratings.dat` y `movies.dat`)

### Para la API
- Python 3.9+
- `fastapi`, `uvicorn`, `pydantic`

> Nota: el repo no incluye `requirements.txt`. Abajo hay comandos `pip` sugeridos.

## Instalación rápida (Python)

En Windows (PowerShell), desde la raíz del repo:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install fastapi uvicorn pydantic pandas pyspark
```

## 1) Ejecutar el pipeline (Spark) para generar recomendaciones

Coloca el dataset MovieLens 1M en una carpeta llamada `ml-1m/` en la raíz del repo:

```
ml-1m/
  ratings.dat
  movies.dat
```

Ejecuta el pipeline (ejemplo con `spark-submit`):

```powershell
spark-submit .\spark-kmeans.py --dataset-path ml-1m --output-dir output\ml-1m-cluster-recommender --k-values 3,5,8 --top-n 10
```

Salida esperada (por cada K):

```
output/ml-1m-cluster-recommender/k=5/
  recommendations_topn/
  user_clusters/
  cluster_stats/
  metrics/
  ...
```

## 2) Construir `recommendations_api.json` (formato para la API)

La API espera un archivo **único** en la raíz del repo llamado `recommendations_api.json` con esta forma:

```json
[
  {
    "user_id": 1,
    "cluster": 2,
    "recommendations": [
      {"movie_id": 123, "movie_title": "Toy Story (1995)", "score": 12.34}
    ]
  }
]
```

Como el pipeline de Spark guarda recomendaciones *fila por fila* en `recommendations_topn/`, una forma simple de generar `recommendations_api.json` es convertir y agrupar esos JSON.

1) Elige qué `K` vas a usar (ej. `k=5`).
2) Ejecuta este script desde la raíz del repo (ajusta la ruta del directorio si cambiaste el `--output-dir`). En **Windows (PowerShell)** puedes correrlo así:

```powershell
$code = @'
import glob
import json
import pandas as pd

# Ajusta aquí el K elegido
k_value = 5
recs_dir = f"output/ml-1m-cluster-recommender/k={k_value}/recommendations_topn"

files = sorted(glob.glob(recs_dir + "/*.json"))
if not files:
  raise SystemExit(f"No se encontraron archivos en: {recs_dir}")

# Spark escribe JSON Lines (un objeto por línea)
df = pd.concat([pd.read_json(fp, lines=True) for fp in files], ignore_index=True)

# Columnas esperadas: userId, cluster, movieId, title, recommendation_score
needed = {"userId", "cluster", "movieId", "title", "recommendation_score"}
missing = needed - set(df.columns)
if missing:
  raise SystemExit(f"Faltan columnas {missing}. Columnas disponibles: {sorted(df.columns)}")

# Orden estable por score (desc)
df = df.sort_values(["userId", "recommendation_score"], ascending=[True, False])

payload = []
for (user_id, cluster), g in df.groupby(["userId", "cluster"], sort=True):
  recs = [
    {
      "movie_id": int(row.movieId),
      "movie_title": str(row.title),
      "score": float(row.recommendation_score),
    }
    for row in g.itertuples(index=False)
  ]
  payload.append({
    "user_id": int(user_id),
    "cluster": int(cluster),
    "recommendations": recs,
  })

with open("recommendations_api.json", "w", encoding="utf-8") as f:
  json.dump(payload, f, ensure_ascii=False, indent=2)

print(f"OK -> recommendations_api.json generado con {len(payload)} usuarios")
'@

python -c $code
```

## 3) Levantar la API (FastAPI)

La API **requiere** que exista `recommendations_api.json` en la raíz del repo.

Ejecuta:

```powershell
uvicorn api_recommendation:app --reload --host 0.0.0.0 --port 8000
```

Endpoints útiles:
- `GET /health`
- `GET /recommendations?limit=100&offset=0`
- `GET /recommendations/{user_id}`

## 4) Usar el frontend

Abre `frontend/index.html` en tu navegador (o usa una extensión como *Live Server* en VS Code).

### Configurar la URL de la API

El frontend consume la API usando la constante `API_BASE_URL` en `frontend/js/app.js`.

- Si corres la API localmente, normalmente será: `http://localhost:8000`
- En el repo actualmente está hardcodeada una IP pública.

Cambia esta línea si lo necesitas:

```js
const API_BASE_URL = 'http://localhost:8000';
```

## Notas / Problemas conocidos

- En `frontend/index.html` el `<link>` del CSS apunta a `css/styles.css`, pero el archivo existente es `frontend/css/style.css`. Si no ves estilos, revisa esa ruta.
- La API carga todo `recommendations_api.json` en memoria al iniciar; si el JSON es muy grande, considera reducir `top-n` o generar un subset.

## Licencia

No especificada.
