"""spark-kmeans.py

Script de clustering y recomendación basado en clusters usando Apache Spark.

Objetivo
--------
- Segmentar usuarios de MovieLens 1M con K-Means usando dos features simples:
    1) cantidad de películas calificadas (actividad)
    2) promedio de rating dado (tendencia de calificación)
- Generar recomendaciones por usuario a partir de su cluster (popularidad/score
    dentro del cluster), excluyendo películas ya vistas.
- Evaluar calidad con Precision@K y Recall@K usando un split train/test.

Entradas
--------
- Dataset MovieLens 1M (por defecto carpeta `ml-1m`).
    Archivos esperados:
    - ratings.dat  (UserID::MovieID::Rating::Timestamp)
    - movies.dat   (MovieID::Title::Genres)

Salidas
-------
- Directorio JSON con métricas, clusters y recomendaciones por cada K.

Notas
-----
- Si el dataset está en GCS (ruta comienza con gs://), se intenta leer directo
    con Spark. Si falla, se hace fallback: el driver descarga los .dat con gcloud
    y se crea el DataFrame desde Pandas.
"""

import argparse
import os
import subprocess
import tempfile

import pandas as pd
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    explode,
    lit,
    log1p,
    row_number,
    split,
    when,
)
from pyspark.storagelevel import StorageLevel


# Script: Continuacion del laboratorio anterior
# Tema: Segmentacion de usuarios + sistema de recomendacion basado en clusters
# Dataset: MovieLens 1M
# Tecnologia: Apache Spark


def parse_args():
    """Parsea argumentos de línea de comandos.

    Permite configurar:
    - ruta del dataset (local o gs://)
    - directorio de salida
    - lista de valores de K a probar
    - top-N por usuario
    - umbral de relevancia para evaluación
    - semilla para reproducibilidad
    """
    parser = argparse.ArgumentParser(
        description="Sistema de recomendacion basado en clusters con Apache Spark."
    )
    parser.add_argument(
        "--dataset-path",
        default="ml-1m",
        help="Ruta al directorio ml-1m.",
    )
    parser.add_argument(
        "--output-dir",
        default="output/ml-1m-cluster-recommender",
        help="Directorio donde se guardaran los resultados en JSON.",
    )
    parser.add_argument(
        "--k-values",
        default="3,5,8",
        help="Valores de K a comparar. Ejemplo: 3,5,8",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Cantidad de recomendaciones por usuario.",
    )
    parser.add_argument(
        "--relevance-threshold",
        type=float,
        default=4.0,
        help="Rating minimo en test para considerar un item relevante.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Semilla para randomSplit y K-Means.",
    )
    return parser.parse_args()


def load_ratings(spark, dataset_path):
    """Carga ratings.dat como DataFrame Spark.

    Si `dataset_path` empieza con gs:// usa lectura desde GCS (con fallback).
    Caso local: lee archivo de texto y separa por '::'.
    """
    if dataset_path.startswith("gs://"):
        return load_ratings_from_gcs(spark, dataset_path)

    # MovieLens 1M almacena ratings con formato:
    # UserID::MovieID::Rating::Timestamp
    return (
        spark.read.text(f"{dataset_path}/ratings.dat")
        .select(split(col("value"), "::").alias("parts"))
        .select(
            col("parts")[0].cast("int").alias("userId"),
            col("parts")[1].cast("int").alias("movieId"),
            col("parts")[2].cast("float").alias("rating"),
            col("parts")[3].cast("long").alias("timestamp"),
        )
    )


def load_movies(spark, dataset_path):
    """Carga movies.dat como DataFrame Spark.

    Si `dataset_path` empieza con gs:// usa lectura desde GCS (con fallback).
    Caso local: lee archivo de texto y separa por '::'.
    """
    if dataset_path.startswith("gs://"):
        return load_movies_from_gcs(spark, dataset_path)

    # MovieLens 1M almacena peliculas con formato:
    # MovieID::Title::Genres
    return (
        spark.read.text(f"{dataset_path}/movies.dat")
        .select(split(col("value"), "::").alias("parts"))
        .select(
            col("parts")[0].cast("int").alias("movieId"),
            col("parts")[1].alias("title"),
            col("parts")[2].alias("genres"),
        )
    )


def download_from_gcs(bucket_file_path, local_file_path):
    """Descarga un archivo desde GCS al filesystem local.

    Nota: se ejecuta en el driver. Requiere que `gcloud` esté instalado y
    autenticado en la máquina donde corre el driver.
    """
    # El driver descarga el archivo desde GCS usando gcloud.
    subprocess.run(
        ["gcloud", "storage", "cp", bucket_file_path, local_file_path],
        check=True,
    )


def get_driver_cache_dir(dataset_path):
    """Devuelve un directorio local para cachear descargas del driver.

    Se usa para no re-descargar `ratings.dat`/`movies.dat` si ya están en el
    disco local del driver.
    """
    dataset_name = dataset_path.rstrip("/").split("/")[-1]
    cache_dir = os.path.join(tempfile.gettempdir(), "spark_movielens_cache", dataset_name)
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir


def load_ratings_from_gcs(spark, dataset_path):
    """Intenta leer ratings.dat desde GCS con Spark.

    Si el conector gs:// no está disponible o falla la lectura, hace fallback a
    descarga en el driver y creación de DataFrame desde Pandas.
    """
    try:
        return (
            spark.read.text(f"{dataset_path}/ratings.dat")
            .select(split(col("value"), "::").alias("parts"))
            .select(
                col("parts")[0].cast("int").alias("userId"),
                col("parts")[1].cast("int").alias("movieId"),
                col("parts")[2].cast("float").alias("rating"),
                col("parts")[3].cast("long").alias("timestamp"),
            )
        )
    except Exception as exc:
        print(
            "No fue posible leer gs:// directamente con Spark. "
            "Se usara fallback por descarga en el driver."
        )
        print(f"Detalle: {exc}")
        return load_ratings_from_gcs_via_driver(spark, dataset_path)


def load_ratings_from_gcs_via_driver(spark, dataset_path):
    """Fallback: descarga ratings.dat al driver y crea DataFrame.

    Ventajas: no depende del conector GCS en el cluster.
    Trade-off: el driver debe tener acceso a gcloud y al bucket.
    """
    # Este camino evita depender del conector gs:// en Spark.
    # El master descarga el archivo y crea el DataFrame desde el driver.
    cache_dir = get_driver_cache_dir(dataset_path)
    local_ratings_path = os.path.join(cache_dir, "ratings.dat")

    if not os.path.exists(local_ratings_path):
        print(f"Descargando ratings desde {dataset_path}/ratings.dat al master...")
        download_from_gcs(f"{dataset_path}/ratings.dat", local_ratings_path)

    ratings_pd = pd.read_csv(
        local_ratings_path,
        sep="::",
        engine="python",
        header=None,
        names=["userId", "movieId", "rating", "timestamp"],
    )

    return spark.createDataFrame(ratings_pd).repartition(8, "userId")


def load_movies_from_gcs(spark, dataset_path):
    """Intenta leer movies.dat desde GCS con Spark (con fallback al driver)."""
    try:
        return (
            spark.read.text(f"{dataset_path}/movies.dat")
            .select(split(col("value"), "::").alias("parts"))
            .select(
                col("parts")[0].cast("int").alias("movieId"),
                col("parts")[1].alias("title"),
                col("parts")[2].alias("genres"),
            )
        )
    except Exception as exc:
        print(
            "No fue posible leer gs:// directamente con Spark para movies.dat. "
            "Se usara fallback por descarga en el driver."
        )
        print(f"Detalle: {exc}")
        return load_movies_from_gcs_via_driver(spark, dataset_path)


def load_movies_from_gcs_via_driver(spark, dataset_path):
    """Fallback: descarga movies.dat al driver y crea DataFrame."""
    # El master descarga el archivo y lo convierte a DataFrame para repartirlo en Spark.
    cache_dir = get_driver_cache_dir(dataset_path)
    local_movies_path = os.path.join(cache_dir, "movies.dat")

    if not os.path.exists(local_movies_path):
        print(f"Descargando movies desde {dataset_path}/movies.dat al master...")
        download_from_gcs(f"{dataset_path}/movies.dat", local_movies_path)

    movies_pd = pd.read_csv(
        local_movies_path,
        sep="::",
        engine="python",
        header=None,
        names=["movieId", "title", "genres"],
        encoding="latin-1",
    )

    return spark.createDataFrame(movies_pd)


def build_movies_with_genres(movies_df):
    """Normaliza géneros para análisis por cluster.

    Convierte `genres` (string tipo "Action|Comedy|...") en filas (movieId, genre)
    explotando la lista. Luego filtra a un conjunto de géneros esperados.
    """
    genre_names = [
        "Action",
        "Adventure",
        "Animation",
        "Childrens",
        "Comedy",
        "Crime",
        "Documentary",
        "Drama",
        "Fantasy",
        "FilmNoir",
        "Horror",
        "Musical",
        "Mystery",
        "Romance",
        "SciFi",
        "Thriller",
        "War",
        "Western",
    ]

    # Se explotan los generos para poder analizarlos por cluster.
    return (
        movies_df.withColumn("genre", explode(split(col("genres"), "\\|")))
        .select("movieId", "genre")
        .filter(col("genre").isNotNull())
        .filter(col("genre").isin(genre_names))
    )


def describe_cluster(cluster_row):
    """Genera una descripción textual (simple) de un cluster.

    Usa dos promedios del cluster:
    - avg_movies_rated: actividad
    - avg_user_rating: sesgo de calificación
    """
    avg_movies = cluster_row["avg_movies_rated"]
    avg_rating = cluster_row["avg_user_rating"]

    if avg_movies > 300:
        activity_type = "Usuario muy activo"
    elif avg_movies > 100:
        activity_type = "Usuario activo"
    else:
        activity_type = "Usuario ocasional o moderado"

    if avg_rating >= 4.0:
        rating_type = "muy positivo"
    elif avg_rating < 3.2:
        rating_type = "mas critico"
    else:
        rating_type = "intermedio"

    return f"{activity_type} con perfil de calificacion {rating_type}"


def analyze_genres_by_cluster(train_ratings, user_clusters, movies_with_genres):
    """Cuenta géneros preferidos por cluster.

    Une:
    - ratings de train (qué vio y calificó el usuario)
    - cluster asignado al usuario
    - géneros por película

    Resultado: DataFrame (cluster, genre, count)
    """
    user_genres = (
        train_ratings.join(user_clusters.select("userId", "cluster"), "userId")
        .join(movies_with_genres, "movieId")
    )

    return user_genres.groupBy("cluster", "genre").count()


def build_user_features(ratings_df):
    """Construye features de usuario para clustering.

    Features elegidas (simples, interpretables):
    - movies_rated: cantidad de items calificados
    - avg_rating: promedio de rating dado
    """
    # Igual que en el laboratorio anterior:
    # actividad del usuario + promedio de rating dado.
    return ratings_df.groupBy("userId").agg(
        count("movieId").alias("movies_rated"),
        avg("rating").alias("avg_rating"),
    )


def scale_features(user_features_df):
    """Arma vector de features y lo estandariza (mean=0, std=1).

    K-Means es sensible a escalas: si una feature tiene valores mucho más
    grandes domina la distancia. Por eso usamos StandardScaler con media y
    desviación.
    """
    assembler = VectorAssembler(
        inputCols=["movies_rated", "avg_rating"],
        outputCol="features_raw",
    )
    features_vector = assembler.transform(user_features_df)

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True,
    )

    scaler_model = scaler.fit(features_vector)
    return scaler_model.transform(features_vector)


def evaluate_recommendations(recommendations_df, test_df, top_n, relevance_threshold):
    """Evalúa recomendaciones usando Precision@K y Recall@K.

    Definición de relevancia:
    - Una película es relevante para un usuario si en `test_df` su rating >= threshold.

    Métricas:
    - hits(u): recomendaciones top-N que aparecen como relevantes en test
    - precision@k(u) = hits(u) / N
    - recall@k(u) = hits(u) / relevant_count(u)
    Devuelve métricas globales (promedio) y por usuario.
    """
    # Peliculas relevantes en test: rating >= threshold
    relevant_test = (
        test_df.filter(col("rating") >= lit(relevance_threshold))
        .select("userId", "movieId")
        .distinct()
    )

    relevant_per_user = relevant_test.groupBy("userId").agg(
        count("movieId").alias("relevant_count")
    )

    hits = (
        recommendations_df.select("userId", "movieId")
        .join(relevant_test, ["userId", "movieId"], "inner")
        .groupBy("userId")
        .agg(count("movieId").alias("hits"))
    )

    per_user_metrics = (
        relevant_per_user.join(hits, "userId", "left")
        .fillna(0, subset=["hits"])
        .withColumn("precision_at_k", col("hits") / lit(top_n))
        .withColumn("recall_at_k", col("hits") / col("relevant_count"))
    )

    global_metrics = per_user_metrics.agg(
        avg("precision_at_k").alias("precision_at_k"),
        avg("recall_at_k").alias("recall_at_k"),
        count("userId").alias("evaluated_users"),
    )

    return global_metrics, per_user_metrics


def generate_recommendations_for_k(
    scaled_data,
    train_ratings,
    test_ratings,
    movies,
    k_value,
    top_n,
    relevance_threshold,
    seed,
):
    """Entrena K-Means con un K dado y genera recomendaciones.

    Flujo:
    1) Entrenar K-Means con features escaladas.
    2) Asignar cluster a cada usuario.
    3) Para cada cluster, calcular score por película basado en ratings de train.
    4) Para cada usuario, recomendar top-N de su cluster excluyendo películas ya vistas.
    5) Evaluar con test (Precision@K, Recall@K).

    Retorna un dict con DataFrames y métricas para persistir/inspeccionar.
    """
    # 6. APLICAR K-MEANS
    kmeans = KMeans(featuresCol="features", k=k_value, seed=seed)
    model = kmeans.fit(scaled_data)

    # 7. ASIGNAR CLUSTERS A LOS USUARIOS
    user_clusters = model.transform(scaled_data).select(
        "userId",
        col("prediction").alias("cluster"),
        "movies_rated",
        "avg_rating",
    )

    cluster_stats = user_clusters.groupBy("cluster").agg(
        avg("movies_rated").alias("avg_movies_rated"),
        avg("avg_rating").alias("avg_user_rating"),
        count("userId").alias("num_users"),
    )

    # 8. CONSTRUIR PELICULAS CANDIDATAS POR CLUSTER
    # Se agregan ratings de train por cluster y pelicula.
    cluster_movie_scores = (
        train_ratings.join(user_clusters.select("userId", "cluster"), "userId")
        .groupBy("cluster", "movieId")
        .agg(
            avg("rating").alias("cluster_avg_movie_rating"),
            count("rating").alias("cluster_rating_count"),
        )
        .withColumn(
            # Score = promedio del cluster * log(1 + cantidad_ratings)
            # La parte log suaviza la ventaja de películas con muchísimos votos.
            "recommendation_score",
            col("cluster_avg_movie_rating") * log1p(col("cluster_rating_count")),
        )
        .join(movies, "movieId", "left")
    )

    # 9. FILTRAR PELICULAS YA VISTAS POR CADA USUARIO
    seen_movies = train_ratings.select("userId", "movieId").distinct()

    candidate_recommendations = (
        user_clusters.select("userId", "cluster")
        .join(cluster_movie_scores, "cluster")
        # left_anti elimina pares (userId, movieId) que ya aparecieron en train.
        .join(seen_movies, ["userId", "movieId"], "left_anti")
    )

    # 10. RANKEAR Y QUEDARSE CON EL TOP-N
    ranking_window = Window.partitionBy("userId").orderBy(
        # Orden estable: prioriza score, luego soporte (cantidad ratings),
        # y finalmente movieId para desempatar.
        col("recommendation_score").desc(),
        col("cluster_rating_count").desc(),
        col("movieId").asc(),
    )

    top_recommendations = (
        candidate_recommendations.withColumn("rank", row_number().over(ranking_window))
        .filter(col("rank") <= lit(top_n))
        .select(
            "userId",
            "cluster",
            "movieId",
            "title",
            "genres",
            "cluster_avg_movie_rating",
            "cluster_rating_count",
            "recommendation_score",
            "rank",
        )
    )

    # 11. EVALUAR RECOMENDACIONES CON TEST
    global_metrics, per_user_metrics = evaluate_recommendations(
        top_recommendations,
        test_ratings,
        top_n,
        relevance_threshold,
    )

    global_metrics = global_metrics.withColumn("k", lit(k_value)).withColumn(
        "training_cost",
        lit(float(model.summary.trainingCost)),
    )

    sample_users = (
        top_recommendations.select("userId").distinct().orderBy("userId").limit(20)
    )
    sample_recommendations = top_recommendations.join(sample_users, "userId").orderBy(
        "userId", "rank"
    )

    return {
        "model": model,
        "user_clusters": user_clusters,
        "cluster_stats": cluster_stats,
        "recommendations": top_recommendations,
        "sample_recommendations": sample_recommendations,
        "global_metrics": global_metrics,
        "per_user_metrics": per_user_metrics,
    }


def main():
    """Punto de entrada.

    Ejecuta el pipeline completo:
    - carga y persistencia de datos
    - EDA básica
    - train/test split
    - features + escalado
    - entrenamiento/evaluación para múltiples K
    - persistencia de resultados a JSON
    """
    args = parse_args()
    k_values = [int(value.strip()) for value in args.k_values.split(",") if value.strip()]

    # 1. INICIALIZAR SPARK SESSION
    spark = SparkSession.builder.appName(
        "MovieLens K-Means Clustering and Recommendation"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark session iniciada")

    # 2. CARGAR DATOS
    print("\nCargando datos de MovieLens 1M...")
    ratings = load_ratings(spark, args.dataset_path).persist(StorageLevel.MEMORY_AND_DISK)
    movies = load_movies(spark, args.dataset_path).persist(StorageLevel.MEMORY_AND_DISK)
    movies_with_genres = build_movies_with_genres(movies).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    # Nota: `count()` fuerza ejecución (action) y sirve como sanity check.

    print(f"Calificaciones: {ratings.count()} registros")
    print(f"Peliculas: {movies.count()} registros")

    # 3. ANALISIS EXPLORATORIO BASICO
    print("\nAnalisis exploratorio:")
    num_users = ratings.select("userId").distinct().count()
    num_movies = ratings.select("movieId").distinct().count()

    print(f"Numero de usuarios: {num_users}")
    print(f"Numero de peliculas calificadas: {num_movies}")

    print("\nDistribucion de ratings:")
    ratings.groupBy("rating").count().orderBy("rating").show()

    # 4. DIVIDIR EL DATASET EN TRAIN Y TEST
    print("\nDividiendo dataset en train/test...")
    train_ratings, test_ratings = ratings.randomSplit([0.8, 0.2], seed=args.seed)
    train_ratings = train_ratings.persist(StorageLevel.MEMORY_AND_DISK)
    test_ratings = test_ratings.persist(StorageLevel.MEMORY_AND_DISK)

    print(f"Train: {train_ratings.count()} registros")
    print(f"Test: {test_ratings.count()} registros")

    # 5. CONSTRUCCION DE FEATURES PARA CLUSTERING
    print("\nConstruyendo features para clustering...")
    user_features = build_user_features(train_ratings).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    user_features.show(10)

    print("\nPreparando vectores de features...")
    scaled_data = scale_features(user_features).persist(StorageLevel.MEMORY_AND_DISK)
    print("Datos normalizados listos")

    # 6-11. K-MEANS + RECOMENDACION + EVALUACION
    print("\nProbando diferentes valores de K...")
    all_metrics = None
    best_result = None
    best_precision = None

    for k_value in k_values:
        print(f"\nEntrenando y evaluando K-Means con K={k_value}...")
        result = generate_recommendations_for_k(
            scaled_data=scaled_data,
            train_ratings=train_ratings,
            test_ratings=test_ratings,
            movies=movies,
            k_value=k_value,
            top_n=args.top_n,
            relevance_threshold=args.relevance_threshold,
            seed=args.seed,
        )

        print("\nEstadisticas por cluster:")
        result["cluster_stats"].orderBy("cluster").show(truncate=False)

        print("\nInterpretacion de clusters:")
        cluster_rows = result["cluster_stats"].orderBy("cluster").collect()
        for cluster_row in cluster_rows:
            cluster_id = cluster_row["cluster"]
            cluster_description = describe_cluster(cluster_row)
            print(f"Cluster {cluster_id}: {cluster_description}")

        print("\nTop 5 generos por cluster:")
        cluster_genres = analyze_genres_by_cluster(
            train_ratings,
            result["user_clusters"],
            movies_with_genres,
        )
        for cluster_row in cluster_rows:
            cluster_id = cluster_row["cluster"]
            print(f"\nCluster {cluster_id}:")
            top_genres = (
                cluster_genres.filter(col("cluster") == lit(cluster_id))
                .orderBy(col("count").desc(), col("genre").asc())
                .limit(5)
                .collect()
            )
            for genre_row in top_genres:
                print(f" - {genre_row['genre']}: {genre_row['count']}")

        print("\nMetricas de recomendacion:")
        result["global_metrics"].show(truncate=False)

        output_base = f"{args.output_dir}/k={k_value}"
        result["user_clusters"].write.mode("overwrite").json(
            f"{output_base}/user_clusters"
        )
        result["cluster_stats"].write.mode("overwrite").json(
            f"{output_base}/cluster_stats"
        )
        cluster_genres.write.mode("overwrite").json(f"{output_base}/cluster_genres")
        # Verificar si hay recomendaciones antes de guardar
        if result["recommendations"].count() > 0:
            result["recommendations"].write.mode("overwrite").json(
                f"{output_base}/recommendations_topn"
            )
            print(f"✅ Guardadas {result['recommendations'].count()} recomendaciones para K={k_value}")
        else:
            print(f"⚠️ No hay recomendaciones para K={k_value}, guardando sample_recommendations como respaldo")
            # Guardar sample_recommendations como recommendations si no hay otras
            result["sample_recommendations"].write.mode("overwrite").json(
                f"{output_base}/recommendations_topn"
            )
        
        result["sample_recommendations"].write.mode("overwrite").json(
            f"{output_base}/sample_recommendations_20_users"
        )
        result["per_user_metrics"].write.mode("overwrite").json(
            f"{output_base}/per_user_metrics"
        )
        result["global_metrics"].write.mode("overwrite").json(f"{output_base}/metrics")

        metrics_row = result["global_metrics"].collect()[0]
        current_precision = metrics_row["precision_at_k"]

        if best_precision is None or current_precision > best_precision:
            best_precision = current_precision
            best_result = result

        if all_metrics is None:
            all_metrics = result["global_metrics"]
        else:
            all_metrics = all_metrics.unionByName(result["global_metrics"])

    # 12. COMPARAR CONFIGURACIONES DE K
    if all_metrics is not None:
        print("\nResumen comparativo entre configuraciones de K:")
        all_metrics.orderBy(col("precision_at_k").desc(), col("recall_at_k").desc()).show(
            truncate=False
        )
        all_metrics.write.mode("overwrite").json(f"{args.output_dir}/metrics_summary")

    # 13. MOSTRAR EJEMPLO DE RECOMENDACIONES PARA 20 USUARIOS
    if best_result is not None:
        print("\nEjemplo de recomendaciones para 20 usuarios:")
        best_result["sample_recommendations"].show(200, truncate=False)

    print(f"\nResultados guardados en: {args.output_dir}")
    print("\nProceso completado exitosamente")
    spark.stop()


if __name__ == "__main__":
    main()
