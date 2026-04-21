from pathlib import Path
from sentence_transformers import SentenceTransformer
import oracledb
from dotenv import load_dotenv
import os


BASE_DIR = Path(__file__).resolve().parent.parent


def load_config():
    load_dotenv(BASE_DIR / ".env")

    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    service_name = os.getenv("DB_SERVICE")

    if not all([username, password, host, port, service_name]):
        raise ValueError("Missing DB settings in .env")

    return username, password, host, port, service_name


def get_connection():
    username, password, host, port, service_name = load_config()

    dsn = oracledb.makedsn(host, int(port), service_name=service_name)

    return oracledb.connect(
        user=username,
        password=password,
        dsn=dsn
    )


def get_sql(search_mode):
    # Varianta 1 = exact search
    # mai precis
    # mai corect matematic
    # mai lent pe volume mari
    # nu are nevoie neaparat de vector index

    if search_mode == "1":
        return """
        SELECT movie_id,
               title,
               overview,
               VECTOR_DISTANCE(embedding, :query_vector, COSINE) AS vector_distance,
               1 - VECTOR_DISTANCE(embedding, :query_vector, COSINE) AS similarity_score
        FROM movies
        ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE)
        FETCH EXACT FIRST 10 ROWS ONLY
        """, "EXACT SEARCH"

    # Varianta 2 = approximate search
    # mult mai rapid pe volume mari
    # foloseste vector index
    # poate rata uneori un rezultat care ar fi fost in topul exact
    # este compromisul clasic: viteza vs perfectiune
    elif search_mode == "2":
        return """
        SELECT movie_id,
               title,
               overview,
               VECTOR_DISTANCE(embedding, :query_vector, COSINE) AS vector_distance,
               1 - VECTOR_DISTANCE(embedding, :query_vector, COSINE) AS similarity_score
        FROM movies
        ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE)
        FETCH APPROX FIRST 10 ROWS ONLY WITH TARGET ACCURACY 80
        """, "APPROXIMATE SEARCH"

    else:
        return None, None


def semantic_search():
    connection = get_connection()
    cursor = connection.cursor()

    model = SentenceTransformer("all-MiniLM-L6-v2")

    print("Alege varianta de cautare:")
    print("1 - Exact search")
    print("2 - Approximate search")
    search_mode = input("Optiune: ").strip()

    sql, mode_label = get_sql(search_mode)

    if not sql:
        print("Optiune invalida.")
        cursor.close()
        connection.close()
        return

    query_text = input("Query: ").strip()

    if not query_text:
        print("Query cannot be empty.")
        cursor.close()
        connection.close()
        return

    query_vector = model.encode(query_text).tolist()
    vector_str = "[" + ",".join(map(str, query_vector)) + "]"

    try:
        cursor.execute(sql, {"query_vector": vector_str})
        results = cursor.fetchall()
    except oracledb.DatabaseError as e:
        print("Eroare la rularea query-ului:")
        print(e)
        cursor.close()
        connection.close()
        return

    print(f"\n{mode_label}")
    print(f"Rezultate pentru: {query_text}\n")

    for row in results:
        movie_id, title, overview_lob, vector_distance, similarity_score = row
        overview = overview_lob.read() if overview_lob else None

        print(f"ID: {movie_id}")
        print(f"Title: {title}")
        print(f"Vector distance: {vector_distance}")
        print(f"Similarity score: {similarity_score}")
        print(f"Overview: {overview}")
        print("-" * 80)

    cursor.close()
    connection.close()


if __name__ == "__main__":
    semantic_search()