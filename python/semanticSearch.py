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


def semantic_search():
    connection = get_connection()
    cursor = connection.cursor()

    model = SentenceTransformer("all-MiniLM-L6-v2")

    query_text = input("Query: ").strip()

    if not query_text:
        print("Query cannot be empty.")
        cursor.close()
        connection.close()
        return

    query_vector = model.encode(query_text).tolist()
    vector_str = "[" + ",".join(map(str, query_vector)) + "]"

    sql = """
    SELECT movie_id,
           title,
           overview,
           VECTOR_DISTANCE(embedding, :query_vector, COSINE) AS vector_distance,
           1 - VECTOR_DISTANCE(embedding, :query_vector, COSINE) AS similarity_score
    FROM movies
    ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE)
    FETCH FIRST 10 ROWS ONLY
    """

    cursor.execute(sql, {"query_vector": vector_str})
    results = cursor.fetchall()

    print(f"\nRezultate pentru: {query_text}\n")

    for row in results:
        movie_id, title, overview_lob, vector_distance, similarity_score = row
        overview = overview_lob.read() if overview_lob else None

        print(f"ID: {movie_id}")
        print(f"Title: {title}")
        print(f"Vector distance: {vector_distance}") # cu cat e mai mica, cu atat rezultatul este mai apropiat semantic
        print(f"Similarity score: {similarity_score}") #cu cat e mai mare, cu atat rezultatul este mai asemanator semantic. 
                                                    # pentru cosine, acesta este derivat direct din distanta prin 1 - distance
        print(f"Overview: {overview}")
        print("-" * 80)

    cursor.close()
    connection.close()


if __name__ == "__main__":
    semantic_search()