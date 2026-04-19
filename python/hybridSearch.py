from pathlib import Path
import os
from dotenv import load_dotenv
import oracledb
from sentence_transformers import SentenceTransformer


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


def hybrid_search(top_k=5, candidate_k=20):
    # query-ul pentru partea de keyword search
    keyword_query = input("Keyword query (Oracle Text syntax): ").strip()

    if not keyword_query:
        print("Keyword query cannot be empty.")
        return

    # query-ul pentru partea de semantic search
    semantic_query = input(
        "Semantic query (press Enter to reuse keyword query): "
    ).strip()

    # Daca nu se introduce semantic query, se foloseste acelasi text ca si pentru keyword query
    if not semantic_query:
        semantic_query = keyword_query

    # Incarca modelul SentenceTransformer folosit pentru a transforma textul in vector
    model = SentenceTransformer("all-MiniLM-L6-v2")

    # transf query-ul semantic intr un embedding numeric
    query_vector = model.encode(semantic_query).tolist()

    vector_str = "[" + ",".join(map(str, query_vector)) + "]"

    connection = get_connection()
    cursor = connection.cursor()

    # Query hibrid:
    # 1. kw = top rezultate keyword cu Oracle Text
    # 2. vec = top rezultate semantice cu VECTOR_DISTANCE
    # 3. fused = combina cele doua liste si calculeaza un scor final
    sql = f"""
    WITH kw AS (
        SELECT movie_id,
               title,
               overview,
               SCORE(1) AS kw_score,
               ROW_NUMBER() OVER (ORDER BY SCORE(1) DESC) AS kw_rank
        FROM movies
        WHERE CONTAINS(search_text, :keyword_query, 1) > 0
        FETCH FIRST {candidate_k} ROWS ONLY
    ),
    vec AS (
        SELECT movie_id,
               title,
               overview,
               VECTOR_DISTANCE(embedding, :query_vector, COSINE) AS vec_distance,
               ROW_NUMBER() OVER (
                   ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE)
               ) AS vec_rank
        FROM movies
        FETCH FIRST {candidate_k} ROWS ONLY
    ),
    fused AS (
        SELECT
            COALESCE(kw.movie_id, vec.movie_id) AS movie_id,
            COALESCE(kw.title, vec.title) AS title,
            COALESCE(kw.overview, vec.overview) AS overview,
            kw.kw_score,
            kw.kw_rank,
            vec.vec_distance,
            vec.vec_rank,
            NVL(1.0 / (60 + kw.kw_rank), 0) +
            NVL(1.0 / (60 + vec.vec_rank), 0) AS hybrid_score
        FROM kw
        FULL OUTER JOIN vec
            ON kw.movie_id = vec.movie_id
    )
    SELECT movie_id,
           title,
           overview,
           kw_score,
           kw_rank,
           vec_distance,
           vec_rank,
           hybrid_score
    FROM fused
    ORDER BY hybrid_score DESC
    FETCH FIRST {top_k} ROWS ONLY
    """

    cursor.execute(sql, {
        "keyword_query": keyword_query,
        "query_vector": vector_str
    })

    results = cursor.fetchall()

    print("\n" + "=" * 110)
    print(f"KEYWORD QUERY : {keyword_query}")
    print(f"SEMANTIC QUERY: {semantic_query}")
    print("=" * 110 + "\n")

    if not results:
        print("No hybrid results found.")
    else:
        for row in results:
            (
                movie_id,
                title,
                overview_lob,
                kw_score,
                kw_rank,
                vec_distance,
                vec_rank,
                hybrid_score
            ) = row

            # overview este de tip LOB in Oracle, deci trebuie citit explicit cu .read()
            overview = overview_lob.read() if overview_lob else None
            if overview:
                overview = overview.replace("\r", " ").replace("\n", " ")

            print(f"ID: {movie_id}")
            print(f"Title: {title}")
            print(f"Keyword score : {kw_score}")
            print(f"Keyword rank  : {kw_rank}")
            print(f"Vector dist   : {vec_distance}")
            print(f"Vector rank   : {vec_rank}")
            print(f"Hybrid score  : {hybrid_score}")
            print(f"Overview      : {overview}")
            print("-" * 110)

    cursor.close()
    connection.close()


if __name__ == "__main__":
    # Punctul de intrare al scriptului
    # Ruleaza cautarea hibrida cu:
    # - top_k=5       -> cate rezultate finale sa afiseze
    # - candidate_k=20 -> cate candidate sa ia initial din fiecare lista
    hybrid_search(top_k=5, candidate_k=20)