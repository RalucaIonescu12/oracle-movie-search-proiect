from pathlib import Path
import pandas as pd
from sentence_transformers import SentenceTransformer
import oracledb
from dotenv import load_dotenv
import os


BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = BASE_DIR / "data" / "imdb_top_1000.csv"
OUTPUT_CSV = BASE_DIR / "data" / "imdb_movies_clean.csv"

def main():
    df = pd.read_csv(INPUT_CSV)

    df = df[["Series_Title", "Genre", "Overview"]].copy()

    df.rename(columns={
        "Series_Title": "title",
        "Genre": "genre",
        "Overview": "overview"
    }, inplace=True)

    # drop valori lipsa
    df.dropna(subset=["title", "genre", "overview"], inplace=True)

    # clean spatii
    for col in ["title", "genre", "overview"]:
        df[col] = df[col].astype(str).str.strip()

    # df = df.head(300).copy()

    # ID
    df.insert(0, "movie_id", range(1, len(df) + 1))

    # textul folosit pentru semantic search
    df["search_text"] = (
        "Title: " + df["title"] +
        ". Genres: " + df["genre"] +
        ". Overview: " + df["overview"]
    )

    df = generate_embeddings(df)

    connect_to_db(df)

    # df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Saved cleaned dataset to: {OUTPUT_CSV}")
    print(f"Type: {type(df["embedding"][0])}")


def generate_embeddings(df):
    model = SentenceTransformer('all-MiniLM-L6-v2')

    df["embedding"] = df["search_text"].apply(lambda x: model.encode(x))

    return df

def connect_to_db(df):
    print("Connecting to DB")

    load_dotenv()

    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    service_name = os.getenv("DB_SERVICE")
    database_name = os.getenv("DB_NAME")

    dsn = oracledb.makedsn(host, int(port), service_name=service_name)

    try:
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        print("Conexiunea la Oracle a fost realizată cu succes!")

    except oracledb.DatabaseError as e:
        print("Eroare la conectarea la Oracle:", e)

    # finally:
    #     if 'connection' in locals():
    #         connection.close()

    cursor = connection.cursor()

    for idx, row in df.iterrows():
        try:
            embedding = row["embedding"].astype("float32")
            embedding_str = "[" + ", ".join(map(str, embedding)) + "]"

            cursor.execute("""
                INSERT INTO movies (movie_id, title, genre, overview, search_text, embedding)
                VALUES (:movie_id, :title, :genre, :overview, :search_text, :embedding)
            """, {
                "movie_id": row["movie_id"],
                "title": row["title"],
                "genre": row["genre"],
                "overview": row["overview"],
                "search_text": row["search_text"],
                "embedding": embedding_str
            })
        except oracledb.DatabaseError as e:
            error, = e.args
            print(f"Eroare la inserția rândului {idx} (movie_id={row['movie_id']}): {error.message}")
            return

    connection.commit()
    cursor.close()
    connection.close()
    print("Datele au fost inserate cu succes!")


def semantic_search():

    load_dotenv()

    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    service_name = os.getenv("DB_SERVICE")
    database_name = os.getenv("DB_NAME")

    # Construim DSN (Data Source Name)
    dsn = oracledb.makedsn(host, int(port), service_name=service_name)

    connection = oracledb.connect(
        user=username,
        password=password,
        dsn=dsn
    )

    cursor = connection.cursor()

    sql = """
    SELECT movie_id, title
    FROM movies
    ORDER BY VECTOR_DISTANCE(embedding, :query_vector, COSINE)
    FETCH FIRST 5 ROWS ONLY
    """

    model = SentenceTransformer('all-MiniLM-L6-v2')

    query_text = "dream"
    query_vector = model.encode(query_text).tolist()

    vector_str = "[" + ",".join(map(str, query_vector)) + "]"

    cursor.execute(sql, {"query_vector": vector_str})

    results = cursor.fetchall()

    for row in results:
        print(row)

    cursor.close()
    connection.close()


if __name__ == "__main__":
    # connect_to_db()
    # main()

    semantic_search()


