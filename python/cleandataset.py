from pathlib import Path
import pandas as pd
from sentence_transformers import SentenceTransformer
import oracledb
from dotenv import load_dotenv
import os


BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = BASE_DIR / "data" / "imdb_top_1000.csv"


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


def load_and_prepare_dataset():
    df = pd.read_csv(INPUT_CSV)

    df = df[["Series_Title", "Genre", "Overview"]].copy()

    df.rename(columns={
        "Series_Title": "title",
        "Genre": "genre",
        "Overview": "overview"
    }, inplace=True)

    df.dropna(subset=["title", "genre", "overview"], inplace=True)

    for col in ["title", "genre", "overview"]:
        df[col] = df[col].astype(str).str.strip()

    df.insert(0, "movie_id", range(1, len(df) + 1))

    df["search_text"] = (
        "Title: " + df["title"] +
        ". Genres: " + df["genre"] +
        ". Overview: " + df["overview"]
    )

    return df


def generate_embeddings(df):
    model = SentenceTransformer("all-MiniLM-L6-v2")
    df["embedding"] = df["search_text"].apply(lambda x: model.encode(x))
    return df


def insert_movies_into_db(df):
    connection = get_connection()
    cursor = connection.cursor()

    for idx, row in df.iterrows():
        try:
            embedding = row["embedding"].astype("float32")
            embedding_str = "[" + ", ".join(map(str, embedding)) + "]"

            cursor.execute("""
                INSERT INTO movies (movie_id, title, genre, overview, search_text, embedding)
                VALUES (:movie_id, :title, :genre, :overview, :search_text, :embedding)
            """, {
                "movie_id": int(row["movie_id"]),
                "title": row["title"],
                "genre": row["genre"],
                "overview": row["overview"],
                "search_text": row["search_text"],
                "embedding": embedding_str
            })

        except oracledb.DatabaseError as e:
            error, = e.args
            print(f"Eroare la inserția rândului {idx} (movie_id={row['movie_id']}): {error.message}")
            connection.rollback()
            cursor.close()
            connection.close()
            return

    connection.commit()
    cursor.close()
    connection.close()
    print("Datele au fost inserate cu succes.")


def main():
    df = load_and_prepare_dataset()

    df = generate_embeddings(df)

    insert_movies_into_db(df)



if __name__ == "__main__":
    main()