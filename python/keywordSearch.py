from pathlib import Path
import os
from dotenv import load_dotenv
import oracledb


class KeywordMovieSearch:
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent.parent
        load_dotenv(self.base_dir / ".env")

        self.username = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.service_name = os.getenv("DB_SERVICE")

        if not all([self.username, self.password, self.host, self.port, self.service_name]):
            raise ValueError("Missing DB settings in .env")

        self.dsn = f"{self.host}:{self.port}/{self.service_name}"

    def get_connection(self):
        return oracledb.connect(
            user=self.username,
            password=self.password,
            dsn=self.dsn
        )

    def search(self, query_text, top_k=5):
        connection = self.get_connection()
        cursor = connection.cursor()

        sql = f"""
        SELECT movie_id,
               title,
               overview,
               SCORE(1) AS keyword_score
        FROM movies
        WHERE CONTAINS(search_text, :query_text, 1) > 0
        ORDER BY keyword_score DESC
        FETCH FIRST {top_k} ROWS ONLY
        """

        cursor.execute(sql, {"query_text": query_text})
        results = cursor.fetchall()

        if not results:
            print("Niciun rezultat.")
        else:
            print(f"\n Rezultate pentru: {query_text}\n")
            for row in results:
                movie_id, title, overview_lob, keyword_score = row
                overview = overview_lob.read() if overview_lob else None

                print(f"ID: {movie_id}")
                print(f"Title: {title}")
                print(f"Keyword score: {keyword_score}")
                print(f"Overview: {overview}")
                print("-" * 100)

        cursor.close()
        connection.close()


if __name__ == "__main__":
    searcher = KeywordMovieSearch()

    query_text = input("Query: ").strip()

    if not query_text:
        print("Input gol")
    else:
        searcher.search(query_text, top_k=10)