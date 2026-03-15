from pathlib import Path
import pandas as pd

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

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Saved cleaned dataset to: {OUTPUT_CSV}")
    print(f"Rows: {len(df)}")

if __name__ == "__main__":
    main()