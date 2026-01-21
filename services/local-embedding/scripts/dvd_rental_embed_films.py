from sentence_transformers import SentenceTransformer
import psycopg2

# Load model locally (downloads once, then cached)
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

conn = psycopg2.connect(
    dbname="dvdrental",
    user="airflow",
    password="airflow",
    host="postgres",
    port=5432,
     options="-c search_path=public"
)

cur = conn.cursor()

cur.execute("""
    SELECT film_id, title || '. ' || description
    FROM film
    WHERE embedding IS NULL
""")

films = cur.fetchall()

for film_id, text in films:
    vector = model.encode(text).tolist()

    cur.execute(
        "UPDATE film SET embedding = %s WHERE film_id = %s",
        (vector, film_id)
    )

conn.commit()
cur.close()
conn.close()
print(f"Embedded and updated {len(films)} films.")