"""
docker run --rm -it --mount type=bind,source="/home/veera/code/data-eng/services/local-embedding/scripts",target="/scripts" --network data-engine-network  local-embedding:latest
"""

from sentence_transformers import SentenceTransformer
import psycopg2

model = SentenceTransformer("all-MiniLM-L6-v2")

conn = psycopg2.connect(
    dbname="dvdrental",
    user="airflow", 
    password="airflow",
    host="postgres", 
    port=5432
)

cur = conn.cursor()

query = """
    SELECT 
        title,
        (embedding <-> %s::vector) AS distance
    FROM film
    ORDER BY embedding <-> %s::vector
    LIMIT 5
    """ 
query_vector = model.encode("funny family movie with kids").tolist() 
cur.execute(query, (query_vector, query_vector))
for row in cur.fetchall(): 
    print(row)