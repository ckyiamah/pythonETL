# modules
from google.cloud import bigquery

client = bigquery.Client(project='loudyams')

sql = "select * from etl_dataset.movies_2005 limit 10"

query_job = client.query(sql)

results = query_job.result()

for r in results:
    print(r.year, r.title, r.genre, r.avg_vote)