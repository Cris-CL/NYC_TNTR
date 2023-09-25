from google.cloud import bigquery

def get_token(project_id,dataset,table_name):

    query = f"SELECT MAX(token) FROM `{project_id}.{dataset}.{table_name}`"

    bq_client = bigquery.Client()
    query_job = bq_client.query(query)
    query_job = query_job.result()

    res_tok = list(query_job)
    last_token = res_tok[0][0]


    return last_token
