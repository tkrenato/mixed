## VTEX to BigQuery - List Orders API
#
# Este script tem o seguinte fluxo:
# List Orders API VTEX(json) ==> Pandas(df) ==> Google Cloud Storage (csv) ==> BigQuery(table)

import pandas as pd
import requests
import json
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import gcsfs

# Vtex
vtex_domain = 'https://replaceme.vtexcommercestable.com.br'
vtex_api = 'api/oms/pvt/orders'

# calcula o período para extracao das ordens
# pega a data atual
fim = date.today()
# pega a data de 30 dias atras 
inicio = fim - timedelta(days=30)
# variavel vtex_creationDate para compor a URL
#vtex_creationDate = f"f_creationDate=creationDate:[{inicio}T02:00:00.000Z TO {fim}T01:59:59.999Z]"
vtex_creationDate = f"f_creationDate=creationDate:[2022-11-01T02:00:00.000Z TO 2022-11-02T01:59:59.999Z]&per_page=100"
# URL utilizada na chamada REST para API da VTEX
url = f"{vtex_domain}/{vtex_api}?{vtex_creationDate}"


#  Configurações comuns do Google Cloud
project_id = "project_id"
table_id = "vtex.vtex-orders"
bucket_name = "vtex_bucket_example"
filename = "orders_tmp.csv"
bq = bigquery.Client() #inicializa o client do BigQuery
job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True)
uri = f"gs://{bucket_name}/{filename}" #arquivo temporario armazenado no Google Cloud Storage


# Pega os valores da AppKey e AppToken do Google Cloud Secrets Manager para não precisar expor
# os valores aqui no script
secret_AppKey = 'AppKey'
secret_AppToken = 'AppToken'
secret_client = secretmanager.SecretManagerServiceClient()
name1 = f"projects/{project_id}/secrets/{secret_AppKey}/versions/latest"
name2 = f"projects/{project_id}/secrets/{secret_AppToken}/versions/latest"
secret1 = secret_client.access_secret_version(request={"name": name1})
secret2 = secret_client.access_secret_version(request={"name": name2})
appkey_value = secret1.payload.data.decode("UTF-8")
apptoken_value = secret2.payload.data.decode("UTF-8")

# pega os valores da AppKey e AppToken e joga no cabecalho http para enviar na chamada post
headers = {
"accept": "application/json",
"X-VTEX-API-AppKey": appkey_value,
"X-VTEX-API-AppToken": apptoken_value
}

pages_response = requests.get(url, headers=headers).json()
total_pages = (pages_response['paging']['pages'])
if total_pages <= 30:
    total_pages = total_pages
else:
    total_pages = 30

all_data_list = []
x = 0
for x in range(1, total_pages+1):
    url_for = f"{url}&page={x}"

    dataset_all = requests.get(url, headers)
    dataset_all_json = dataset_all.json()
    response_for = requests.get(url_for, headers=headers).json()
    number_of_entries = len(response_for['list'])

    for i in range(0, number_of_entries):
        all_data_list.append(response_for['list'][i])

df = pd.DataFrame(all_data_list).reset_index(drop = True)

df.to_csv(f"gs://{bucket_name}/{filename}")

load_job = bq.load_table_from_uri(uri, table_id, job_config=job_config)
load_job.result()


