import gzip
import random
from datetime import datetime, timedelta

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# Função para gerar um valor aleatório entre 1 e 10
def random_value():
    return random.randint(1, 10)


# Número de objetos no array Parquet
num_objects = 36525  # 36525 dias = 100 anos

# Data atual
current_date = datetime.now()

# Criar um DataFrame com os dados
data = {
    "data": [(current_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(num_objects)],
    "valor_a": [random_value() for _ in range(num_objects)],
    "valor_b": [random_value() for _ in range(num_objects)],
    "valor_c": [random_value() for _ in range(num_objects)],
    "valor_d": [random_value() for _ in range(num_objects)]
}

df = pd.DataFrame(data)

# Salvar o DataFrame em um arquivo Parquet
parquet_filename = "dados.parquet"
table = pa.Table.from_pandas(df)
pq.write_table(table, parquet_filename)

# Comprimir o arquivo Parquet em gzip
parquet_gz_filename = "dados.parquet.gz"
with open(parquet_filename, 'rb') as f_in:
    with gzip.open(parquet_gz_filename, 'wb') as f_out:
        f_out.writelines(f_in)

# Configuração do cliente S3 para o LocalStack
s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

# Nome do bucket e chave do objeto no LocalStack
bucket_name = 'my-bucket'
object_key = 'dados.parquet.gz'

# Fazer o upload do arquivo Parquet compactado para o bucket LocalStack
with open(parquet_gz_filename, 'rb') as fileobj:
    s3.upload_fileobj(fileobj, bucket_name, object_key)

print("Arquivo Parquet compactado em gzip enviado para o bucket: my-bucket")
