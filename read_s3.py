import boto3
import gzip
import pyarrow.parquet as pq
import time

# Configuração do cliente S3 para o LocalStack
s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

# Nome do bucket e chave do objeto no LocalStack
bucket_name = 'my-bucket'
object_key = 'dados.parquet.gz'

# Nome do arquivo descompactado
output_filename = "dados.parquet"

try:
    response = s3.get_object(Bucket=bucket_name, Key=object_key)

    # Lê e descompacta o conteúdo do arquivo .gz
    with gzip.open(response["Body"], "rb") as gzipped_file:
        gziped = gzipped_file.read()

    # Ler o arquivo Parquet descompactado
    table = pq.read_table(gziped)

    # Converter a tabela Parquet em um DataFrame Pandas (opcional)
    df = table.to_pandas()

    # Imprimir os dados (ou realizar outras operações)
    print(df.head())

except Exception as e:
    print(f"Erro ao ler o arquivo do S3: {str(e)}")
