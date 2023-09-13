import json
import urllib.parse
import boto3
import gzip
import time
import ujson

print('Loading function')

s3 = boto3.client('s3', config=boto3.session.Config(s3={'use_accelerate_endpoint': True}))


def lambda_handler(event, context):
    
    start_time = time.time()
    data_inicio = event.get('data_inicio')
    data_fim = event.get('data_fim')
    
    try:
        response = s3.get_object(Bucket=event.get("bucket_name"), Key=event.get("object_key"))
        # Obtém o tamanho do arquivo em bytes
        file_size_bytes = response["ContentLength"]
        elapsed_time = time.time() - start_time
        print(f"Tempo até aqui 1: {elapsed_time:.2f} segundos")
    
        # Converte o tamanho para megabytes (MB)
        file_size_mb = file_size_bytes / (1024 * 1024) 
        print(f"Tamanho do arquivo: {file_size_mb:.2f} MB")

        # Lê e descompacta o conteúdo do arquivo .gz
        with gzip.open(response["Body"], "rb") as gzipped_file:
            gziped = gzipped_file.read()
            elapsed_time = time.time() - start_time
            print(f"Tempo até aqui 2.1: {elapsed_time:.2f} segundos")
            data = json.loads(gziped)
        elapsed_time = time.time() - start_time
        print(f"Tempo até aqui 2.2: {elapsed_time:.2f} segundos")
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    print("Data " + str(len(data)))
    # Realiza a pesquisa binária para encontrar os limites do intervalo de datas
    #lower_index = binary_search_lower_limit(data, 'data', data_inicio)
    #upper_index = binary_search_upper_limit(data, 'data', data_fim)
    # Filtra os objetos dentro do intervalo
    #filtered_data = data[lower_index:upper_index+1]
    
    
    filtered_data = []
    # Itera pelos objetos e verifica se estão dentro do intervalo de datas
    for obj in data:
        data_atualizacao = obj.get('data')
        if data_inicio <= data_atualizacao <= data_fim:
            filtered_data.append(obj)
    elapsed_time = time.time() - start_time
    print(f"Tempo até aqui 3: {elapsed_time:.2f} segundos")
    body = json.dumps(filtered_data)
    elapsed_time = time.time() - start_time
    print(f"Tempo até aqui 4: {elapsed_time:.2f} segundos")
    return {
        'statusCode': 200,
        'body': filtered_data
    }
    
def binary_search_lower_limit(data, key, target_date):
    left, right = 0, len(data) - 1
    while left <= right:
        mid = (left + right) // 2
        if data[mid][key] < target_date:
            left = mid + 1
        else:
            right = mid - 1
    return left

def binary_search_upper_limit(data, key, target_date):
    left, right = 0, len(data) - 1
    while left <= right:
        mid = (left + right) // 2
        if data[mid][key] <= target_date:
            left = mid + 1
        else:
            right = mid - 1
    return right