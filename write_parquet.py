import random
from datetime import datetime, timedelta
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

print("Arquivo Parquet criado com sucesso: dados.parquet")
