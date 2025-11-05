import os
import pandas as pd

RAW_PATH = "dataset/raw" 
BRONZE_PATH = "dataset/bronze"

os.makedirs(BRONZE_PATH, exist_ok=True)

json_files = [f for f in os.listdir(RAW_PATH) if f.endswith(".json")]

for json_file in json_files:
    json_path = os.path.join(RAW_PATH, json_file)
    df = pd.read_json(json_path)

    parquet_path = BRONZE_PATH

    df.to_parquet(
        parquet_path,
        engine='pyarrow',
        index=False,
        partition_cols=['ano', 'mes'],
        compression='snappy'
    )

    print(f"{json_file} processado e salvo em Parquet")

    os.remove(f"{RAW_PATH}/{json_file}")
