import os
import pandas as pd

RAW_PATH = "dataset/raw"
BRONZE_PATH = "dataset/bronze"

os.makedirs(BRONZE_PATH, exist_ok=True)

json_files = sorted(
    [f for f in os.listdir(RAW_PATH) if f.endswith(".json")],
    key=lambda x: int(x.split("_page_")[1].split(".")[0])
)

for json_file in json_files:
    json_path = os.path.join(RAW_PATH, json_file)
    df = pd.read_json(json_path)

    if "ano" not in df.columns or "mes" not in df.columns:
        print(f"⚠️ {json_file} ignorado (faltam colunas 'ano' e 'mes')")
        continue

    for (ano, mes), grupo in df.groupby(["ano", "mes"]):
        pasta_destino = os.path.join(BRONZE_PATH, f"ano={ano}", f"mes={mes}")
        os.makedirs(pasta_destino, exist_ok=True)
        parquet_name = f"{json_file.replace('.json', '')}_ano{ano}_mes{mes}.parquet"
        parquet_path = os.path.join(pasta_destino, parquet_name)
        grupo.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
        print(f"✅ {json_file} -> salvo em {parquet_path}")

    os.remove(json_path)

print("\n Todos os JSONs foram processados e movidos corretamente para o formato Parquet!")

