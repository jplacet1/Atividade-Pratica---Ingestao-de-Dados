# importar
import pandas as pd
import os
from functions import validate_all

BRONZE_PATH = "dataset/bronze"
SILVER_PATH = "dataset/silver"

def trasnform_silver():

# loop Listar pasta de anos 
    anos = os.listdir(BRONZE_PATH)
    for ano in anos:
        ano_path = os.path.join(BRONZE_PATH, ano)

# loop Listar pastas de meses 
        meses = os.listdir(ano_path)
        for mes in meses:
            mes_path = os.path.join(ano_path, mes)

# Transformação do arquivo
            for arquivo in os.listdir(mes_path):
                if arquivo.endswith(".parquet"):
                    parquet_path = os.path.join(mes_path, arquivo)
                    df = pd.read_parquet(parquet_path)
                    df = df.dropna(subset=["valor", "nome_favorecido"])
                    df = df.drop_duplicates()
                    validate_all(df)
                    silver_path = parquet_path.replace("bronze", "silver")
                    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
                    df.to_parquet(silver_path, index=False)
                    print(f"Arquivo salvo em {silver_path}")
                    os.remove(parquet_path)
                    
trasnform_silver()