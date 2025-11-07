# importar
import pandas as pd
import os

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

                    print(f"lido{parquet_path}")
                    
trasnform_silver()