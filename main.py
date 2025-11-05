import json
import requests
import os
from datetime import datetime
import time

API_URL = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data"

RAW_PATH = "dataset/raw"

os.makedirs(RAW_PATH, exist_ok=True)

def get_data():

    headers = {
    "Authorization": "Token 1705c07327a9f4e1d73d5bf2d02c75afc422e1ec"
    }
    page = 1
    total_data = []

    while page < 1001:
        print("Baixando página...")
        response = requests.get(API_URL, params={"page":page}, headers=headers)

        if response.status_code == 429:
            print(f"Muitas requisições, aguardando 30 segundos...")
            time.sleep(30)
            continue

        if response.status_code != 200:
            print(f"Erro{response.status_code}")
            break

        data = response.json()
        results = data.get("results", [])

        if not results:
            break

        filename = f"{RAW_PATH}/gastos_diretos_page_{page}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        total_data.extend(results)

        if not data.get("next"):
            break

        page += 1

        print(f"Download concluído. {len(total_data)} registros baixados.")

get_data()