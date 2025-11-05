# Atividade-Pratica---Ingestao-de-Dados

O projeto consiste em duas funções principais:

  1. Obter os dados via API da Brasil IO e guardar na pasta raw (main.py)
  2. Converter para Parquet e organizar por ano e mês, guardando na pasta bronze (convert_to_parquet.py)

No arquivo main.py, a função get_data() tem um loop para obter as 1000 páginas. Dentro,  a API é chamada, passando a URL e os parâmetros page e header, respectivamente para paginação e autenticação.
Cada arquivo baixado contém cerca de 1000 registros, cada um é uma página da API transformada em JSON.

No convert_to_parquet.py os arquivos são listados e ordenados, para que não haja sobreposição, lê cada JSON como dataframe agrupa os dados por ano e mês e salva cada grupo como um arquivo Parquet em pastas organizadas por ano/mês.
