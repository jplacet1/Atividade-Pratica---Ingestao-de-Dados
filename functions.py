import unidecode
import pandas as pd
def validate_text(df):
    text_cols = [
        "nome_acao", "nome_elemento_despesa", "nome_favorecido", "nome_funcao",
        "nome_grupo_despesa", "nome_orgao", "nome_orgao_superior",
        "nome_programa", "nome_subfuncao", "nome_unidade_gestora", "linguagem_cidada"
    ]

    for col in text_cols:
        df[col] = (df[col]
                .astype(str)
                .str.strip()
                .apply(unidecode.unidecode)  # remove acentos
                .str.title())
        

def validate_codes(df):
    code_cols = [
        "codigo_acao", "codigo_elemento_despesa", "codigo_favorecido",
        "codigo_funcao", "codigo_grupo_despesa", "codigo_orgao",
        "codigo_orgao_superior", "codigo_programa", "codigo_subfuncao",
        "codigo_unidade_gestora", "gestao_pagamento", "numero_documento"
    ]

    for col in code_cols:
        df[col] = df[col].astype(str).str.strip()


def validate_date(df):
    df["data_pagamento"] = pd.to_datetime(df["data_pagamento"], errors="coerce")
    df["data_pagamento_original"] = pd.to_datetime(df["data_pagamento_original"], errors="coerce")

def validate_values(df):
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

def validate_int(df):
    df["ano"] = df["ano"].astype(int)
    df["mes"] = df["mes"].astype(int)

def validate_all(df):
    validate_values(df)
    validate_codes(df)
    validate_date(df)
    validate_int(df)
    validate_values(df)
    validate_text(df)

