import pandas as pd
from pathlib import Path

# ============================
# Base dir (funciona em .py e notebook)
# ============================
try:
    BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
except NameError:
    BASE_DIR = Path.cwd()

GOLD_PATH = BASE_DIR / "data" / "gold"
GOLD_PATH.mkdir(parents=True, exist_ok=True)

# ============================
# Parâmetros
# ============================
DATA_INICIO = "2025-01-01"
DATA_FIM = "2025-12-31"

# ============================
# Geração do calendário diário
# ============================
datas = pd.date_range(start=DATA_INICIO, end=DATA_FIM, freq="D")

df_tempo = pd.DataFrame({"data": datas})

# ============================
# Atributos principais
# ============================
df_tempo["ano"] = df_tempo["data"].dt.year
df_tempo["mes"] = df_tempo["data"].dt.month
df_tempo["dia"] = df_tempo["data"].dt.day
df_tempo["ano_mes"] = df_tempo["data"].dt.strftime("%Y-%m")
df_tempo["trimestre"] = df_tempo["data"].dt.quarter
df_tempo["semana_ano_iso"] = df_tempo["data"].dt.isocalendar().week.astype(int)  # ISO week
df_tempo["dia_semana_num"] = df_tempo["data"].dt.weekday + 1  # 1=Seg ... 7=Dom

# Nomes em PT-BR (sem depender de locale do SO)
dias_pt = {1: "Segunda", 2: "Terça", 3: "Quarta", 4: "Quinta", 5: "Sexta", 6: "Sábado", 7: "Domingo"}
meses_pt = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

df_tempo["dia_semana_nome"] = df_tempo["dia_semana_num"].map(dias_pt)
df_tempo["mes_nome"] = df_tempo["mes"].map(meses_pt)

# Flags úteis para BI
df_tempo["is_fim_de_semana"] = df_tempo["dia_semana_num"].isin([6, 7]).astype(int)

# Chave útil para relacionamentos no BI (uma coluna, estável)
df_tempo["id_data"] = df_tempo["data"].dt.strftime("%Y%m%d").astype(int)

# Ordenar colunas (boa prática)
df_tempo = df_tempo[
    [
        "id_data",
        "data",
        "ano",
        "mes",
        "mes_nome",
        "dia",
        "ano_mes",
        "trimestre",
        "semana_ano_iso",
        "dia_semana_num",
        "dia_semana_nome",
        "is_fim_de_semana",
    ]
]

print("Dim tempo criada:", df_tempo.shape)
print(df_tempo.head())

# ============================
# Salvar
# ============================
out = GOLD_PATH / "gold_dim_tempo.parquet"
df_tempo.to_parquet(out, index=False)
print("Salvo em:", out)
