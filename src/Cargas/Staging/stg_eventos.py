import pandas as pd
from pathlib import Path

# ============================
# Caminhos
# ============================

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent

RAW_PATH = BASE_DIR / "data" / "raw" / "raw_eventos.parquet"
STG_PATH = BASE_DIR / "data" / "stg" 

STG_PATH.mkdir(parents=True, exist_ok=True)

# ============================
# Leitura RAW
# ============================

df_eventos = pd.read_parquet(RAW_PATH)

print("RAW carregado:", df_eventos.shape)

# ============================
# Regra 1
# ============================

# Deletando registros duplicados
df_eventos = df_eventos.drop_duplicates()

print("Retirando registros duplicados:", df_eventos.shape)

# ============================
# Etapa 2 - Garantir tipos
# ============================

# Deletando registros duplicados
df_eventos = df_eventos.drop_duplicates()

# Converter datas
df_eventos["data_evento"] = pd.to_datetime(df_eventos["data_evento"], dayfirst=True)

# Padronizar strings
df_eventos["tipo_evento"] = df_eventos["tipo_evento"].astype(str).str.strip()
df_eventos["status"] = df_eventos["status"].astype(str).str.strip()

# Preencher nulos
df_eventos["motivo"] = df_eventos["motivo"].fillna("Nao informado")

# Garantir tipo numérico
df_eventos["horas_evento"] = pd.to_numeric(df_eventos["horas_evento"], errors="coerce")


# ============================
# Salvando
# ============================

output_file = STG_PATH / "stg_eventos.parquet"

df_eventos.to_parquet(output_file, index=False)

print("Arquivo salvo em:", output_file)
