import pandas as pd
from pathlib import Path

# ============================
# Caminhos
# ============================

BASE_DIR = Path(__file__).resolve().parent.parent

HEADCOUNT_PATH = BASE_DIR / "data" / "stg" / "stg_headcount.parquet"
EVENTOS_PATH = BASE_DIR / "data" / "stg" / "stg_eventos.parquet"

GOLD_PATH = BASE_DIR / "data" / "gold"
GOLD_PATH.mkdir(parents=True, exist_ok=True)

# ============================
# Leitura
# ============================

df_headcount = pd.read_parquet(HEADCOUNT_PATH)
df_eventos = pd.read_parquet(EVENTOS_PATH)

print("Headcount:", df_headcount.shape)
print("Eventos:", df_eventos.shape)

# ============================
# 1️⃣ Filtrar apenas eventos aprovados
# ============================

df_eventos = df_eventos[df_eventos["status"] == "Aprovado"]

# Garantir tipo numérico
df_eventos["horas_evento"] = pd.to_numeric(
    df_eventos["horas_evento"],
    errors="coerce"
)

# ============================
# 2️⃣ Preparar headcount com ativo_no_mes
# ============================

df_headcount_validacao = df_headcount[
    ["id_funcionario", "ano_mes", "ativo_no_mes"]
].drop_duplicates()

# ============================
# 3️⃣ Merge LEFT para auditoria
# ============================

df_gold = df_eventos.merge(
    df_headcount_validacao,
    on=["id_funcionario", "ano_mes"],
    how="left"
)

# Se não encontrou no headcount, considerar como 0
df_gold["ativo_no_mes"] = df_gold["ativo_no_mes"].fillna(0).astype(int)

print("Após merge:", df_gold.shape)

# ============================
# 4️⃣ Selecionar campos finais (todos eventos + ativo)
# ============================

print(df_gold["ativo_no_mes"].value_counts())

# ============================
# Salvando
# ============================

output_file = GOLD_PATH / "gold_fato_absenteismo_auditoria.csv"

df_gold.to_csv(output_file, index=False)

print("Gold auditoria salva com sucesso.")
