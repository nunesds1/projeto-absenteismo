import pandas as pd
from pathlib import Path

# ============================
# Caminhos
# ============================

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent

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
# 1️⃣ Garantir funcionários ativos
# ============================

df_eventos["chave_func_mes"] = (
    df_eventos["id_funcionario"].astype(str) + "_" +
    df_eventos["ano_mes"].astype(str)
)

# df_headcount = df_headcount[df_headcount["ativo_no_mes"] == 1]

# Manter apenas chave necessária para validação
# df_headcount_chave = df_headcount[["id_funcionario", "ano_mes"]].drop_duplicates()

# ============================
# 2️⃣ Filtrar apenas eventos aprovados
# ============================

df_eventos = df_eventos[df_eventos["status"] == "Aprovado"]

# ============================
# 3️⃣ Garantir que horas_evento esteja em horas (float)
# ============================

df_eventos["horas_evento"] = pd.to_numeric(
    df_eventos["horas_evento"],
    errors="coerce"
)

print("Eventos aprovado:", df_eventos.shape)

# ============================
# 4️⃣ Preparar headcount com ativo_no_mes
# ============================

df_headcount_validacao = df_headcount[
    ["id_funcionario", "ano_mes", "ativo_no_mes", "base_horas_mes"]
].drop_duplicates()

print("Headcount validado:", df_headcount_validacao.shape)

# Unificando eventos com headcount para validar se o funcionário estava ativo no mês do evento
df_gold = df_eventos.merge(
    df_headcount_validacao,
    on=["id_funcionario", "ano_mes"],
    how="left"
)

# Se não encontrou no headcount, considerar como 0
df_gold["ativo_no_mes"] = df_gold["ativo_no_mes"].fillna(0).astype(int)

print("Após merge:", df_gold.shape)

# Adicionando colunas de tempo em minutos

df_gold["horas_evento"] = pd.to_numeric(df_gold["horas_evento"], errors="coerce")
df_gold["base_horas_mes"] = pd.to_numeric(df_gold["base_horas_mes"], errors="coerce")

df_gold["horas_evento_min"] = (
    df_gold["horas_evento"] * 60
).round().astype("Int64")

df_gold["base_horas_mes_min"] = (
    df_gold["base_horas_mes"] * 60
).round().astype("Int64")

# ============================
# 5️⃣ Selecionar colunas finais
# ============================

df_gold = df_gold[
    [
        "chave_func_mes",
        "id_funcionario",
        "data_evento",
        "ano_mes",
        "tipo_evento",
        "horas_evento",
        "motivo",
        "cid",
        "origem",
        "status",
        "ativo_no_mes",
        "base_horas_mes",
        "horas_evento_min",
        "base_horas_mes_min"
    ]
]

# ============================
# Salvando GOLD
# ============================

output_file = GOLD_PATH / "gold_fato_absenteismo.parquet"

df_gold.to_parquet(output_file, index=False)

print("Gold fato salva com sucesso:", output_file)
