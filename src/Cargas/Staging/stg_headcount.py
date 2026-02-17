import pandas as pd
from pathlib import Path

# ============================
# Caminhos
# ============================

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent

RAW_PATH = BASE_DIR / "data" / "raw" / "raw_headcount.parquet"
STG_PATH = BASE_DIR / "data" / "stg" 

STG_PATH.mkdir(parents=True, exist_ok=True)

# ============================
# Leitura RAW
# ============================

df_headcount = pd.read_parquet(RAW_PATH)

print("RAW carregado:", df_headcount.shape)

# ============================
# Etapa 1 - Priorizar ativos
# ============================

df_headcount = (
    df_headcount
    .sort_values(
        by=["id_funcionario", "ano_mes", "ativo_no_mes"],
        ascending=[True, True, False]
    )
    .drop_duplicates(
        subset=["id_funcionario", "ano_mes"],
        keep="first"
    )
)

print("Após remoção de duplicidade:", df_headcount.shape)

# ============================
# Etapa 2 - Manter apenas ativos
# ============================

# df_headcount = df_headcount[df_headcount["ativo_no_mes"] == 1]

# print("Após filtro ativos:", df_headcount.shape)

# ============================
# Validação de integridade
# ============================

check = df_headcount.groupby(["id_funcionario", "ano_mes"]).size().value_counts()
print("Validação chave:")
print(check)

# ============================
# Salvando
# ============================

output_file = STG_PATH / "stg_headcount.parquet"

df_headcount.to_parquet(output_file, index=False)

print("Arquivo salvo em:", output_file)
