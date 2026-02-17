import pandas as pd
from pathlib import Path

try:
    BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
except NameError:
    BASE_DIR = Path.cwd()

CID_PATH = BASE_DIR / "bancos_complementares" / "base_cid.csv"
GOLD_PATH = BASE_DIR / "data" / "gold"

GOLD_PATH.mkdir(parents=True, exist_ok=True)

# ============================
# Leitura
# ============================

df_cid = pd.read_csv(
    CID_PATH,
    sep=";",
    encoding="utf-8",
)

print("Base CID:", df_cid.shape)

# ============================
# Padronizar nomes das colunas
# ============================

df_cid.columns = (
    df_cid.columns
    .str.strip()
    .str.lower()
)

# ============================
# Validar schema
# ============================

expected_cols = ["cid", "grupo_cid", "descricao_grupo"]

missing = set(expected_cols) - set(df_cid.columns)

if missing:
    raise ValueError(f"Colunas obrigatórias ausentes na base CID: {missing}")

# ============================
# Limpeza
# ============================

df_cid["cid"] = df_cid["cid"].str.strip().str.upper()

df_cid = df_cid.drop_duplicates(subset="cid")

# ============================
# Criar registro UNKNOWN
# (Boa prática de DW)
# ============================

unknown_row = pd.DataFrame({
    "cid": ["UNKNOWN"],
    "grupo_cid": ["UNKNOWN"],
    "descricao_grupo": ["Não informado"]
})

df_cid = pd.concat([df_cid, unknown_row], ignore_index=True)

print("Dim CID final:", df_cid.shape)

# ============================
# Salvar
# ============================

output = GOLD_PATH / "gold_dim_cid.parquet"

df_cid.to_parquet(output, index=False)

print("gold_dim_cid criada com sucesso.")
