import pandas as pd
from pathlib import Path

# ============================
# Base dir (funciona em .py e notebook)
# ============================
try:
    BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
except NameError:
    BASE_DIR = Path.cwd()

HEADCOUNT_PATH = BASE_DIR / "data" / "stg" / "stg_headcount.parquet"
COMP_PATH = BASE_DIR / "bancos_complementares" / "base_inf_complementar_funcionarios.csv"

GOLD_PATH = BASE_DIR / "data" / "gold"
GOLD_PATH.mkdir(parents=True, exist_ok=True)

# ============================
# Leitura
# ============================
df_hc = pd.read_parquet(HEADCOUNT_PATH)
print("Headcount STG:", df_hc.shape)

# ============================
# Seleção de colunas do headcount
# ============================
cols_hc = [
    "id_funcionario",
    "ano_mes",
    "uf",
    "unidade",
    "area",
    "cargo",
    "regime",
    "turno",
    "escala",
    "dias_uteis_mes",
    "base_horas_mes",
]

missing = [c for c in cols_hc if c not in df_hc.columns]
if missing:
    raise ValueError(f"Colunas ausentes no stg_headcount: {missing}")

df_dim = df_hc[cols_hc].copy()

# ============================
# Garantir 1 linha por funcionario/mês
# ============================
dup = df_dim.duplicated(subset=["id_funcionario", "ano_mes"]).sum()
if dup > 0:
    # Regra de segurança: manter o registro com maior base_horas_mes
    df_dim = (
        df_dim.sort_values(["id_funcionario", "ano_mes", "base_horas_mes"], ascending=[True, True, False])
              .drop_duplicates(subset=["id_funcionario", "ano_mes"], keep="first")
    )
    print(f"⚠️ Encontradas {dup} duplicidades; aplicamos regra de dedup (maior base_horas_mes).")

# ============================
# Criar chave funcionario_mes
# ============================

df_dim["chave_func_mes"] = (
    df_dim["id_funcionario"].astype(str) + "_" +
    df_dim["ano_mes"].astype(str)
)

# ============================
# Enriquecimento com base complementar
# ============================
cols_comp = [
    "id_funcionario",
    "faixa_etaria",
    "escolaridade",
    "tempo_empresa_anos",
    "tipo_contrato",
    "modalidade_trabalho",
    "cidade",
    "pcd",
    "sexo",
]

if not COMP_PATH.exists():
    raise FileNotFoundError(f"Arquivo não encontrado: {COMP_PATH}")

df_comp = pd.read_csv(COMP_PATH, encoding="utf-8", sep=";")

# Limpar espaços em branco dos nomes das colunas
df_comp.columns = df_comp.columns.str.strip()

# Verificar colunas antes do merge
missing_comp = [c for c in cols_comp if c not in df_comp.columns]
if missing_comp:
    raise ValueError(f"Colunas ausentes na base complementar: {missing_comp}")

df_comp = df_comp[cols_comp].drop_duplicates(subset=["id_funcionario"])

# LEFT JOIN: mantém todos do headcount
df_dim = df_dim.merge(df_comp, on="id_funcionario", how="left")

# ============================
# Auditorias rápidas
# ============================
print("Gold dim funcionario (func x mês):", df_dim.shape)
print("Chave única (id_funcionario, ano_mes):", df_dim[["id_funcionario", "ano_mes"]].drop_duplicates().shape[0])

# quantos sem enriquecimento?
enriq_nulls = df_dim["faixa_etaria"].isna().sum()
if enriq_nulls > 0:
    print(f"⚠️ {enriq_nulls} linhas sem match na base complementar (faixa_etaria nula).")

# Ajustando ordem de colunas (chave_func_mes primeiro)
cols_order = ["chave_func_mes"] + [c for c in df_dim.columns if c != "chave_func_mes"]
df_dim = df_dim[cols_order]

# ============================
# Salvar
# ============================
out = GOLD_PATH / "gold_dim_funcionario_mes.parquet"
df_dim.to_parquet(out, index=False)
print("Salvo em:", out)
