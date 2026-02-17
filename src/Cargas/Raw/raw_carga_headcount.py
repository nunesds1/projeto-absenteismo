import pandas as pd
import glob
from pathlib import Path

# ============================
# Definição de caminhos
# ============================

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent

SOURCE_PATH = BASE_DIR / "banco_de_headcount" / "*.csv"
RAW_PATH = BASE_DIR / "data" / "raw" 

RAW_PATH.mkdir(parents=True, exist_ok=True)

# ============================
# Leitura de múltiplos arquivos
# ============================

files = glob.glob(str(SOURCE_PATH))

if not files:
    raise FileNotFoundError("Nenhum arquivo CSV encontrado na pasta banco_de_headcount.")

print(f"{len(files)} arquivos encontrados.")

df_headcount = pd.concat(
    [
        pd.read_csv(
            file,
            sep=";",
            encoding="utf-8",
        )
        for file in files
    ],
    ignore_index=True
)

print("Total de registros:", df_headcount.shape[0])
print("Total de colunas:", df_headcount.shape[1])

print("\nArquivos processados:")
for file in files:
    print("-", Path(file).name)

# ============================
# Salvando na RAW
# ============================

output_file = RAW_PATH / "raw_headcount.parquet"

df_headcount.to_parquet(output_file, index=False)

print("\nArquivo salvo em:", output_file)
