import os
from app.logic import bootstrap_all, export_excel_por_filial
from app.db import get_engine

if __name__ == "__main__":
    data_dir = os.getenv("DATA_DIR", "./data")
    print(f"[RUN] DATA_DIR={data_dir}")
    up = bootstrap_all(data_dir)
    print(f"[RUN] upserts={up}")
    engine = get_engine()
    out = os.path.join(data_dir, "relatorio_filiais.xlsx")
    ano = export_excel_por_filial(out, engine)
    print(f"[RUN] Gerado: {out} (ano {ano})")
