import io
import pandas as pd

def gerar_planilha_filial(df: pd.DataFrame, filial: str, ano: int) -> io.BytesIO:
    buf = io.BytesIO()
    por_produto = (
        df.groupby(["tabela", "cod_produto", "produto"], dropna=False)["valor"]
        .sum(min_count=1).reset_index().sort_values(["tabela", "cod_produto"])
    )
    por_municipio = (
        df.groupby(["municipio", "cod_produto", "produto"], dropna=False)["valor"]
        .sum(min_count=1).reset_index().sort_values(["municipio", "cod_produto"])
    )
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="dados")
        por_produto.to_excel(w, index=False, sheet_name="por_produto")
        por_municipio.to_excel(w, index=False, sheet_name="por_municipio")
        pd.DataFrame([{"filial": filial, "ano": ano, "linhas": int(df.shape[0])}]).to_excel(w, index=False, sheet_name="sobre")
    buf.seek(0)
    return buf
