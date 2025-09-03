# app/main.py
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse
import pandas as pd
import tempfile, os, shutil, re

# ajuste este import para o seu módulo real:
from app.reporting import montar_relatorio_df  # <- troque se o caminho for diferente

app = FastAPI()

def _slug(s: str) -> str:
    s = re.sub(r"[^0-9A-Za-z]+", "_", s).strip("_")
    return re.sub(r"_+", "_", s)

def _nome_arquivo(filial: str, ano: int, fmt: str) -> str:
    return f"relatorio_{_slug(filial)}_{ano}.{fmt}"

@app.get("/relatorio/{filial}")
def baixar_relatorio(
    background: BackgroundTasks,
    filial: str,
    ano: int = Query(..., ge=1900, le=2100, description="Ano do relatório"),
    formato: str = Query("xlsx", pattern="^(xlsx|html)$", description="xlsx ou html"),
    gerar_vazio: bool = Query(False, description="Gera arquivo mesmo sem dados")
):
    # 1) Busca os dados do relatório
    try:
        df = montar_relatorio_df(filial, ano)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao montar relatório: {e}")

    # 2) Se vazio e não quiser gerar, devolve 204
    if (df is None or df.empty) and not gerar_vazio:
        raise HTTPException(status_code=204, detail="Sem dados")

    # 3) Se vazio mas gerar_vazio=True, cria um DF “amigável”
    if df is None or df.empty:
        df = pd.DataFrame([{
            "filial": filial,
            "ano": ano,
            "mensagem": "Sem dados para os parâmetros informados."
        }])

    # 4) Gera arquivo em diretório temporário
    tmpdir = tempfile.mkdtemp(prefix="rel_")
    fmt = "xlsx" if formato == "xlsx" else "html"
    fpath = os.path.join(tmpdir, _nome_arquivo(filial, ano, fmt))

    try:
        if fmt == "xlsx":
            with pd.ExcelWriter(fpath, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name="dados")
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            df.to_html(fpath, index=False)
            media_type = "text/html; charset=utf-8"
    except Exception as e:
        shutil.rmtree(tmpdir, True)
        raise HTTPException(status_code=500, detail=f"Erro ao gerar arquivo: {e}")

    # 5) Limpa o tmp após enviar
    background.add_task(shutil.rmtree, tmpdir, True)

    # 6) Envia o arquivo (Content-Length/Disposition corretos)
    return FileResponse(
        fpath,
        media_type=media_type,
        filename=os.path.basename(fpath),
        # headers={"Cache-Control": "no-store"}  # opcional
    )

@app.get("/healthz")
def healthz():
    return {"status": "ok"}