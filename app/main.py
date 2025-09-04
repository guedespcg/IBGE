import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.responses import FileResponse, HTMLResponse
from logic import refresh_materialized_views

app = FastAPI(title="AFUBRA IBGE/SIDRA Automation", version="1.1.0")

def L():
    # Import tardio para evitar dependência cíclica
    import importlib
    return importlib.import_module("logic")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/init")
def init():
    try:
        logic = L()
        logic.ensure_all(logic.get_engine())
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/bootstrap")
def bootstrap(
    req: dict | None = Body(None),
    groups: Optional[str] = Query(None, description="Ex.: vegetal,rebanho (aquicultura opcional)"),
):
    data_dir = (req or {}).get("data_dir") or os.getenv("DATA_DIR") or "/data"
    groups_list = [g.strip() for g in groups.split(",")] if groups else None
    try:
        logic = L()
        up = logic.bootstrap_all(data_dir, groups=groups_list)
        return {"ok": True, "upserts": up}
    except FileNotFoundError as fe:
        raise HTTPException(status_code=404, detail=str(fe))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/bootstrap")
def bootstrap_get(
    data_dir: Optional[str] = Query(None),
    groups: Optional[str] = Query(None, description="Ex.: vegetal,rebanho (aquicultura opcional)"),
):
    data_dir = data_dir or os.getenv("DATA_DIR") or "/data"
    groups_list = [g.strip() for g in groups.split(",")] if groups else None
    try:
        logic = L()
        up = logic.bootstrap_all(data_dir, groups=groups_list)
        return {"ok": True, "upserts": up}
    except FileNotFoundError as fe:
        raise HTTPException(status_code=404, detail=str(fe))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status")
def status():
    try:
        logic = L()
        return logic.get_status(logic.get_engine())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/auditoria/duplicados")
def auditoria_duplicados():
    try:
        logic = L()
        return {"items": logic.get_codigos_duplicados(logic.get_engine())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/auditoria/lookup.xlsx")
def auditoria_lookup_xlsx():
    try:
        logic = L()
        tmp_paths = logic.build_lookup_files("/app")
        return FileResponse(
            tmp_paths["xlsx_lookup"],
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename="codigo_municipio_lookup.xlsx",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/produtos")
def produtos(ano: Optional[int] = Query(None)):
    try:
        logic = L()
        return {"ano": ano or logic.get_last_year(logic.get_engine()), "produtos": logic.list_produtos(logic.get_engine(), ano)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/relatorio/x.xlsx")
def relatorio_xlsx():
    try:
        logic = L()
        tmp_path = "/app/_relatorio_filiais.xlsx"
        ano = logic.export_excel_por_filial(tmp_path, logic.get_engine())
        filename = f"relatorio_filiais_{ano}.xlsx"
        return FileResponse(
            tmp_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=filename,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
from fastapi import HTTPException

@app.post("/db/refresh-mv")
def api_refresh_mv(concurrently: bool = False):
    """
    Atualiza as MVs (por padrão sem bloquear leitura).
    Use ?concurrently=true para tentar REFRESH CONCURRENTLY (requer índice único).
    """
    try:
        mv_list = refresh_materialized_views(concurrently=concurrently)
        return {"ok": True, "refreshed": mv_list, "concurrently": concurrently}
    except Exception as e:
        # devolve erro legível no JSON
        raise HTTPException(status_code=500, detail=f"Falha ao atualizar MV: {e}")

@app.get("/")
def root():
    return HTMLResponse("""
    <html><head><title>AFUBRA IBGE/SIDRA</title></head>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
      <h2>AFUBRA IBGE/SIDRA — API</h2>
      <ul>
        <li>GET <code>/health</code></li>
        <li>POST <code>/init</code></li>
        <li>POST <code>/bootstrap</code> — body opcional: {"data_dir": "/data"}; query opcional: <code>?groups=vegetal,rebanho</code></li>
        <li>GET <code>/bootstrap?data_dir=/data&groups=vegetal,rebanho</code></li>
        <li>GET <code>/status</code> — contagens, ano mais recente, linhas por grupo</li>
        <li>GET <code>/auditoria/duplicados</code> — códigos IBGE presentes em mais de uma filial</li>
        <li>GET <code>/auditoria/lookup.xlsx</code> — arquivo para conferência/substituição</li>
        <li>GET <code>/produtos</code> — lista de produtos no último ano</li>
        <li>GET <code>/relatorio/x.xlsx</code> — planilha final (abas por filial)</li>
      </ul>
    </body></html>
    """)
