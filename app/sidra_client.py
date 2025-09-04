from urllib.parse import quote
from utils import http_get_json

BASE = "https://servicodados.ibge.gov.br/api/v3"

def get_agregado_metadados(agregado_id: int):
    """Metadados do agregado (variáveis, classificações, etc.)."""
    url = f"{BASE}/agregados/{agregado_id}/metadados"
    return http_get_json(url)

def find_variavel_id(meta: dict, like: str | None):
    """Procura variável cujo nome contém o texto informado (case-insensitive)."""
    if not meta or not like:
        return None
    like_norm = like.lower()
    for v in meta.get("variaveis", []):
        nome = (v.get("nome") or "").lower()
        if like_norm in nome:
            try:
                return int(v["id"])
            except Exception:
                pass
    # fallback: primeira variável
    try:
        return int(meta.get("variaveis", [])[0]["id"])
    except Exception:
        return None

def build_values_url(
    agregado_id: int,
    variavel_id: int,
    localidade_nivel: str,
    codigos_localidade: list[int],
    class_id: int | None = None,
    cat_ids: list[int] | None = None,
    periodo: str = "last",
) -> str:
    """
    Monta URL da API v3 /values:
      /agregados/{ag}/periodos/{periodo}/variaveis/{var}?localidades=N6[...]
      [&classificacao={class_id}[cat,cat,...]]
    """
    n = localidade_nivel.upper()
    loc = f"{n}[{','.join(str(x) for x in codigos_localidade)}]"
    base = f"{BASE}/agregados/{agregado_id}/periodos/{quote(str(periodo))}/variaveis/{variavel_id}"
    params = [("localidades", loc)]
    if class_id and cat_ids:
        klass = f"{class_id}[{','.join(str(x) for x in cat_ids)}]"
        params.append(("classificacao", klass))
    # serializa preservando [] e vírgulas
    qs = "&".join(f"{k}={quote(v, safe='[] ,')}" for k, v in params)
    return f"{base}?{qs}"
