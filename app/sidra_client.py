import requests
from typing import Dict, List, Tuple, Optional, Any
from unidecode import unidecode

SIDRA_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados"
LOCALIDADES_API = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

DEFAULT_TIMEOUT = (10, 90)  # (connect, read)

def _get(url: str) -> Any:
    r = requests.get(url, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()

def _metadados(tabela: int) -> Dict[str, Any]:
    url = f"{SIDRA_BASE}/{tabela}/metadados"
    md = _get(url)
    if isinstance(md, list) and md:
        md = md[0]
    if not isinstance(md, dict):
        raise RuntimeError(f"Metadados em formato inesperado para tabela {tabela}.")
    return md

def list_variaveis_sidra(tabela: int) -> List[Dict[str, Any]]:
    md = _metadados(tabela)
    vars_ = md.get("variaveis", []) or []
    out: List[Dict[str, Any]] = []
    for v in vars_:
        if isinstance(v, dict):
            vid_raw = v.get("id") or v.get("codigo")
            try:
                vid = int(vid_raw) if vid_raw is not None else None
            except Exception:
                vid = None
            nome = v.get("nome") or v.get("descricao") or str(vid_raw) or "desconhecida"
            unidade_field = v.get("unidade")
            unidade = None
            if isinstance(unidade_field, dict):
                unidade = unidade_field.get("id") or unidade_field.get("nome")
            elif isinstance(unidade_field, str):
                unidade = unidade_field
            out.append({"id": vid, "nome": nome, "unidade": unidade})
        else:
            out.append({"id": None, "nome": str(v), "unidade": None})
    return out

def _find_variavel_id(tabela: int, prefer: List[str]) -> Tuple[int, Optional[str]]:
    vars_ = list_variaveis_sidra(tabela)
    for p in prefer:
        for v in vars_:
            nome = unidecode(str(v.get("nome", ""))).lower()
            if p.lower() in nome and v.get("id") is not None:
                return int(v["id"]), v.get("unidade")
    for v in vars_:
        if v.get("id") is not None:
            return int(v["id"]), v.get("unidade")
    raise RuntimeError(f"Nenhuma variável utilizável encontrada para tabela {tabela}.")

def list_produtos_sidra(tabela: int) -> List[Dict[str, Any]]:
    md = _metadados(tabela)
    classifs = md.get("classificacoes", []) or []
    alvo = None
    for c in classifs:
        nome = unidecode(str(c.get("nome", ""))).lower()
        if "produto" in nome:
            alvo = c
            break
    if not alvo:
        return []
    categorias = alvo.get("categorias", []) or []
    res: List[Dict[str, Any]] = []
    for cat in categorias:
        if isinstance(cat, dict):
            res.append({"id": str(cat.get("id")), "nome": cat.get("nome")})
        else:
            res.append({"id": None, "nome": str(cat)})
    return res

def resolve_municipio_codigos_ibge(pares_nome_uf: List[Tuple[str, str]]) -> Dict[Tuple[str, str], str]:
    if not pares_nome_uf:
        return {}
    all_mun = _get(LOCALIDADES_API)
    idx: Dict[Tuple[str, str], str] = {}
    if isinstance(all_mun, list):
        for m in all_mun:
            try:
                nome = unidecode(m["nome"]).lower()
                uf = m["microrregiao"]["mesorregiao"]["UF"]["sigla"]
                idx[(nome, uf)] = str(m["id"])
            except Exception:
                continue
    out: Dict[Tuple[str, str], str] = {}
    for nome, uf in pares_nome_uf:
        key = (unidecode(nome).lower(), uf.upper())
        if key in idx:
            out[key] = idx[key]
    return out

def _chunk(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]

def coletar_pam_por_municipios_produtos(
    tabela: int,
    ano: Optional[int],
    municipio_codigos: List[str],
    produtos: List[str],
) -> List[Dict[str, Any]]:
    variavel_id, _un = _find_variavel_id(
        tabela,
        prefer=[
            "quantidade produzida",
            "produção da aquicultura",
            "efetivo dos rebanhos",
        ],
    )
    md = _metadados(tabela)
    classifs = md.get("classificacoes", []) or []
    class_prod = None
    for c in classifs:
        if "produto" in unidecode(str(c.get("nome", ""))).lower():
            class_prod = c
            break
    if not class_prod:
        raise RuntimeError(f"Classificação 'Produto' não encontrada na tabela {tabela}.")
    class_id = class_prod.get("id")
    if class_id is None:
        raise RuntimeError(f"ID da classificação 'Produto' ausente na tabela {tabela}.")

    if ano is None:
        periods = _get(f"{SIDRA_BASE}/{tabela}/periodos")
        anos: List[int] = []
        if isinstance(periods, list):
            for p in periods:
                try:
                    anos.append(int(str(p)))
                except Exception:
                    pass
        if not anos:
            raise RuntimeError(f"Períodos não encontrados para tabela {tabela}.")
        ano = max(anos)

    muni_chunks = _chunk([str(x) for x in municipio_codigos], 70)
    prod_chunks = _chunk([str(p) for p in produtos], 30)

    out: List[Dict[str, Any]] = []
    for mchunk in muni_chunks:
        for pchunk in prod_chunks:
            url = (
                f"{SIDRA_BASE}/{tabela}/periodos/{ano}/variaveis/{variavel_id}"
                f"?localidades=N6[{','.join(mchunk)}]"
                f"&classificacao={class_id}[{','.join(pchunk)}]"
            )
            js = _get(url)
            if not js:
                continue
            node = js[0] if isinstance(js, list) and js else js
            unidade = None
            ufield = node.get("unidade") if isinstance(node, dict) else None
            if isinstance(ufield, dict):
                unidade = ufield.get("id") or ufield.get("nome")
            elif isinstance(ufield, str):
                unidade = ufield
            resultados = node.get("resultados", []) if isinstance(node, dict) else []
            for r in resultados:
                try:
                    cat = (r.get("classificacoes", [{}]) or [{}])[0].get("categoria", {}) or {}
                    prod_id = cat.get("id")
                    prod_nome = cat.get("nome")
                    series = r.get("series", []) or []
                    for s in series:
                        loc = s.get("localidade", {}) or {}
                        cod_mun = str(loc.get("id"))
                        nome_mun = loc.get("nome")
                        serie_vals = s.get("serie", {}) or {}
                        val = serie_vals.get(str(ano))
                        out.append(
                            {
                                "ano": int(ano),
                                "variavel": int(variavel_id),
                                "unidade": unidade,
                                "codigo_ibge": cod_mun,
                                "municipio": nome_mun,
                                "uf": "",
                                "cod_produto": str(prod_id) if prod_id is not None else None,
                                "produto": prod_nome,
                                "valor": None if val in (None, "", "...") else try_parse_number(val),
                            }
                        )
                except Exception:
                    continue
    return out

def try_parse_number(x: Any):
    if x is None:
        return None
    s = str(x).strip().replace(".", "").replace(",", ".")
    try:
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        return None
