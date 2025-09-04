import os
from pathlib import Path
from collections import defaultdict, Counter

import pandas as pd
from sqlalchemy import text
from rapidfuzz import fuzz, process as rf_process

from db import get_engine, ensure_schema
from utils import normalize_name, http_get_json, try_float
from sidra_client import get_agregado_metadados, find_variavel_id, build_values_url

LOCALIDADES_BASE = "https://servicodados.ibge.gov.br/api/v1/localidades"
UFS_SUL = {"RS": 43, "SC": 42, "PR": 41}

TARGETS = {
    "vegetal": [
        "milho", "soja", "trigo", "feijao", "arroz",
        "fumo", "tabaco", "melancia", "cana", "erva mate",
        "pessego", "tomate", "cebola", "uva",
    ],
    "rebanho": [
        "bovinos", "bubalinos", "caprinos", "ovinos",
        "suinos", "equinos", "galinaceos",
    ],
    "aquicultura": ["tilapia"],
}

TABLES = {
    "vegetal": {"table_id": 1612, "variavel_like": "quantidade produzida"},        # PAM
    "rebanho": {"table_id": 3939, "variavel_like": "efetivo dos rebanhos"},        # PPM
    "aquicultura": {"table_id": 3946, "variavel_like": "produção da aquicultura"}, # PPM
}

# ---------------- infra básica ----------------

def ensure_all(engine=None):
    engine = engine or get_engine()
    ensure_schema(engine)

def get_engine():
    from db import get_engine as ge
    return ge()

# ---------------- entrada: municípios por filial ----------------

def load_municipios_filiais_from_excel(xlsx_path: str) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx_path)
    df = xl.parse(xl.sheet_names[0])
    cols = {c.strip().lower(): c for c in df.columns}

    def pick(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    c_filial = pick("filial")
    c_mun = pick("municipio", "município", "municípios", "municipios", "nome_municipio")
    c_uf = pick("uf", "estado")
    if not (c_filial and c_mun):
        raise ValueError("Planilha precisa ter colunas 'Filial' e 'Município'.")

    out = pd.DataFrame(
        {
            "filial": df[c_filial].astype(str).str.strip(),
            "nome_municipio": df[c_mun].astype(str).str.strip(),
        }
    )
    out["uf"] = df[c_uf].astype(str).str.strip().str.upper() if c_uf else None

    if out["uf"] is not None and out["uf"].notna().any():
        out = out[out["uf"].isin(["RS", "SC", "PR"]) | out["uf"].isna()].copy()

    out["nome_normalizado"] = out["nome_municipio"].map(lambda x: normalize_name(str(x)))
    out.drop_duplicates(subset=["filial", "nome_municipio"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out

def upsert_municipios_filiais(df: pd.DataFrame, engine=None):
    engine = engine or get_engine()
    with engine.begin() as conn:
        for row in df.to_dict("records"):
            conn.execute(
                text(
                    """
                    INSERT INTO public.municipios_filiais
                        (filial, nome_municipio, uf, codigo_ibge, nome_normalizado)
                    VALUES (:filial, :nome_municipio, :uf, NULL, :nome_normalizado)
                    ON CONFLICT (filial, nome_municipio) DO UPDATE SET
                        uf = EXCLUDED.uf,
                        nome_normalizado = EXCLUDED.nome_normalizado
                    """
                ),
                row,
            )

def fetch_municipios_ibge_rs_sc_pr() -> pd.DataFrame:
    out = []
    for uf_sigla, uf_id in UFS_SUL.items():
        url = f"{LOCALIDADES_BASE}/estados/{uf_id}/municipios"
        js = http_get_json(url)
        for m in js:
            out.append(
                {
                    "uf": uf_sigla,
                    "codigo_ibge": int(m["id"]),
                    "nome_municipio": m["nome"],
                    "nome_normalizado": normalize_name(m["nome"]),
                }
            )
    return pd.DataFrame(out)

def match_cods_ibge(engine=None, score_threshold: int = 88):
    engine = engine or get_engine()
    df_ibge = fetch_municipios_ibge_rs_sc_pr()
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT id, filial, nome_municipio, COALESCE(uf,''), nome_normalizado "
                "FROM public.municipios_filiais"
            )
        ).fetchall()

    updates = []
    for rid, filial, nome_municipio, uf, nome_norm in rows:
        pool = df_ibge[df_ibge["uf"] == uf] if uf else df_ibge
        if pool.empty:
            continue
        choices = pool["nome_normalizado"].tolist()
        match = rf_process.extractOne(nome_norm, choices, scorer=fuzz.WRatio)
        if match:
            _, score, idx = match
            if score >= score_threshold:
                row = pool.iloc[idx]
                updates.append((rid, int(row["codigo_ibge"]), row["uf"]))

    if not updates:
        return 0

    with engine.begin() as conn:
        for rid, cod, uf in updates:
            conn.execute(
                text("UPDATE public.municipios_filiais SET codigo_ibge=:c, uf=:u WHERE id=:r"),
                {"c": cod, "u": uf, "r": rid},
            )
    return len(updates)

# ---------------- coleta SIDRA ----------------

def _chunk(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def _upsert_sidra_rows(engine, recs):
    if not recs:
        return 0
    with engine.begin() as conn:
        for r in recs:
            conn.execute(
                text(
                    """
                    INSERT INTO public.dados_sidra_brutos
                        (tabela, variavel, ano, cod_municipio, nome_municipio, uf,
                         produto_codigo, produto_nome, unidade, valor_str, valor_num, origem)
                    VALUES
                        (:tabela, :variavel, :ano, :cod_municipio, :nome_municipio, :uf,
                         :produto_codigo, :produto_nome, :unidade, :valor_str, :valor_num, 'SIDRA')
                    ON CONFLICT (tabela, variavel, ano, cod_municipio, produto_codigo)
                    DO UPDATE SET valor_str = EXCLUDED.valor_str, valor_num = EXCLUDED.valor_num
                    """
                ),
                r,
            )
    return len(recs)

def _pick_targets_in_class(meta_class, group_name: str):
    targets_norm = [normalize_name(t) for t in TARGETS[group_name]]
    sel = {}
    for cl in meta_class:
        cl_name = normalize_name(cl.get("nome") or "")
        if not any(x in cl_name for x in ["produto", "rebanho", "aquicultura"]):
            continue
        class_id = int(cl["id"])
        keep = {}
        for cat in cl.get("categorias", []):
            cid = int(cat["id"])
            cname = normalize_name(cat.get("nome") or "")
            if any(t in cname for t in targets_norm):
                keep[cid] = cat.get("nome") or ""
        if keep:
            sel[class_id] = keep
    return sel

def collect_sidra_for_group(group_name: str, engine=None, verbose: bool = True) -> int:
    engine = engine or get_engine()
    table_id = int(TABLES[group_name]["table_id"])
    meta = get_agregado_metadados(table_id)

    var_id = (
        find_variavel_id(meta, TABLES[group_name]["variavel_like"])
        or find_variavel_id(meta, "quantidade produzida")
        or find_variavel_id(meta, "efetivo")
        or find_variavel_id(meta, "produção")
    )
    if not var_id:
        if verbose:
            print(f"[{group_name}] Variável não encontrada — ignorando grupo.")
        return 0

    class_matches = _pick_targets_in_class(meta.get("classificacoes", []), group_name)
    if not class_matches:
        if verbose:
            print(f"[{group_name}] Nenhuma categoria alvo encontrada na tabela {table_id}.")
        return 0

    with engine.begin() as conn:
        munis = conn.execute(
            text(
                "SELECT DISTINCT codigo_ibge, nome_municipio, uf "
                "FROM public.municipios_filiais "
                "WHERE codigo_ibge IS NOT NULL "
                "ORDER BY nome_municipio"
            )
        ).fetchall()
    if not munis:
        if verbose:
            print(f"[{group_name}] Nenhum município com código IBGE.")
        return 0

    muni_codes = [int(x[0]) for x in munis]
    muni_chunks = _chunk(muni_codes, 30)
    total = 0

    for class_id, cats in class_matches.items():
        cat_ids = list(cats.keys())

        for mchunk in muni_chunks:
            mset = set(mchunk)
            for pchunk in _chunk(cat_ids, 8):
                pset = set(pchunk)
                url = build_values_url(table_id, var_id, "n6", mchunk, class_id, pchunk, periodo="last")
                try:
                    js = http_get_json(url)
                except Exception as e:
                    if verbose:
                        print(f"[{group_name}] Falha HTTP em {url}: {e}")
                    continue

                if not isinstance(js, list) or len(js) <= 1:
                    continue

                header = js[0]
                unidade = header.get("Unidade", "")

                for row in js[1:]:
                    try:
                        cat_id = None
                        for k, v in row.items():
                            if k.endswith("C") and str(v).isdigit():
                                v_int = int(v)
                                if v_int in pset:
                                    cat_id = v_int
                                    break
                        if not cat_id:
                            continue

                        cod_mun = None
                        for k, v in row.items():
                            if k.endswith("C") and str(v).isdigit():
                                v_int = int(v)
                                if v_int in mset:
                                    cod_mun = v_int
                                    break
                        if not cod_mun:
                            continue

                        ano_val = None
                        if "Ano" in row and str(row["Ano"]).strip():
                            ano_val = int(str(row["Ano"])[:4])
                        elif "Mês" in row and str(row["Mês"]).strip():
                            ano_val = int(str(row["Mês"])[:4])
                        else:
                            for v in row.values():
                                s = str(v)
                                if len(s) >= 4 and s[:4].isdigit():
                                    y = int(s[:4])
                                    if 1900 <= y <= 2100:
                                        ano_val = y
                                        break
                        if not ano_val:
                            continue

                        nome_mun = row.get("D3N") or row.get("Município") or ""

                        val = row.get("V")
                        val_num = try_float(val)

                        rec = {
                            "tabela": table_id,
                            "variavel": int(var_id),
                            "ano": int(ano_val),
                            "cod_municipio": int(cod_mun),
                            "nome_municipio": nome_mun,
                            "uf": None,
                            "produto_codigo": int(cat_id),
                            "produto_nome": cats.get(cat_id, ""),
                            "unidade": unidade,
                            "valor_str": str(val) if val is not None else None,
                            "valor_num": val_num,
                        }
                        total += _upsert_sidra_rows(engine, [rec])
                    except Exception:
                        continue

    return total

# ---------------- exportações e auditoria ----------------

def get_last_year(engine=None):
    engine = engine or get_engine()
    with engine.begin() as conn:
        y = conn.execute(text("SELECT MAX(ano) FROM public.dados_sidra_brutos")).scalar_one()
    return int(y) if y else None

def export_excel_por_filial(dest_path: str, engine=None):
    engine = engine or get_engine()
    with engine.begin() as conn:
        ano = conn.execute(text("SELECT MAX(ano) FROM public.dados_sidra_brutos")).scalar_one()
        if not ano:
            raise RuntimeError("Sem dados para exportar. Rode a coleta.")
        munis = pd.read_sql(
            text(
                "SELECT filial, nome_municipio, uf, codigo_ibge "
                "FROM public.municipios_filiais "
                "WHERE codigo_ibge IS NOT NULL "
                "ORDER BY filial, nome_municipio"
            ),
            conn,
        )
        dados = pd.read_sql(
            text(
                "SELECT ano, cod_municipio, nome_municipio, uf, produto_codigo, produto_nome, valor_num "
                "FROM public.dados_sidra_brutos "
                "WHERE ano = :ano"
            ),
            conn,
            params={"ano": int(ano)},
        )

    pivot = dados.pivot_table(
        index=["cod_municipio", "nome_municipio", "uf"],
        columns="produto_nome",
        values="valor_num",
        aggfunc="first",
    ).reset_index()

    with pd.ExcelWriter(dest_path, engine="openpyxl") as writer:
        for filial, g in munis.groupby("filial"):
            munis_da_filial = g[["codigo_ibge", "nome_municipio", "uf"]].drop_duplicates()
            merged = munis_da_filial.merge(
                pivot,
                left_on=["codigo_ibge", "nome_municipio", "uf"],
                right_on=["cod_municipio", "nome_municipio", "uf"],
                how="left",
            ).drop(columns=["cod_municipio"])
            merged.sort_values(by=["nome_municipio"], inplace=True)
            merged.to_excel(writer, sheet_name=str(filial)[:31], index=False)

    return int(ano)

def list_produtos(engine=None, ano: int | None = None):
    engine = engine or get_engine()
    ano = ano or get_last_year(engine)
    if not ano:
        return []
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT produto_nome FROM public.dados_sidra_brutos WHERE ano=:y ORDER BY produto_nome"),
            {"y": int(ano)},
        ).fetchall()
    return [r[0] for r in rows]

def _df_municipios_filiais(engine):
    with engine.begin() as conn:
        df = pd.read_sql(
            text("SELECT filial, nome_municipio, uf, codigo_ibge FROM public.municipios_filiais"),
            conn,
        )
    return df

def get_codigos_duplicados(engine=None):
    engine = engine or get_engine()
    df = _df_municipios_filiais(engine)
    df["codigo_ibge"] = pd.to_numeric(df["codigo_ibge"], errors="coerce").astype("Int64")
    df_dup = df.dropna(subset=["codigo_ibge"]).groupby("codigo_ibge").agg({
        "filial": lambda s: sorted(set(v for v in s if pd.notna(v))),
        "nome_municipio": lambda s: sorted(set(v for v in s if pd.notna(v))),
        "uf": lambda s: sorted(set(v for v in s if pd.notna(v))),
        "codigo_ibge": "size",
    }).rename(columns={"codigo_ibge": "ocorrencias"}).reset_index()
    df_dup = df_dup[df_dup["ocorrencias"] > 1].sort_values(by="ocorrencias", ascending=False)
    items = []
    for _, row in df_dup.iterrows():
        items.append({
            "codigo_ibge": int(row["codigo_ibge"]),
            "filiais": row["filial"],
            "nomes": row["nome_municipio"],
            "ufs": row["uf"],
            "ocorrencias": int(row["ocorrencias"]),
        })
    return items

def build_lookup_files(out_dir: str):
    """
    Gera:
      - codigo_municipio_lookup.xlsx   (lookup completo)
      - codigo_municipio_duplicados.xlsx (apenas duplicados)
      - codigo_municipio_lookup_simples.csv (codigo, nome_preferido, uf)
    """
    engine = get_engine()
    df = _df_municipios_filiais(engine)
    df["codigo_ibge"] = pd.to_numeric(df["codigo_ibge"], errors="coerce").astype("Int64")

    # lookup completo
    g = df.groupby("codigo_ibge", dropna=False)
    def agg_list(s):
        return sorted(set(str(v) for v in s.dropna().astype(str).tolist()))
    lookup = g.agg({
        "nome_municipio": agg_list,
        "uf": agg_list,
        "filial": agg_list,
    }).reset_index()

    # nome preferido = mais frequente
    pref = (
        df.dropna(subset=["codigo_ibge"])
          .groupby(["codigo_ibge", "nome_municipio"])
          .size().reset_index(name="freq")
          .sort_values(["codigo_ibge", "freq"], ascending=[True, False])
          .drop_duplicates(subset=["codigo_ibge"])
          .rename(columns={"nome_municipio": "nome_preferido"})
          [["codigo_ibge", "nome_preferido"]]
    )
    lookup = lookup.merge(pref, on="codigo_ibge", how="left")

    lookup["qtd_nomes_distintos"] = lookup["nome_municipio"].apply(len)
    lookup["qtd_ufs_distintas"] = lookup["uf"].apply(len)
    lookup["qtd_filiais"] = lookup["filial"].apply(len)
    lookup["duplicado_entre_filiais"] = lookup["qtd_filiais"] > 1
    lookup["conflito_de_nome"] = lookup["qtd_nomes_distintos"] > 1
    lookup = lookup.sort_values(["duplicado_entre_filiais", "qtd_filiais", "codigo_ibge"], ascending=[False, False, True])

    dups = lookup[lookup["duplicado_entre_filiais"]].copy()

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    xlsx_lookup = out_dir / "codigo_municipio_lookup.xlsx"
    xlsx_dups = out_dir / "codigo_municipio_duplicados.xlsx"
    csv_simple = out_dir / "codigo_municipio_lookup_simples.csv"

    with pd.ExcelWriter(xlsx_lookup, engine="openpyxl") as w:
        lookup.to_excel(w, index=False, sheet_name="lookup")
    with pd.ExcelWriter(xlsx_dups, engine="openpyxl") as w:
        dups.to_excel(w, index=False, sheet_name="duplicados")

    # CSV simples
    def first_or_blank(lst):
        return lst[0] if isinstance(lst, list) and len(lst) else ""
    simple = lookup[["codigo_ibge", "nome_preferido", "uf"]].copy()
    simple["uf_preferida"] = simple["uf"].apply(first_or_blank)
    simple = simple.drop(columns=["uf"]).rename(columns={"uf_preferida": "uf"})
    simple.to_csv(csv_simple, index=False, encoding="utf-8-sig")

    return {
        "xlsx_lookup": str(xlsx_lookup),
        "xlsx_duplicados": str(xlsx_dups),
        "csv_simples": str(csv_simple),
        "qtd_duplicados": int(len(dups)),
        "qtd_codigos": int(len(lookup)),
    }

def get_status(engine=None):
    engine = engine or get_engine()
    with engine.begin() as conn:
        total_munis = conn.execute(text("SELECT COUNT(*) FROM public.municipios_filiais")).scalar_one()
        com_codigo = conn.execute(text("SELECT COUNT(*) FROM public.municipios_filiais WHERE codigo_ibge IS NOT NULL")).scalar_one()
        ano = conn.execute(text("SELECT MAX(ano) FROM public.dados_sidra_brutos")).scalar_one()
        by_table = []
        if ano:
            for k, v in TABLES.items():
                cnt = conn.execute(
                    text("SELECT COUNT(*) FROM public.dados_sidra_brutos WHERE tabela=:t AND ano=:y"),
                    {"t": int(v["table_id"]), "y": int(ano)},
                ).scalar_one()
                by_table.append({"grupo": k, "tabela": v["table_id"], "linhas_ano": int(cnt)})
    dups = get_codigos_duplicados(engine)
    return {
        "municipios_total": int(total_munis),
        "municipios_com_codigo": int(com_codigo),
        "municipios_sem_codigo": int(total_munis - com_codigo),
        "duplicados_entre_filiais": len(dups),
        "ultimo_ano": int(ano) if ano else None,
        "linhas_por_grupo_no_ano": by_table,
    }

# ---------------- orquestração ----------------

def bootstrap_all(data_dir: str, groups: list[str] | None = None):
    engine = get_engine()
    ensure_all(engine)

    # arquivo de entrada
    candidates = [
        os.path.join(data_dir, "municipio_por_filial.xlsx"),
        os.path.join(data_dir, "municipios_por_filial.xlsx"),
    ]
    muni_xlsx = next((p for p in candidates if os.path.exists(p)), None)
    if not muni_xlsx:
        raise FileNotFoundError(f"Arquivo não encontrado: {candidates[0]} (ou {candidates[1]})")

    df = load_municipios_filiais_from_excel(muni_xlsx)
    upsert_municipios_filiais(df, engine)
    n = match_cods_ibge(engine)
    print(f"[IBGE códigos] Atualizados: {n} municípios")

    # grupos (parametrizável)
    groups_to_run = groups or ["vegetal", "rebanho", "aquicultura"]
    total = 0
    for grp in groups_to_run:
        if grp not in TABLES:
            print(f"[WARN] Grupo desconhecido: {grp} — ignorando")
            continue
        try:
            up = collect_sidra_for_group(grp, engine, verbose=True)
            print(f"[SIDRA] grupo={grp} upserts={up}")
            total += up
        except Exception as e:
            print(f"[WARN] Falha ao coletar grupo {grp}: {e} (seguindo)")
    return total
# --- [REFRESH MATERIALIZED VIEWS] -------------------------------------------
from db import get_engine

def refresh_materialized_views(concurrently: bool = False) -> list[str]:
    """
    Atualiza as materialized views usadas no BI.
    Se concurrently=True, tenta usar REFRESH CONCURRENTLY (requer índice único).
    Retorna a lista de MVs atualizadas.
    """
    mv_names = ["public.mv_fato_ultimo_ano"]
    clause = "CONCURRENTLY " if concurrently else ""
    stmt_tpl = f"REFRESH MATERIALIZED VIEW {clause}{{mv}};"

    eng = get_engine()
    with eng.begin() as conn:
        for mv in mv_names:
            conn.exec_driver_sql(stmt_tpl.format(mv=mv))
    return mv_names

# --- [REFRESH MATERIALIZED VIEWS] -------------------------------------------

from db import get_engine

def refresh_materialized_views(concurrently: bool = False) -> list[str]:
    """
    Atualiza as materialized views usadas no BI.
    Se concurrently=True, tenta usar REFRESH CONCURRENTLY (requer índice único).
    Retorna a lista de MVs atualizadas.
    """
    mv_names = ["public.mv_fato_ultimo_ano"]
    clause = "CONCURRENTLY " if concurrently else ""
    stmt_tpl = f"REFRESH MATERIALIZED VIEW {clause}{{mv}};"

    eng = get_engine()
    with eng.begin() as conn:
        for mv in mv_names:
            conn.exec_driver_sql(stmt_tpl.format(mv=mv))
    return mv_names
