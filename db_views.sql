-- ============================================================
-- AFUBRA IBGE/SIDRA - Camada de Consumo e Desempenho (Postgres)
-- Cria índices, views e materialized view para BI.
-- Idempotente: pode rodar quantas vezes quiser.
-- ============================================================

-- =========================
-- ÍNDICES (desempenho)
-- =========================
CREATE INDEX IF NOT EXISTS idx_mf_codigo_ibge
  ON public.municipios_filiais (codigo_ibge);

CREATE INDEX IF NOT EXISTS idx_mf_filial
  ON public.municipios_filiais (filial);

CREATE INDEX IF NOT EXISTS idx_sidra_year_table_muni_prod
  ON public.dados_sidra_brutos (ano, tabela, cod_municipio, produto_codigo);

CREATE INDEX IF NOT EXISTS idx_sidra_produto_nome
  ON public.dados_sidra_brutos (produto_nome);

CREATE INDEX IF NOT EXISTS idx_sidra_cod_municipio
  ON public.dados_sidra_brutos (cod_municipio);

-- =========================
-- VIEW: Fato "com duplicidade" (uso dentro da filial)
--   Observação: se filtrar múltiplas filiais juntas, pode haver double-count.
-- =========================
CREATE OR REPLACE VIEW public.vw_fato_filial_produto_anual AS
SELECT
  f.filial,
  d.uf,
  d.cod_municipio,
  COALESCE(d.nome_municipio, f.nome_municipio) AS nome_municipio,
  d.ano,
  CASE d.tabela
    WHEN 1612 THEN 'vegetal'
    WHEN 3939 THEN 'rebanho'
    WHEN 3946 THEN 'aquicultura'
    ELSE 'desconhecido'
  END AS grupo,
  d.produto_codigo,
  d.produto_nome,
  d.unidade,
  d.valor_num
FROM public.dados_sidra_brutos d
JOIN public.municipios_filiais f
  ON f.codigo_ibge = d.cod_municipio;

COMMENT ON VIEW public.vw_fato_filial_produto_anual IS
'Fato por filial/município/produto/ano. Útil para análises por uma filial.
Se agrupar múltiplas filiais, municípios compartilhados podem gerar dupla contagem.';

-- Índices de suporte para a view (em cima das tabelas já indexadas):
-- (nada adicional necessário aqui)

-- =========================
-- VIEW: Fato "exclusivo" (sem double-count entre filiais)
--   Regra de desempate: primeira filial por ordem alfabética para cada município.
--   Útil para comparativos entre filiais.
-- =========================
CREATE OR REPLACE VIEW public.vw_fato_filial_exclusiva AS
WITH base AS (
  SELECT
    codigo_ibge,
    filial,
    ROW_NUMBER() OVER (PARTITION BY codigo_ibge ORDER BY filial ASC) AS rn
  FROM public.municipios_filiais
),
exclusiva AS (
  SELECT codigo_ibge, filial
  FROM base
  WHERE rn = 1
)
SELECT
  e.filial,
  d.uf,
  d.cod_municipio,
  COALESCE(d.nome_municipio, mf.nome_municipio) AS nome_municipio,
  d.ano,
  CASE d.tabela
    WHEN 1612 THEN 'vegetal'
    WHEN 3939 THEN 'rebanho'
    WHEN 3946 THEN 'aquicultura'
    ELSE 'desconhecido'
  END AS grupo,
  d.produto_codigo,
  d.produto_nome,
  d.unidade,
  d.valor_num
FROM public.dados_sidra_brutos d
JOIN exclusiva e
  ON e.codigo_ibge = d.cod_municipio
LEFT JOIN public.municipios_filiais mf
  ON mf.codigo_ibge = d.cod_municipio
 AND mf.filial = e.filial;

COMMENT ON VIEW public.vw_fato_filial_exclusiva IS
'Fato por filial com cada município atribuído a UMA filial (sem duplicidade).
Regra: primeira filial em ordem alfabética para o município.';

-- =========================
-- MATERIALIZED VIEW: Atalhos para o último ano (painéis rápidos)
--   - Agrega por filial/grupo/produto no último ano
--   - Atualize após nova coleta com: REFRESH MATERIALIZED VIEW public.mv_fato_ultimo_ano;
-- =========================

-- Descobrir último ano (subselect interno)
CREATE MATERIALIZED VIEW IF NOT EXISTS public.mv_fato_ultimo_ano AS
SELECT
  f.filial,
  d.uf,
  CASE d.tabela
    WHEN 1612 THEN 'vegetal'
    WHEN 3939 THEN 'rebanho'
    WHEN 3946 THEN 'aquicultura'
    ELSE 'desconhecido'
  END AS grupo,
  d.produto_nome,
  d.unidade,
  d.ano,
  SUM(d.valor_num) AS valor
FROM public.dados_sidra_brutos d
JOIN public.municipios_filiais f
  ON f.codigo_ibge = d.cod_municipio
WHERE d.ano = (SELECT MAX(ano) FROM public.dados_sidra_brutos)
GROUP BY f.filial, d.uf, grupo, d.produto_nome, d.unidade, d.ano;

-- Índices para navegação da MV
CREATE INDEX IF NOT EXISTS idx_mv_ultimo_ano_filial
  ON public.mv_fato_ultimo_ano (filial);
CREATE INDEX IF NOT EXISTS idx_mv_ultimo_ano_grupo
  ON public.mv_fato_ultimo_ano (grupo);
CREATE INDEX IF NOT EXISTS idx_mv_ultimo_ano_produto
  ON public.mv_fato_ultimo_ano (produto_nome);
CREATE INDEX IF NOT EXISTS idx_mv_ultimo_ano_ano
  ON public.mv_fato_ultimo_ano (ano);

COMMENT ON MATERIALIZED VIEW public.mv_fato_ultimo_ano IS
'Agregado por filial/grupo/produto no último ano para acelerar dashboards. 
Atualize após nova coleta: REFRESH MATERIALIZED VIEW public.mv_fato_ultimo_ano;';

-- =========================
-- VIEW utilitária: Produtos do último ano (para filtros)
-- =========================
CREATE OR REPLACE VIEW public.vw_produtos_ultimo_ano AS
SELECT DISTINCT produto_nome
FROM public.dados_sidra_brutos
WHERE ano = (SELECT MAX(ano) FROM public.dados_sidra_brutos)
ORDER BY produto_nome;

COMMENT ON VIEW public.vw_produtos_ultimo_ano IS
'Lista de produtos disponíveis no último ano. Útil para filtros no BI.';

# --- [REFRESH MATERIALIZED VIEWS] -------------------------------------------

from db import get_engine

def refresh_materialized_views(concurrently: bool = False) -> list[str]:
    """
    Atualiza as materialized views usadas no BI.
    Se concurrently=True, usa REFRESH CONCURRENTLY (requer índice único na MV).
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


-- =========================
-- FIM
-- =========================
