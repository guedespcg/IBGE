import os
from sqlalchemy import create_engine as _create_engine, text

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        url = os.getenv("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL não definido.")
        _engine = _create_engine(url, pool_pre_ping=True, future=True)
    return _engine

def ensure_schema():
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.municipios_filiais (
                id SERIAL PRIMARY KEY,
                filial TEXT NOT NULL,
                nome_municipio TEXT NOT NULL,
                uf CHAR(2) NOT NULL,
                codigo_ibge VARCHAR(7) NULL
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.culturas_filiais (
                id SERIAL PRIMARY KEY,
                filial TEXT NOT NULL,
                tabela INTEGER NOT NULL,
                cod_produto TEXT NOT NULL,
                produto TEXT NULL
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.dados_sidra_brutos (
                id BIGSERIAL PRIMARY KEY,
                ano INT NOT NULL,
                tabela INT NOT NULL,
                variavel INT NOT NULL,
                uf CHAR(2) NOT NULL DEFAULT '',
                municipio TEXT NOT NULL,
                codigo_ibge VARCHAR(7) NOT NULL,
                cod_produto TEXT NOT NULL,
                produto TEXT NULL,
                valor NUMERIC NULL,
                unidade TEXT NULL,
                fonte TEXT NOT NULL DEFAULT 'SIDRA',
                filial TEXT NOT NULL,
                criado_em TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            );
        """))
