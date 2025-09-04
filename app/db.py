import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def get_engine():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL não definido no ambiente.")
    engine = create_engine(url, future=True)
    return engine

def get_session():
    engine = get_engine()
    return sessionmaker(bind=engine, future=True)()

def ensure_schema(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS public.municipios_filiais (
        id SERIAL PRIMARY KEY,
        filial VARCHAR(120) NOT NULL,
        nome_municipio VARCHAR(160) NOT NULL,
        uf CHAR(2),
        codigo_ibge INTEGER,
        nome_normalizado VARCHAR(200),
        UNIQUE (filial, nome_municipio)
    );

    CREATE INDEX IF NOT EXISTS idx_munic_nome_norm ON public.municipios_filiais (nome_normalizado);
    CREATE INDEX IF NOT EXISTS idx_munic_cod_ibge ON public.municipios_filiais (codigo_ibge);

    CREATE TABLE IF NOT EXISTS public.produtos_sidra (
        codigo INTEGER PRIMARY KEY,
        nome VARCHAR(200) NOT NULL,
        grupo VARCHAR(40) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS public.dados_sidra_brutos (
        id BIGSERIAL PRIMARY KEY,
        tabela INTEGER NOT NULL,
        variavel INTEGER NOT NULL,
        ano INTEGER NOT NULL,
        cod_municipio INTEGER NOT NULL,
        nome_municipio VARCHAR(160) NOT NULL,
        uf CHAR(2),
        produto_codigo INTEGER,
        produto_nome VARCHAR(200),
        unidade VARCHAR(64),
        valor_str VARCHAR(64),
        valor_num DOUBLE PRECISION,
        coleta_em TIMESTAMP DEFAULT NOW(),
        origem VARCHAR(40) DEFAULT 'SIDRA',
        UNIQUE (tabela, variavel, ano, cod_municipio, produto_codigo)
    );

    CREATE INDEX IF NOT EXISTS idx_sidra_munic ON public.dados_sidra_brutos (cod_municipio);
    CREATE INDEX IF NOT EXISTS idx_sidra_prod ON public.dados_sidra_brutos (produto_codigo);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
