from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import Integer, String, BigInteger, CHAR

Base = declarative_base()

class MunicipioFilial(Base):
    __tablename__ = "municipios_filiais"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filial: Mapped[str] = mapped_column(String(120))
    nome_municipio: Mapped[str] = mapped_column(String(160))
    uf: Mapped[str] = mapped_column(CHAR(2), nullable=True)
    codigo_ibge: Mapped[int] = mapped_column(Integer, nullable=True)
    nome_normalizado: Mapped[str] = mapped_column(String(200), nullable=True)

class ProdutoSIDRA(Base):
    __tablename__ = "produtos_sidra"
    codigo: Mapped[int] = mapped_column(Integer, primary_key=True)
    nome: Mapped[str] = mapped_column(String(200))
    grupo: Mapped[str] = mapped_column(String(40))

class DadoSidraBruto(Base):
    __tablename__ = "dados_sidra_brutos"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tabela: Mapped[int] = mapped_column(Integer)
    variavel: Mapped[int] = mapped_column(Integer)
    ano: Mapped[int] = mapped_column(Integer)
    cod_municipio: Mapped[int] = mapped_column(Integer)
    nome_municipio: Mapped[str] = mapped_column(String(160))
    uf: Mapped[str] = mapped_column(CHAR(2))
    produto_codigo: Mapped[int] = mapped_column(Integer, nullable=True)
    produto_nome: Mapped[str] = mapped_column(String(200), nullable=True)
    unidade: Mapped[str] = mapped_column(String(64), nullable=True)
    valor_str: Mapped[str] = mapped_column(String(64), nullable=True)
    valor_num: Mapped[float] = mapped_column(nullable=True)
    origem: Mapped[str] = mapped_column(String(40))
