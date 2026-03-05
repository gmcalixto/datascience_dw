-- ============================================================
-- DW DEMO (PostgreSQL) - DDL COMPLETO
-- Operacional (OLTP) + DW (Dimensional)
-- Coloque este arquivo em: sql/init/002_ddl_completo.sql
-- Ele será executado automaticamente pelo Postgres no docker-compose.
-- ============================================================

-- ---------- 1) Schemas ----------
CREATE SCHEMA IF NOT EXISTS operacional;
CREATE SCHEMA IF NOT EXISTS dw;

-- Opcional: deixar o schema padrão como operacional para sessões interativas
-- (No ETL, prefira sempre qualificar com schema.)
-- ALTER ROLE postgres SET search_path TO operacional, dw, public;

-- ---------- 2) OPERACIONAL (OLTP) ----------
-- 2.1 Filial
CREATE TABLE IF NOT EXISTS operacional.filial (
  id_filial      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nome           VARCHAR(80) NOT NULL,
  cidade         VARCHAR(80) NOT NULL,
  uf             CHAR(2)     NOT NULL,
  ativo          BOOLEAN     NOT NULL DEFAULT TRUE,
  criado_em      TIMESTAMP   NOT NULL DEFAULT NOW(),
  atualizado_em  TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- 2.2 Cliente (sem dados sensíveis reais; use hash/fake)
CREATE TABLE IF NOT EXISTS operacional.cliente (
  id_cliente     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nome           VARCHAR(120) NOT NULL,
  cpf_hash       VARCHAR(128), -- opcional (hash), não use CPF real
  cidade         VARCHAR(80),
  uf             CHAR(2),
  criado_em      TIMESTAMP NOT NULL DEFAULT NOW(),
  atualizado_em  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 2.3 Produto
CREATE TABLE IF NOT EXISTS operacional.produto (
  id_produto     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku            VARCHAR(40)  NOT NULL UNIQUE,
  nome           VARCHAR(120) NOT NULL,
  categoria      VARCHAR(80)  NOT NULL,
  preco_lista    NUMERIC(12,2) NOT NULL CHECK (preco_lista >= 0),
  ativo          BOOLEAN      NOT NULL DEFAULT TRUE,
  criado_em      TIMESTAMP    NOT NULL DEFAULT NOW(),
  atualizado_em  TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- 2.4 Venda (cabeçalho)
CREATE TABLE IF NOT EXISTS operacional.venda (
  id_venda       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_filial      INTEGER NOT NULL REFERENCES operacional.filial(id_filial),
  id_cliente     BIGINT  REFERENCES operacional.cliente(id_cliente),
  data_hora      TIMESTAMP NOT NULL,
  status         VARCHAR(20) NOT NULL,
  canal          VARCHAR(20) NOT NULL,
  observacao     TEXT,
  criado_em      TIMESTAMP NOT NULL DEFAULT NOW(),
  atualizado_em  TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT ck_venda_status CHECK (status IN ('CONCLUIDA','CANCELADA','PENDENTE')),
  CONSTRAINT ck_venda_canal  CHECK (canal  IN ('LOJA','ONLINE','TELEVENDAS'))
);

-- 2.5 Item de Venda (detalhe)
CREATE TABLE IF NOT EXISTS operacional.item_venda (
  id_item        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_venda       BIGINT NOT NULL REFERENCES operacional.venda(id_venda) ON DELETE CASCADE,
  id_produto     BIGINT NOT NULL REFERENCES operacional.produto(id_produto),
  qtd            INTEGER NOT NULL CHECK (qtd > 0),
  preco_unit     NUMERIC(12,2) NOT NULL CHECK (preco_unit >= 0),
  desconto       NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (desconto >= 0),
  criado_em      TIMESTAMP NOT NULL DEFAULT NOW(),
  atualizado_em  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Índices operacionais (para extração incremental e joins comuns)
CREATE INDEX IF NOT EXISTS ix_venda_datahora   ON operacional.venda (data_hora);
CREATE INDEX IF NOT EXISTS ix_venda_filial     ON operacional.venda (id_filial);
CREATE INDEX IF NOT EXISTS ix_item_venda_venda ON operacional.item_venda (id_venda);
CREATE INDEX IF NOT EXISTS ix_item_venda_prod  ON operacional.item_venda (id_produto);

-- ---------- 3) DATA WAREHOUSE (Dimensional) ----------
-- 3.1 Controle de ETL (para incremental)
CREATE TABLE IF NOT EXISTS dw.etl_controle (
  processo          VARCHAR(60) PRIMARY KEY,
  ultima_execucao   TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00',
  atualizado_em     TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO dw.etl_controle (processo, ultima_execucao)
VALUES ('carga_dw_vendas', '2000-01-01 00:00:00')
ON CONFLICT (processo) DO NOTHING;

-- 3.2 Dimensão Tempo
CREATE TABLE IF NOT EXISTS dw.dim_tempo (
  sk_tempo      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  data          DATE NOT NULL UNIQUE,
  ano           SMALLINT NOT NULL,
  mes           SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),
  dia           SMALLINT NOT NULL CHECK (dia BETWEEN 1 AND 31),
  trimestre     SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
  dia_semana    SMALLINT NOT NULL CHECK (dia_semana BETWEEN 1 AND 7), -- 1=Seg ... 7=Dom (padrão do ETL)
  nome_mes      VARCHAR(12) NOT NULL
);

-- 3.3 Dimensão Filial
CREATE TABLE IF NOT EXISTS dw.dim_filial (
  sk_filial     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_filial     INTEGER NOT NULL UNIQUE,
  nome          VARCHAR(80) NOT NULL,
  cidade        VARCHAR(80) NOT NULL,
  uf            CHAR(2) NOT NULL,
  ativo         BOOLEAN NOT NULL,
  atualizado_em TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 3.4 Dimensão Produto
CREATE TABLE IF NOT EXISTS dw.dim_produto (
  sk_produto    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_produto    BIGINT NOT NULL UNIQUE,
  sku           VARCHAR(40) NOT NULL,
  nome          VARCHAR(120) NOT NULL,
  categoria     VARCHAR(80) NOT NULL,
  ativo         BOOLEAN NOT NULL,
  atualizado_em TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 3.5 Dimensão Cliente (minimalista, evitando dados sensíveis)
CREATE TABLE IF NOT EXISTS dw.dim_cliente (
  sk_cliente    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_cliente    BIGINT NOT NULL UNIQUE,
  cidade        VARCHAR(80),
  uf            CHAR(2),
  atualizado_em TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 3.6 Fato Venda Item (grão: item de venda)
CREATE TABLE IF NOT EXISTS dw.fato_venda_item (
  sk_tempo        INTEGER NOT NULL REFERENCES dw.dim_tempo(sk_tempo),
  sk_filial       INTEGER NOT NULL REFERENCES dw.dim_filial(sk_filial),
  sk_produto      BIGINT  NOT NULL REFERENCES dw.dim_produto(sk_produto),
  sk_cliente      BIGINT  REFERENCES dw.dim_cliente(sk_cliente),
  id_venda        BIGINT  NOT NULL,  -- degenerate dimension (para drill-through)
  id_item         BIGINT  NOT NULL,  -- degenerate dimension (chave do item no OLTP)
  qtd             INTEGER NOT NULL CHECK (qtd > 0),
  receita_bruta   NUMERIC(14,2) NOT NULL CHECK (receita_bruta >= 0),
  desconto        NUMERIC(14,2) NOT NULL CHECK (desconto >= 0),
  receita_liquida NUMERIC(14,2) NOT NULL CHECK (receita_liquida >= 0),
  carregado_em    TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id_item)  -- garante idempotência simples na carga (um item vira uma linha)
);

-- Índices DW (para acelerar consolidações)
CREATE INDEX IF NOT EXISTS ix_fato_tempo_filial  ON dw.fato_venda_item (sk_tempo, sk_filial);
CREATE INDEX IF NOT EXISTS ix_fato_produto       ON dw.fato_venda_item (sk_produto);
CREATE INDEX IF NOT EXISTS ix_fato_venda         ON dw.fato_venda_item (id_venda);

-- ---------- 4) VIEWS de Indicadores (opcional, mas muito útil p/ aula) ----------
-- 4.1 Indicadores diários por filial
CREATE OR REPLACE VIEW dw.vw_kpi_diario_filial AS
SELECT
  t.data,
  f.nome          AS filial,
  f.cidade,
  f.uf,
  SUM(x.receita_liquida)                 AS receita_liquida,
  SUM(x.receita_bruta)                   AS receita_bruta,
  SUM(x.desconto)                        AS desconto_total,
  CASE WHEN SUM(x.receita_bruta) > 0
       THEN ROUND((SUM(x.desconto) / SUM(x.receita_bruta)) * 100, 2)
       ELSE 0 END                        AS pct_desconto_medio,
  COUNT(DISTINCT x.id_venda)             AS qtd_vendas,
  SUM(x.qtd)                             AS itens_vendidos,
  CASE WHEN COUNT(DISTINCT x.id_venda) > 0
       THEN ROUND(SUM(x.receita_liquida) / COUNT(DISTINCT x.id_venda), 2)
       ELSE 0 END                        AS ticket_medio,
  CASE WHEN COUNT(DISTINCT x.id_venda) > 0
       THEN ROUND(SUM(x.qtd)::numeric / COUNT(DISTINCT x.id_venda), 2)
       ELSE 0 END                        AS itens_por_venda
FROM dw.fato_venda_item x
JOIN dw.dim_tempo  t ON t.sk_tempo  = x.sk_tempo
JOIN dw.dim_filial f ON f.sk_filial = x.sk_filial
GROUP BY t.data, f.nome, f.cidade, f.uf;

-- 4.2 Indicadores mensais por filial e categoria
CREATE OR REPLACE VIEW dw.vw_kpi_mensal_filial_categoria AS
SELECT
  t.ano,
  t.mes,
  f.nome             AS filial,
  p.categoria,
  SUM(x.receita_liquida)         AS receita_liquida,
  COUNT(DISTINCT x.id_venda)     AS qtd_vendas,
  SUM(x.qtd)                     AS itens_vendidos
FROM dw.fato_venda_item x
JOIN dw.dim_tempo   t ON t.sk_tempo   = x.sk_tempo
JOIN dw.dim_filial  f ON f.sk_filial  = x.sk_filial
JOIN dw.dim_produto p ON p.sk_produto = x.sk_produto
GROUP BY t.ano, t.mes, f.nome, p.categoria;

-- ---------- 5) Comentário final ----------
-- A carga ETL típica fará:
-- 1) Preencher dw.dim_tempo (ou pré-popular um range de datas)
-- 2) UPSERT em dw.dim_filial, dw.dim_produto, dw.dim_cliente a partir do operacional
-- 3) Inserir incrementalmente em dw.fato_venda_item (somente vendas CONCLUIDAS)
-- 4) Consultar as views dw.vw_kpi_* para mostrar consolidação de indicadores