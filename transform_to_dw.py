import os
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

PROCESSO = "carga_dw_vendas"





def engine_from_env():
    user = os.getenv("PGUSER", "user")
    pwd = os.getenv("PGPASSWORD", "password")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "mydb")
    return create_engine(
        f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}",
        future=True
    )


def ensure_controle(conn):
    conn.execute(text("""
        INSERT INTO dw.etl_controle (processo, ultima_execucao)
        VALUES (:p, '2000-01-01 00:00:00')
        ON CONFLICT (processo) DO NOTHING
    """), {"p": PROCESSO})


def get_last_run(conn) -> datetime:
    return conn.execute(text("""
        SELECT ultima_execucao
        FROM dw.etl_controle
        WHERE processo = :p
    """), {"p": PROCESSO}).scalar_one()


def get_changed_range(conn, last_run: datetime):
    """
    Incremental baseado em venda.atualizado_em (didático e simples).
    """
    row = conn.execute(text("""
        SELECT
          MIN(v.data_hora) AS min_data_hora,
          MAX(v.data_hora) AS max_data_hora
        FROM operacional.venda v
        WHERE v.atualizado_em >= :lr
    """), {"lr": last_run}).mappings().one()

    return row["min_data_hora"], row["max_data_hora"]


def upsert_dim_tempo_for_range(conn, min_dt: datetime, max_dt: datetime):
    """
    CORREÇÃO: Postgres não aceita :param::date.
    Use CAST(:param AS date) para bind params.
    """
    if min_dt is None or max_dt is None:
        return

    conn.execute(text("""
        WITH datas AS (
          SELECT generate_series(
            CAST(:dini AS date),
            CAST(:dfim AS date),
            interval '1 day'
          )::date AS d
        )
        INSERT INTO dw.dim_tempo (data, ano, mes, dia, trimestre, dia_semana, nome_mes)
        SELECT
          d,
          EXTRACT(YEAR FROM d)::int,
          EXTRACT(MONTH FROM d)::int,
          EXTRACT(DAY FROM d)::int,
          ((EXTRACT(MONTH FROM d)::int - 1) / 3 + 1)::int,
          EXTRACT(ISODOW FROM d)::int, -- 1=Seg ... 7=Dom
          TO_CHAR(d, 'FMMonth')
        FROM datas
        ON CONFLICT (data) DO NOTHING
    """), {"dini": min_dt.date(), "dfim": max_dt.date()})


def upsert_dimensions(conn):
    # Filial
    conn.execute(text("""
        INSERT INTO dw.dim_filial (id_filial, nome, cidade, uf, ativo, atualizado_em)
        SELECT id_filial, nome, cidade, uf, ativo, NOW()
        FROM operacional.filial
        ON CONFLICT (id_filial) DO UPDATE SET
          nome = EXCLUDED.nome,
          cidade = EXCLUDED.cidade,
          uf = EXCLUDED.uf,
          ativo = EXCLUDED.ativo,
          atualizado_em = NOW()
    """))

    # Produto
    conn.execute(text("""
        INSERT INTO dw.dim_produto (id_produto, sku, nome, categoria, ativo, atualizado_em)
        SELECT id_produto, sku, nome, categoria, ativo, NOW()
        FROM operacional.produto
        ON CONFLICT (id_produto) DO UPDATE SET
          sku = EXCLUDED.sku,
          nome = EXCLUDED.nome,
          categoria = EXCLUDED.categoria,
          ativo = EXCLUDED.ativo,
          atualizado_em = NOW()
    """))

    # Cliente (minimalista)
    conn.execute(text("""
        INSERT INTO dw.dim_cliente (id_cliente, cidade, uf, atualizado_em)
        SELECT id_cliente, cidade, uf, NOW()
        FROM operacional.cliente
        ON CONFLICT (id_cliente) DO UPDATE SET
          cidade = EXCLUDED.cidade,
          uf = EXCLUDED.uf,
          atualizado_em = NOW()
    """))


def load_fact_incremental(conn, last_run: datetime):
    """
    Incremental por venda.atualizado_em.
    Carrega somente CONCLUIDA.
    Idempotência por id_item (PK da fato).
    """
    conn.execute(text("""
        INSERT INTO dw.fato_venda_item (
          sk_tempo, sk_filial, sk_produto, sk_cliente,
          id_venda, id_item,
          qtd, receita_bruta, desconto, receita_liquida,
          carregado_em
        )
        SELECT
          dt.sk_tempo,
          df.sk_filial,
          dp.sk_produto,
          dc.sk_cliente,
          v.id_venda,
          iv.id_item,
          iv.qtd,
          ROUND((iv.qtd * iv.preco_unit)::numeric, 2) AS receita_bruta,
          ROUND(iv.desconto::numeric, 2)              AS desconto,
          ROUND(((iv.qtd * iv.preco_unit) - iv.desconto)::numeric, 2) AS receita_liquida,
          NOW()
        FROM operacional.venda v
        JOIN operacional.item_venda iv ON iv.id_venda = v.id_venda
        JOIN dw.dim_tempo dt           ON dt.data = (v.data_hora::date)
        JOIN dw.dim_filial df          ON df.id_filial = v.id_filial
        JOIN dw.dim_produto dp         ON dp.id_produto = iv.id_produto
        LEFT JOIN dw.dim_cliente dc    ON dc.id_cliente = v.id_cliente
        WHERE v.status = 'CONCLUIDA'
          AND v.atualizado_em >= :lr
        ON CONFLICT (id_item) DO UPDATE SET
          sk_tempo = EXCLUDED.sk_tempo,
          sk_filial = EXCLUDED.sk_filial,
          sk_produto = EXCLUDED.sk_produto,
          sk_cliente = EXCLUDED.sk_cliente,
          id_venda = EXCLUDED.id_venda,
          qtd = EXCLUDED.qtd,
          receita_bruta = EXCLUDED.receita_bruta,
          desconto = EXCLUDED.desconto,
          receita_liquida = EXCLUDED.receita_liquida,
          carregado_em = NOW()
    """), {"lr": last_run})


def update_last_run(conn):
    conn.execute(text("""
        UPDATE dw.etl_controle
        SET ultima_execucao = NOW(),
            atualizado_em = NOW()
        WHERE processo = :p
    """), {"p": PROCESSO})


def main():
    eng = engine_from_env()
    with eng.begin() as conn:
        ensure_controle(conn)
        last_run = get_last_run(conn)

        min_dt, max_dt = get_changed_range(conn, last_run)
        if min_dt is None or max_dt is None:
            print(f"[DW] Nada a transformar/carregar desde {last_run}.")
            return

        upsert_dim_tempo_for_range(conn, min_dt, max_dt)
        upsert_dimensions(conn)
        load_fact_incremental(conn, last_run)
        update_last_run(conn)

        print(f"[DW] OK. Atualizado desde {last_run}. Intervalo: {min_dt} -> {max_dt}")


if __name__ == "__main__":
    main()