import os
import time
from sqlalchemy import create_engine, text



# ========= Config =========
PGUSER = os.getenv("PGUSER", "user")
PGPASSWORD = os.getenv("PGPASSWORD", "password")
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "mydb")

DB_URL = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"

PRINT_MAX_ROWS = int(os.getenv("PRINT_MAX_ROWS", "50"))


def run_query(conn, name: str, sql: str, params: dict | None = None, max_rows: int = PRINT_MAX_ROWS):
    print("\n" + "=" * 80)
    print(f"[QUERY] {name}")
    print("-" * 80)

    t0 = time.time()
    result = conn.execute(text(sql), params or {})
    elapsed = (time.time() - t0) * 1000

    rows = result.mappings().fetchmany(max_rows)
    all_cols = list(rows[0].keys()) if rows else list(result.keys())

    print(f"Tempo: {elapsed:.1f} ms")
    print(f"Colunas: {all_cols}")

    if not rows:
        print("Resultado: (0 linhas)")
        return [], all_cols

    # Print rows
    for i, r in enumerate(rows, start=1):
        print(f"{i:>3}: " + " | ".join(f"{k}={r.get(k)}" for k in all_cols))

    # Aviso se há mais linhas (não dá para saber sem consumir tudo; então apenas informamos limite)
    print(f"\nMostradas {len(rows)} linhas (limite {max_rows}).")
    return rows, all_cols


def main():
    engine = create_engine(DB_URL, future=True)

    queries = [
        (
            "1) Contagem de itens OP (CONCLUIDA) vs DW",
            """
            SELECT
              (SELECT COUNT(*)
               FROM operacional.venda v
               JOIN operacional.item_venda iv ON iv.id_venda = v.id_venda
               WHERE v.status = 'CONCLUIDA') AS op_itens_concluida,
              (SELECT COUNT(*) FROM dw.fato_venda_item) AS dw_itens;
            """
        ),
        (
            "2) Totais financeiros OP vs DW",
            """
            SELECT
              op.receita_bruta_op,
              dw.receita_bruta_dw,
              (op.receita_bruta_op - dw.receita_bruta_dw) AS diff_receita_bruta,
              op.desconto_op,
              dw.desconto_dw,
              (op.desconto_op - dw.desconto_dw) AS diff_desconto,
              op.receita_liquida_op,
              dw.receita_liquida_dw,
              (op.receita_liquida_op - dw.receita_liquida_dw) AS diff_receita_liquida
            FROM
              (SELECT
                  ROUND(SUM((iv.qtd * iv.preco_unit))::numeric, 2) AS receita_bruta_op,
                  ROUND(SUM(iv.desconto)::numeric, 2)             AS desconto_op,
                  ROUND(SUM((iv.qtd * iv.preco_unit) - iv.desconto)::numeric, 2) AS receita_liquida_op
               FROM operacional.venda v
               JOIN operacional.item_venda iv ON iv.id_venda = v.id_venda
               WHERE v.status = 'CONCLUIDA'
              ) op
            CROSS JOIN
              (SELECT
                  ROUND(SUM(receita_bruta)::numeric, 2)   AS receita_bruta_dw,
                  ROUND(SUM(desconto)::numeric, 2)        AS desconto_dw,
                  ROUND(SUM(receita_liquida)::numeric, 2) AS receita_liquida_dw
               FROM dw.fato_venda_item
              ) dw;
            """
        ),
        (
            "3) Divergências por dia + filial (deve voltar 0 linhas)",
            """
            WITH op AS (
              SELECT
                v.data_hora::date AS data,
                v.id_filial,
                COUNT(DISTINCT v.id_venda) AS qtd_vendas,
                COUNT(iv.id_item)          AS qtd_itens,
                ROUND(SUM((iv.qtd * iv.preco_unit))::numeric, 2) AS receita_bruta,
                ROUND(SUM(iv.desconto)::numeric, 2)              AS desconto,
                ROUND(SUM((iv.qtd * iv.preco_unit) - iv.desconto)::numeric, 2) AS receita_liquida
              FROM operacional.venda v
              JOIN operacional.item_venda iv ON iv.id_venda = v.id_venda
              WHERE v.status = 'CONCLUIDA'
              GROUP BY 1,2
            ),
            dwq AS (
              SELECT
                t.data AS data,
                df.id_filial,
                COUNT(DISTINCT x.id_venda) AS qtd_vendas,
                COUNT(x.id_item)           AS qtd_itens,
                ROUND(SUM(x.receita_bruta)::numeric, 2)   AS receita_bruta,
                ROUND(SUM(x.desconto)::numeric, 2)        AS desconto,
                ROUND(SUM(x.receita_liquida)::numeric, 2) AS receita_liquida
              FROM dw.fato_venda_item x
              JOIN dw.dim_tempo t   ON t.sk_tempo = x.sk_tempo
              JOIN dw.dim_filial df ON df.sk_filial = x.sk_filial
              GROUP BY 1,2
            )
            SELECT
              COALESCE(op.data, dwq.data) AS data,
              COALESCE(op.id_filial, dwq.id_filial) AS id_filial,
              COALESCE(op.qtd_vendas, 0) AS op_qtd_vendas,
              COALESCE(dwq.qtd_vendas, 0) AS dw_qtd_vendas,
              (COALESCE(op.qtd_vendas, 0) - COALESCE(dwq.qtd_vendas, 0)) AS diff_qtd_vendas,
              COALESCE(op.qtd_itens, 0) AS op_qtd_itens,
              COALESCE(dwq.qtd_itens, 0) AS dw_qtd_itens,
              (COALESCE(op.qtd_itens, 0) - COALESCE(dwq.qtd_itens, 0)) AS diff_qtd_itens,
              COALESCE(op.receita_bruta, 0) AS op_receita_bruta,
              COALESCE(dwq.receita_bruta, 0) AS dw_receita_bruta,
              (COALESCE(op.receita_bruta, 0) - COALESCE(dwq.receita_bruta, 0)) AS diff_receita_bruta,
              COALESCE(op.desconto, 0) AS op_desconto,
              COALESCE(dwq.desconto, 0) AS dw_desconto,
              (COALESCE(op.desconto, 0) - COALESCE(dwq.desconto, 0)) AS diff_desconto,
              COALESCE(op.receita_liquida, 0) AS op_receita_liquida,
              COALESCE(dwq.receita_liquida, 0) AS dw_receita_liquida,
              (COALESCE(op.receita_liquida, 0) - COALESCE(dwq.receita_liquida, 0)) AS diff_receita_liquida
            FROM op
            FULL OUTER JOIN dwq
              ON op.data = dwq.data AND op.id_filial = dwq.id_filial
            WHERE
              (COALESCE(op.qtd_vendas, 0) <> COALESCE(dwq.qtd_vendas, 0))
              OR (COALESCE(op.qtd_itens, 0) <> COALESCE(dwq.qtd_itens, 0))
              OR (COALESCE(op.receita_bruta, 0) <> COALESCE(dwq.receita_bruta, 0))
              OR (COALESCE(op.desconto, 0) <> COALESCE(dwq.desconto, 0))
              OR (COALESCE(op.receita_liquida, 0) <> COALESCE(dwq.receita_liquida, 0))
            ORDER BY 1,2;
            """
        ),
        (
            "4) Itens faltando no DW (existe no OP e não foi carregado)",
            """
            SELECT
              iv.id_item,
              v.id_venda,
              v.data_hora,
              v.id_filial,
              iv.id_produto,
              iv.qtd,
              iv.preco_unit,
              iv.desconto
            FROM operacional.venda v
            JOIN operacional.item_venda iv ON iv.id_venda = v.id_venda
            LEFT JOIN dw.fato_venda_item f ON f.id_item = iv.id_item
            WHERE v.status = 'CONCLUIDA'
              AND f.id_item IS NULL
            ORDER BY v.data_hora, iv.id_item
            LIMIT 200;
            """
        ),
        (
            "5) Itens sobrando no DW (DW sem correspondente no OP ou OP não CONCLUIDA)",
            """
            SELECT
              f.id_item,
              f.id_venda,
              t.data AS data_dw,
              df.id_filial AS id_filial_dw,
              f.qtd,
              f.receita_bruta,
              f.desconto,
              f.receita_liquida
            FROM dw.fato_venda_item f
            JOIN dw.dim_tempo t   ON t.sk_tempo = f.sk_tempo
            JOIN dw.dim_filial df ON df.sk_filial = f.sk_filial
            LEFT JOIN operacional.item_venda iv ON iv.id_item = f.id_item
            LEFT JOIN operacional.venda v       ON v.id_venda = f.id_venda
            WHERE iv.id_item IS NULL
               OR v.id_venda IS NULL
               OR v.status <> 'CONCLUIDA'
            ORDER BY t.data DESC, f.id_item
            LIMIT 200;
            """
        ),
        (
            "6) Checagem de chaves dimensionais (deve dar 0 em tudo)",
            """
            SELECT
              SUM(CASE WHEN dt.sk_tempo  IS NULL THEN 1 ELSE 0 END) AS sem_dim_tempo,
              SUM(CASE WHEN df.sk_filial IS NULL THEN 1 ELSE 0 END) AS sem_dim_filial,
              SUM(CASE WHEN dp.sk_produto IS NULL THEN 1 ELSE 0 END) AS sem_dim_produto
            FROM dw.fato_venda_item f
            LEFT JOIN dw.dim_tempo   dt ON dt.sk_tempo   = f.sk_tempo
            LEFT JOIN dw.dim_filial  df ON df.sk_filial  = f.sk_filial
            LEFT JOIN dw.dim_produto dp ON dp.sk_produto = f.sk_produto;
            """
        ),
        (
            "7) Sanity check (receita total + ticket médio)",
            """
            SELECT
              (SELECT ROUND(SUM(receita_liquida)::numeric, 2) FROM dw.fato_venda_item) AS dw_receita_liquida_total,
              (SELECT ROUND(SUM((iv.qtd * iv.preco_unit) - iv.desconto)::numeric, 2)
               FROM operacional.venda v
               JOIN operacional.item_venda iv ON iv.id_venda = v.id_venda
               WHERE v.status='CONCLUIDA') AS op_receita_liquida_total,
              (SELECT ROUND(SUM(receita_liquida)::numeric / NULLIF(COUNT(DISTINCT id_venda),0), 2)
               FROM dw.fato_venda_item) AS dw_ticket_medio,
              (SELECT ROUND(
                  SUM((iv.qtd * iv.preco_unit) - iv.desconto)::numeric / NULLIF(COUNT(DISTINCT v.id_venda),0),
                  2
               )
               FROM operacional.venda v
               JOIN operacional.item_venda iv ON iv.id_venda = v.id_venda
               WHERE v.status='CONCLUIDA') AS op_ticket_medio;
            """
        ),
    ]

    with engine.connect() as conn:
        # Só leitura
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")

        failed = False

        for name, sql in queries:
            rows, cols = run_query(conn, name, sql)

            # checks simples para destacar divergência nas queries 1 e 2
            if name.startswith("1)") and rows:
                op = rows[0].get("op_itens_concluida")
                dw = rows[0].get("dw_itens")
                if op != dw:
                    print(f"\n[ALERTA] Contagem diferente! OP={op} DW={dw}")
                    failed = True

            if name.startswith("2)") and rows:
                d1 = rows[0].get("diff_receita_bruta")
                d2 = rows[0].get("diff_desconto")
                d3 = rows[0].get("diff_receita_liquida")
                if any(v not in (0, 0.0, None) for v in (d1, d2, d3)):
                    print(f"\n[ALERTA] Diferença financeira detectada: bruta={d1} desconto={d2} líquida={d3}")
                    failed = True

            if name.startswith("3)") and rows:
                print("\n[ALERTA] Há divergências por dia/filial (o ideal é 0 linhas).")
                failed = True

            if name.startswith("4)") and rows:
                print("\n[ALERTA] Existem itens faltando no DW (OP tem e DW não).")
                failed = True

            if name.startswith("5)") and rows:
                print("\n[ALERTA] Existem itens sobrando no DW (DW tem e OP não/ não CONCLUIDA).")
                failed = True

            if name.startswith("6)") and rows:
                if any(rows[0].get(k, 0) not in (0, None) for k in ["sem_dim_tempo", "sem_dim_filial", "sem_dim_produto"]):
                    print("\n[ALERTA] Fato com chaves sem dimensão correspondente.")
                    failed = True

        print("\n" + "=" * 80)
        if failed:
            print("[RESULTADO] Validação encontrou divergências. Verifique os blocos sinalizados.")
            raise SystemExit(1)
        else:
            print("[RESULTADO] Validação OK ✅ (OP e DW batem para vendas CONCLUIDA).")


def run_query(conn, name: str, sql: str, params=None, max_rows=50):
    print("\n" + "-" * 80)
    print(name)
    t0 = time.time()
    res = conn.execute(text(sql), params or {})
    ms = (time.time() - t0) * 1000

    rows = res.mappings().fetchmany(max_rows)
    cols = list(rows[0].keys()) if rows else list(res.keys())

    print(f"Tempo: {ms:.1f} ms | Linhas (mostradas): {len(rows)} | Colunas: {cols}")
    for i, r in enumerate(rows, start=1):
        print(f"{i:>3}: " + " | ".join(f"{k}={r.get(k)}" for k in cols))
    return rows, cols


if __name__ == "__main__":
    main()