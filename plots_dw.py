import os
from datetime import date, datetime, timedelta

import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text


# -----------------------------
# Config
# -----------------------------

PGUSER = os.getenv("PGUSER", "user")
PGPASSWORD = os.getenv("PGPASSWORD", "password")
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "mydb")


DB_URL = f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"

# Período padrão: últimos 20 dias (ajuste livremente)
DEFAULT_DAYS = int(os.getenv("DAYS", "20"))

# Pasta de saída para as imagens
OUT_DIR = os.getenv("OUT_DIR", "out_plots")


def get_engine():
    return create_engine(DB_URL, future=True)


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def load_kpi_diario(engine, dt_ini: date, dt_fim: date) -> pd.DataFrame:
    sql = """
    SELECT
      data,
      filial,
      cidade,
      uf,
      receita_liquida,
      receita_bruta,
      desconto_total,
      pct_desconto_medio,
      qtd_vendas,
      itens_vendidos,
      ticket_medio,
      itens_por_venda
    FROM dw.vw_kpi_diario_filial
    WHERE data BETWEEN :dt_ini AND :dt_fim
    ORDER BY data, filial;
    """
    return pd.read_sql(text(sql), engine, params={"dt_ini": dt_ini, "dt_fim": dt_fim})


def load_kpi_mensal_categoria(engine, ano_ini: int, mes_ini: int, ano_fim: int, mes_fim: int) -> pd.DataFrame:
    # filtro simples por (ano,mes) no intervalo
    sql = """
    SELECT
      ano, mes, filial, categoria,
      receita_liquida, qtd_vendas, itens_vendidos
    FROM dw.vw_kpi_mensal_filial_categoria
    WHERE (ano > :a0 OR (ano = :a0 AND mes >= :m0))
      AND (ano < :a1 OR (ano = :a1 AND mes <= :m1))
    ORDER BY ano, mes, filial, categoria;
    """
    return pd.read_sql(
        text(sql),
        engine,
        params={"a0": ano_ini, "m0": mes_ini, "a1": ano_fim, "m1": mes_fim},
    )


# -----------------------------
# Plots
# -----------------------------
def plot_receita_diaria_linha(df: pd.DataFrame):
    """
    Linhas: receita líquida diária por filial
    """
    if df.empty:
        print("[plot_receita_diaria_linha] Sem dados no período.")
        return

    pivot = df.pivot_table(index="data", columns="filial", values="receita_liquida", aggfunc="sum").fillna(0)

    plt.figure()
    for col in pivot.columns:
        plt.plot(pivot.index, pivot[col], label=col)

    plt.title("Receita líquida diária por filial")
    plt.xlabel("Data")
    plt.ylabel("Receita líquida")
    plt.legend()
    plt.tight_layout()
    path = os.path.join(OUT_DIR, "01_receita_liquida_diaria_por_filial.png")
    plt.savefig(path, dpi=140)
    plt.close()
    print(f"[OK] {path}")


def plot_receita_total_barras(df: pd.DataFrame):
    """
    Barras: receita líquida total por filial no período
    """
    if df.empty:
        print("[plot_receita_total_barras] Sem dados no período.")
        return

    agg = df.groupby("filial", as_index=False)["receita_liquida"].sum().sort_values("receita_liquida", ascending=False)

    plt.figure()
    plt.bar(agg["filial"], agg["receita_liquida"])
    plt.title("Receita líquida total por filial (período)")
    plt.xlabel("Filial")
    plt.ylabel("Receita líquida")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    path = os.path.join(OUT_DIR, "02_receita_liquida_total_por_filial.png")
    plt.savefig(path, dpi=140)
    plt.close()
    print(f"[OK] {path}")


def plot_heatmap_dia_semana_filial(df: pd.DataFrame, engine):
    """
    Heatmap (matplotlib puro):
    receita líquida por dia da semana x filial
    """
    if df.empty:
        print("[plot_heatmap_dia_semana_filial] Sem dados no período.")
        return

    # Pega dia da semana a partir da dim_tempo para o período
    sql = """
    SELECT t.data, t.dia_semana
    FROM dw.dim_tempo t
    WHERE t.data BETWEEN :dt_ini AND :dt_fim;
    """
    dt_ini = df["data"].min()
    dt_fim = df["data"].max()
    tempo = pd.read_sql(text(sql), engine, params={"dt_ini": dt_ini, "dt_fim": dt_fim})

    base = df.merge(tempo, on="data", how="left")

    pivot = base.pivot_table(index="dia_semana", columns="filial", values="receita_liquida", aggfunc="sum").fillna(0)

    plt.figure()
    img = plt.imshow(pivot.values, aspect="auto")
    plt.title("Heatmap: Receita líquida por dia da semana x filial")
    plt.xlabel("Filial")
    plt.ylabel("Dia da semana (1=Seg ... 7=Dom)")
    plt.xticks(range(len(pivot.columns)), pivot.columns, rotation=25, ha="right")
    plt.yticks(range(len(pivot.index)), pivot.index)
    plt.colorbar(img, label="Receita líquida")
    plt.tight_layout()
    path = os.path.join(OUT_DIR, "03_heatmap_receita_dia_semana_x_filial.png")
    plt.savefig(path, dpi=140)
    plt.close()
    print(f"[OK] {path}")


def plot_receita_mensal_categoria_empilhado(df_m: pd.DataFrame):
    """
    Barras empilhadas: receita líquida mensal por categoria, empilhada por filial (top categorias)
    """
    if df_m.empty:
        print("[plot_receita_mensal_categoria_empilhado] Sem dados no período.")
        return

    # Cria uma coluna "periodo" YYYY-MM
    df_m = df_m.copy()
    df_m["periodo"] = df_m["ano"].astype(str) + "-" + df_m["mes"].astype(str).str.zfill(2)

    # Pega top 6 categorias no período
    top_cat = (
        df_m.groupby("categoria", as_index=False)["receita_liquida"]
        .sum()
        .sort_values("receita_liquida", ascending=False)
        .head(6)["categoria"]
        .tolist()
    )
    df_m = df_m[df_m["categoria"].isin(top_cat)]

    # Pivot: índice periodo+categoria, colunas filial
    pivot = df_m.pivot_table(
        index=["periodo", "categoria"],
        columns="filial",
        values="receita_liquida",
        aggfunc="sum",
        fill_value=0,
    )

    # Plot empilhado manual
    plt.figure()
    x_labels = [f"{p}\n{c}" for (p, c) in pivot.index]
    x = range(len(x_labels))

    bottom = None
    for filial in pivot.columns:
        vals = pivot[filial].values
        if bottom is None:
            plt.bar(x, vals, label=filial)
            bottom = vals
        else:
            plt.bar(x, vals, bottom=bottom, label=filial)
            bottom = bottom + vals

    plt.title("Receita líquida mensal por categoria (Top 6) - empilhado por filial")
    plt.xlabel("Período e categoria")
    plt.ylabel("Receita líquida")
    plt.xticks(list(x), x_labels, rotation=25, ha="right")
    plt.legend()
    plt.tight_layout()
    path = os.path.join(OUT_DIR, "04_receita_mensal_categoria_empilhado_por_filial.png")
    plt.savefig(path, dpi=140)
    plt.close()
    print(f"[OK] {path}")


def main():
    ensure_out_dir()
    engine = get_engine()

    dt_fim = date.today()
    dt_ini = dt_fim - timedelta(days=DEFAULT_DAYS)

    df = load_kpi_diario(engine, dt_ini, dt_fim)

    plot_receita_diaria_linha(df)
    plot_receita_total_barras(df)
    plot_heatmap_dia_semana_filial(df, engine)

    # mensal: pega intervalo baseado no período diário
    ano_ini, mes_ini = dt_ini.year, dt_ini.month
    ano_fim, mes_fim = dt_fim.year, dt_fim.month
    df_m = load_kpi_mensal_categoria(engine, ano_ini, mes_ini, ano_fim, mes_fim)
    plot_receita_mensal_categoria_empilhado(df_m)

    print(f"\nImagens geradas em: {OUT_DIR}/")


if __name__ == "__main__":
    main()