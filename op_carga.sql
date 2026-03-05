-- ============================================================
-- DW DEMO (PostgreSQL) - SEED OPERACIONAL (DADOS FICTÍCIOS)
-- Cria 5 filiais + produtos + clientes + vendas + itens de venda
-- Coloque este arquivo em: sql/init/003_seed_operacional.sql
-- ============================================================

BEGIN;

-- ---------- 1) FILIAIS (5) ----------
INSERT INTO operacional.filial (nome, cidade, uf, ativo)
VALUES
  ('Filial Centro',      'São Paulo',     'SP', TRUE),
  ('Filial Zona Sul',    'São Paulo',     'SP', TRUE),
  ('Filial Campinas',    'Campinas',      'SP', TRUE),
  ('Filial Santos',      'Santos',        'SP', TRUE),
  ('Filial São José',    'São José dos Campos', 'SP', TRUE)
ON CONFLICT DO NOTHING;

-- ---------- 2) PRODUTOS (12) ----------
INSERT INTO operacional.produto (sku, nome, categoria, preco_lista, ativo)
VALUES
  ('SKU-001', 'Camiseta Básica',        'Vestuário',  59.90, TRUE),
  ('SKU-002', 'Calça Jeans',            'Vestuário', 159.90, TRUE),
  ('SKU-003', 'Tênis Casual',           'Calçados',  229.90, TRUE),
  ('SKU-004', 'Meia Esportiva',         'Acessórios', 19.90, TRUE),
  ('SKU-005', 'Boné',                   'Acessórios', 49.90, TRUE),
  ('SKU-006', 'Jaqueta Corta-Vento',    'Vestuário', 299.90, TRUE),
  ('SKU-007', 'Mochila',                'Acessórios',189.90, TRUE),
  ('SKU-008', 'Relógio Digital',        'Eletrônicos',199.90, TRUE),
  ('SKU-009', 'Fone Bluetooth',         'Eletrônicos',149.90, TRUE),
  ('SKU-010', 'Garrafa Térmica',        'Utilidades', 79.90, TRUE),
  ('SKU-011', 'Óculos de Sol',          'Acessórios',129.90, TRUE),
  ('SKU-012', 'Tênis Corrida',          'Calçados',  349.90, TRUE)
ON CONFLICT (sku) DO NOTHING;

-- ---------- 3) CLIENTES (20) ----------
INSERT INTO operacional.cliente (nome, cpf_hash, cidade, uf)
SELECT
  'Cliente ' || gs::text,
  md5('cpf_fake_' || gs::text),
  (ARRAY['São Paulo','Campinas','Santos','São José dos Campos','Sorocaba'])[1 + (gs % 5)],
  'SP'
FROM generate_series(1,20) gs
ON CONFLICT DO NOTHING;

-- ---------- 4) VENDAS + ITENS (60 vendas no período + mix por filial/canal) ----------
-- Estratégia:
-- - 60 vendas em 20 dias (3 por dia em média)
-- - status majoritariamente CONCLUIDA e algumas CANCELADA/PENDENTE
-- - itens 1..4 por venda
-- - descontos ocasionais
WITH params AS (
  SELECT
    (SELECT MIN(id_filial) FROM operacional.filial) AS min_filial,
    (SELECT MIN(id_cliente) FROM operacional.cliente) AS min_cliente
),
vendas AS (
  INSERT INTO operacional.venda (id_filial, id_cliente, data_hora, status, canal, observacao)
  SELECT
    -- 5 filiais (ciclo)
    (SELECT id_filial FROM operacional.filial ORDER BY id_filial LIMIT 1 OFFSET (gs % 5)) AS id_filial,
    -- clientes (ciclo)
    (SELECT id_cliente FROM operacional.cliente ORDER BY id_cliente LIMIT 1 OFFSET (gs % 20)) AS id_cliente,
    -- datas nos últimos 20 dias
    (NOW()::date - ((gs % 20))::int) + (time '10:00' + ((gs % 9) * interval '45 minutes')) AS data_hora,
    -- status (80% concluída, 10% cancelada, 10% pendente)
    CASE
      WHEN (gs % 10) IN (0) THEN 'CANCELADA'
      WHEN (gs % 10) IN (1) THEN 'PENDENTE'
      ELSE 'CONCLUIDA'
    END AS status,
    -- canal (mais loja)
    CASE
      WHEN (gs % 10) IN (0,1) THEN 'ONLINE'
      WHEN (gs % 10) IN (2) THEN 'TELEVENDAS'
      ELSE 'LOJA'
    END AS canal,
    NULL::text AS observacao
  FROM generate_series(1,60) gs
  RETURNING id_venda, data_hora, status
),
itens AS (
  INSERT INTO operacional.item_venda (id_venda, id_produto, qtd, preco_unit, desconto)
  SELECT
    v.id_venda,
    -- produtos 12 (ciclo, com variação por venda/item)
    (SELECT id_produto FROM operacional.produto ORDER BY id_produto LIMIT 1
     OFFSET ((v.id_venda + it.i) % 12)) AS id_produto,
    -- quantidade 1..3
    1 + ((v.id_venda + it.i) % 3) AS qtd,
    -- preço unit a partir do preço_lista com pequena variação (simula promo)
    ROUND( (p.preco_lista * (0.95 + (((v.id_venda + it.i) % 7) * 0.01)))::numeric, 2 ) AS preco_unit,
    -- desconto ocasional (0, 5, 10, 15 reais) só em parte dos itens e preferindo CONCLUIDA
    CASE
      WHEN v.status = 'CONCLUIDA' AND ((v.id_venda + it.i) % 6) = 0 THEN 15.00
      WHEN v.status = 'CONCLUIDA' AND ((v.id_venda + it.i) % 6) = 1 THEN 10.00
      WHEN v.status = 'CONCLUIDA' AND ((v.id_venda + it.i) % 6) = 2 THEN  5.00
      ELSE 0.00
    END AS desconto
  FROM vendas v
  -- 1..4 itens por venda
  JOIN LATERAL generate_series(1, 1 + (v.id_venda % 4)) AS it(i) ON TRUE
  JOIN LATERAL (
    SELECT preco_lista
    FROM operacional.produto
    ORDER BY id_produto
    LIMIT 1
    OFFSET ((v.id_venda + it.i) % 12)
  ) p ON TRUE
  RETURNING 1
)
SELECT 1;

COMMIT;

-- ---------- 5) CHECK RÁPIDO (apenas para você rodar manualmente se quiser) ----------
-- SELECT status, count(*) FROM operacional.venda GROUP BY 1 ORDER BY 1;
-- SELECT count(*) FROM operacional.item_venda;
-- SELECT * FROM operacional.filial ORDER BY id_filial;