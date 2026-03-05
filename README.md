# Exemplo de Data Warehouse (DW)

Projeto de exemplo demonstrando a implementação de um Data Warehouse com dados operacionais (OLTP) e dimensional (OLAP) usando PostgreSQL.

## 📋 Descrição

Este projeto implementa uma arquitetura completa de Data Warehouse, incluindo:
- **Banco Operacional (OLTP)**: Filiais, clientes, produtos e vendas
- **Dimensional (OLAP)**: Fatos e dimensões para análise de dados
- **Docker Compose**: Ambiente containerizado com PostgreSQL

## 🚀 Início Rápido

### Pré-requisitos
- Docker e Docker Compose instalados
- Git

### Instalação

1. Clone o repositório:
```bash
git clone https://github.com/gmcalixto/datascience_dw.git
cd datascience_dw
```

2. Inicie o banco de dados:
```bash
docker-compose up -d
```

3. Execute o script de criação de tabelas:
```bash
docker-compose exec postgres psql -U user -d mydb -f /tmp/op_dw.sql
```

4. Carregue os dados fictícios:
```bash
docker-compose exec postgres psql -U user -d mydb -f /tmp/op_carga.sql
```

## 📊 Estrutura do Banco

### Schemas
- **operacional**: Tabelas OLTP (Filial, Cliente, Produto, Venda, Item Venda)
- **dw**: Tabelas dimensionais e de fatos (DIM_Data, DIM_Filial, DIM_Cliente, DIM_Produto, FATO_Venda)

### Tabelas Principais

#### Operacional
- `filial`: 5 filiais de São Paulo
- `cliente`: 20 clientes fictícios
- `produto`: 12 produtos em diferentes categorias
- `venda`: 150 itens de venda com status variados

#### DW (Dimensional)
- `dim_data`: Dimensão de datas
- `dim_filial`: Dimensão de filiais
- `dim_cliente`: Dimensão de clientes
- `dim_produto`: Dimensão de produtos
- `fato_venda`: Fato de vendas consolidado

## 🔧 Configuração

### Docker Compose
O arquivo `docker-compose.yml` configura:
- PostgreSQL 13
- Banco de dados: `mydb`
- Usuário: `user`
- Senha: `password`
- Porta: `5432`

### Acesso ao Banco

```bash
docker-compose exec postgres psql -U user -d mydb
```

## 📁 Arquivos do Projeto

- `docker-compose.yml`: Configuração do ambiente Docker
- `op_dw.sql`: Script DDL (criação de tabelas, schemas, índices)
- `op_carga.sql`: Script de carga de dados fictícios
- `requirements.txt`: Dependências Python
- `transform_to_dw.sql`: ETL para transformação operacional → DW
- `validar_dw.sql`: Validações de integridade do DW
- `plots_dw.py`: Geração de gráficos analíticos
- `transform_to_dw.py`: ETL em Python
- `validar_dw.py`: Validações em Python

## 🧪 Consultando os Dados

Exemplo: Contar itens de venda
```sql
SELECT COUNT(*) AS itens FROM operacional.item_venda;
```

Exemplo: Vendas por filial
```sql
SELECT 
    f.nome, 
    COUNT(*) AS total_vendas,
    SUM(v.valor_total) AS receita
FROM operacional.venda v
JOIN operacional.filial f ON v.id_filial = f.id_filial
GROUP BY f.nome
ORDER BY receita DESC;
```

## 🛑 Parar o Ambiente

```bash
docker-compose down
```

## 📝 Licença

Este projeto é um exemplo educacional e está disponível para uso livre.

## 👤 Autor

gmcalixto
