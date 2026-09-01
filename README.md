# Pipeline de Dados — Teste Técnico Analytics Engineer Sênior (Vena BPO)

Pipeline ELT que consolida três fontes heterogêneas (API de vendas, scraping
de preços de concorrentes e banco transacional SQLite) em um dataset
analítico no BigQuery, para alimentar um dashboard diário de "saúde
comercial".

> Status: ingestão do SQLite implementada (clientes, produtos, itens_pedido
> em chunks -> Parquet no GCS -> raw no BigQuery). Ingestão da API, do
> scraping, modelagem dbt e orquestração Dagster seguem nos próximos
> commits. Ver `docs/architecture.md` para o diagrama de fluxo e as decisões
> de arquitetura.

## Estrutura do repositório

```
.
├── docs/
│   └── architecture.md        # diagrama do fluxo + decisões de arquitetura
├── scripts/
│   └── ingest_sqlite.py        # orquestra a ingestão do SQLite (standalone por enquanto)
├── src/vena_pipeline/
│   ├── config.py               # configuração central (lê .env)
│   ├── ingestion/               # extração de cada fonte (raw)
│   │   ├── sqlite_extract.py    # ✅ implementado — extração em chunks
│   │   ├── loaders.py           # ✅ implementado — Parquet no GCS + load BigQuery raw
│   │   ├── api_vendas.py        # próxima etapa
│   │   └── scraping_concorrentes.py  # próxima etapa
│   ├── dagster_defs/            # orquestração (próxima etapa)
│   │   ├── assets.py
│   │   ├── resources.py
│   │   ├── asset_checks.py
│   │   └── definitions.py
│   └── utils/                   # logging estruturado, clientes GCP
├── dbt/vena_pipeline/            # staging -> mart, tests, snapshot (SCD2)
├── tests/                        # testes unitários (pytest)
└── data/                         # scratch local (gitignored)
```

## Setup local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# preencher .env com o caminho da credencial e os parâmetros do ambiente

# copiar o sqlite fornecido para dentro de data/ (não versionado)
cp /caminho/para/banco_transacional.sqlite data/
```

A credencial de service account (`sa-candidato-ae-diego.json`) **não deve
ser commitada** — está no `.gitignore`. Aponte `GOOGLE_APPLICATION_CREDENTIALS`
no `.env` para o caminho local do arquivo.

## Rodando o pipeline

```bash
# Ingestão do SQLite (clientes, produtos, itens_pedido) — standalone por
# enquanto, vira asset do Dagster na etapa de orquestração
python scripts/ingest_sqlite.py

# Dagster UI (ainda sem assets registrados)
dagster dev -f src/vena_pipeline/dagster_defs/definitions.py

# dbt (staging + mart) — próxima etapa
cd dbt/vena_pipeline && dbt build

# Testes unitários
pytest
```

### Ingestão do SQLite — decisões desta etapa

- **Sem `LIMIT/OFFSET`:** o generator de `itens_pedido` usa um único cursor
  `sqlite3` com `fetchmany`, mantendo a posição entre chamadas. Paginação via
  `OFFSET` re-varre a tabela do zero a cada página — O(n²) em 5M linhas seria
  proibitivo.
- **Raw fiel à fonte:** nenhuma limpeza/tipagem acontece na extração
  (`preco_tabela` continua texto, `data_item` continua string). Isso é
  responsabilidade da camada staging (dbt, próxima etapa).
- **Idempotência via `WRITE_TRUNCATE`:** como o SQLite é um dump completo
  (sem coluna de watermark confiável para CDC), cada execução sobrescreve a
  tabela raw correspondente em vez de fazer append — rodar duas vezes
  seguidas não duplica dado. Detalhes em `ingestion/loaders.py`.
- Validado contra o arquivo real de 5M linhas: pico de memória de ~240MB
  processando lotes de 100k linhas (bem abaixo do que seria carregar a
  tabela inteira em pandas).

## Uso de IA no desenvolvimento

> Seção obrigatória pelo teste — preenchida ao final, com o detalhamento
> real de ferramentas, metodologia e o que foi revisado/corrigido
> manualmente.

## Decisões de arquitetura e trade-offs

Ver `docs/architecture.md`.
