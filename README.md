# Pipeline de Dados — Teste Técnico Analytics Engineer Sênior (Vena BPO)

Pipeline ELT que consolida três fontes heterogêneas (API de vendas, scraping
de preços de concorrentes e banco transacional SQLite) em um dataset
analítico no BigQuery, para alimentar um dashboard diário de "saúde
comercial".

> Status: **esqueleto do projeto** — estrutura, configuração e contratos
> definidos. A lógica de cada etapa (ingestão, modelagem dbt, orquestração
> Dagster) é implementada nos commits seguintes. Ver `docs/architecture.md`
> para o diagrama de fluxo e as decisões de arquitetura.

## Estrutura do repositório

```
.
├── docs/
│   └── architecture.md        # diagrama do fluxo + decisões de arquitetura
├── src/vena_pipeline/
│   ├── config.py               # configuração central (lê .env)
│   ├── ingestion/               # extração de cada fonte (raw)
│   │   ├── api_vendas.py
│   │   ├── scraping_concorrentes.py
│   │   └── sqlite_extract.py
│   ├── dagster_defs/            # orquestração
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
# Dagster UI
dagster dev -f src/vena_pipeline/dagster_defs/definitions.py

# dbt (staging + mart)
cd dbt/vena_pipeline && dbt build

# Testes unitários
pytest
```

*(comandos ficam completos conforme as etapas de ingestão/orquestração são
implementadas — no momento os assets do Dagster ainda não existem.)*

## Uso de IA no desenvolvimento

> Seção obrigatória pelo teste — preenchida ao final, com o detalhamento
> real de ferramentas, metodologia e o que foi revisado/corrigido
> manualmente.

## Decisões de arquitetura e trade-offs

Ver `docs/architecture.md`.
