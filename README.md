# Pipeline de Dados — Teste Técnico Analytics Engineer Sênior (Vena BPO)

Pipeline ELT que consolida três fontes heterogêneas (API de vendas, scraping
de preços de concorrentes e banco transacional SQLite) em um dataset
analítico no BigQuery, para alimentar um dashboard diário de "saúde
comercial".

> Status: ingestão do SQLite e camada staging + mart (dbt) implementadas.
> Ingestão da API, do scraping e orquestração Dagster seguem nos próximos
> commits. Ver `docs/architecture.md` para o diagrama de fluxo e as
> decisões de arquitetura.

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
├── dbt/vena_pipeline/
│   ├── models/staging/           # ✅ stg_clientes, stg_produtos, stg_itens_pedido
│   ├── models/marts/             # ✅ mart_saude_comercial
│   ├── snapshots/                # ✅ clientes_snapshot (SCD2)
│   └── packages.yml              # dbt_utils (teste de unicidade composta)
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

# dbt: instalar dependências (dbt_utils), depois rodar staging + snapshot + mart + testes
cd dbt/vena_pipeline
dbt deps
dbt build

# Dagster UI (ainda sem assets registrados)
dagster dev -f src/vena_pipeline/dagster_defs/definitions.py

# Testes unitários (Python)
pytest
```

### Ingestão do SQLite — decisões desta etapa

- **Sem `LIMIT/OFFSET`:** o generator de `itens_pedido` usa um único cursor
  `sqlite3` com `fetchmany`, mantendo a posição entre chamadas. Paginação via
  `OFFSET` re-varre a tabela do zero a cada página — O(n²) em 5M linhas seria
  proibitivo.
- **Raw fiel à fonte:** nenhuma limpeza/tipagem acontece na extração
  (`preco_tabela` continua texto, `data_item` continua string). Isso é
  responsabilidade da camada staging (dbt).
- **Idempotência via `WRITE_TRUNCATE`:** como o SQLite é um dump completo
  (sem coluna de watermark confiável para CDC), cada execução sobrescreve a
  tabela raw correspondente em vez de fazer append — rodar duas vezes
  seguidas não duplica dado. Detalhes em `ingestion/loaders.py`.
- Validado contra o arquivo real de 5M linhas: pico de memória de ~240MB
  processando lotes de 100k linhas (bem abaixo do que seria carregar a
  tabela inteira em pandas).

### Camada staging + mart (dbt) — decisões desta etapa

- **Dedup por `ROW_NUMBER()`** em todas as três staging, mantendo a versão
  mais recente por `_ingested_at` (coluna de auditoria adicionada na
  ingestão). Não há informação na fonte pra arbitrar qual duplicata é "a
  certa" além disso.
- **`preco_tabela` (texto → NUMERIC) e `ativo` (5 valores → BOOLEAN)** em
  `stg_produtos`: valores que não batem com nenhum padrão conhecido viram
  `NULL` em vez de quebrar o build inteiro.
- **`stg_itens_pedido` materializada como TABLE particionada por
  `data_item`** (não `VIEW`, o default do projeto): 5M linhas recalculadas
  a cada query do mart seria caro. Também não usamos incremental aqui — a
  raw é recarregada por completo a cada execução (`WRITE_TRUNCATE`), então
  não existe delta real pra aproveitar.
- **`clientes_snapshot` (SCD2, strategy=`check`)**: a fonte não tem uma
  coluna de "última atualização" confiável (`data_cadastro` é a data de
  cadastro, não de alteração), então a estratégia de detecção de mudança
  compara diretamente as colunas de negócio (`nome`, `cidade`, `estado`,
  `segmento`). Roda sobre `stg_clientes` (já deduplicado), não sobre a raw.
- **Join point-in-time no mart:** `mart_saude_comercial` resolve o cliente
  de cada venda usando `data_item` contra o intervalo de vigência do
  snapshot (`dbt_valid_from`/`dbt_valid_to`), não o estado atual do
  cliente — é exatamente pra isso que o SCD2 existe: uma venda de janeiro
  reflete o segmento que o cliente tinha em janeiro, mesmo que ele tenha
  mudado de segmento depois.
- **Testes de integridade referencial com `severity: warn`** (não `error`)
  em `cliente_id`/`produto_id` de `stg_itens_pedido`: o desafio injeta
  chaves órfãs de propósito (~75k linhas). O objetivo do teste aqui é
  medir/expor a taxa de órfãos, não travar o build inteiro por causa de um
  problema de dado já conhecido e documentado.
- Validado com `dbt build` real contra o BigQuery (`vena-teste`): 16 PASS,
  2 WARN (as ~75k/~74k chaves órfãs propositais do desafio — números batem
  exatamente com o profiling inicial dos dados), 0 ERROR.

## Uso de IA no desenvolvimento

> Seção obrigatória pelo teste — preenchida ao final, com o detalhamento
> real de ferramentas, metodologia e o que foi revisado/corrigido
> manualmente.

## Decisões de arquitetura e trade-offs

Ver `docs/architecture.md`.
