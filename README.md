# Pipeline de Dados — Teste Técnico Analytics Engineer Sênior (Vena BPO)

Pipeline ELT que consolida três fontes heterogêneas (API de vendas, scraping
de preços de concorrentes e banco transacional SQLite) em um dataset
analítico no BigQuery, para alimentar um dashboard diário de "saúde
comercial".

> Status: ingestão do SQLite, camada staging + mart (dbt), ingestão da
> API de vendas e ingestão do scraping implementadas e validadas contra
> o ambiente real. Orquestração Dagster é a próxima etapa. Ver
> `docs/architecture.md` para o diagrama de fluxo e as decisões de
> arquitetura.

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
│   │   ├── api_vendas.py        # ✅ implementado — retry/backoff em 429/500
│   │   └── scraping_concorrentes.py  # ✅ implementado — parsing resiliente a schema drift
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

# Ingestão da API de vendas — retry/backoff em 429/500
python scripts/ingest_api_vendas.py

# Ingestão do scraping — parsing resiliente a schema drift
python scripts/ingest_scraping_concorrentes.py

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

### Bug real encontrado após a etapa 4 (API de vendas) — join point-in-time do mart

Ao inspecionar `mart_saude_comercial` via `bq query` depois de já termos
ingerido a API de vendas, `cliente_nome`/`cidade`/`estado`/`segmento`
apareciam `NULL` em **100% das linhas** — não pegamos isso antes porque os
testes de qualidade do mart cobriam só `item_id`/`data_item`, não os
campos de cliente.

**Causa:** o join point-in-time original exigia estritamente
`data_item >= dbt_valid_from`. Como `itens_pedido` é histórico retroativo
(vendas de 2024) e `dbt_valid_from` da primeira versão de cada cliente é a
hora em que o `dbt snapshot` rodou pela primeira vez (02/09/2026), **toda**
venda é anterior ao início do rastreamento — a condição falhava sempre.

**Correção:** a janela de vigência continua sendo a preferência, mas se
nenhuma versão cobre a data da venda (caso do dado retroativo), o join cai
pra versão mais antiga conhecida como melhor aproximação disponível, em
vez de `NULL` (via `ROW_NUMBER()` com prioridade, não mais um join direto
com `AND` na condição de data). Adicionado
`tests/assert_client_join_coverage.sql` (teste singular) especificamente
pra essa regressão — falha se algum cliente que existe em `stg_clientes`
aparecer com `cliente_nome` nulo no mart (órfãos propositais continuam
nulos e não contam como falha, são excluídos via `INNER JOIN` no teste).

Ainda precisa ser revalidado com `dbt build` no seu ambiente.

### Ingestão da API de vendas — decisões desta etapa

- **Retry com `tenacity`**: 429 (rate limit) e 500 (falha intermitente) são
  tratados como erros transitórios (`TransientAPIError`) e retentados com
  backoff exponencial + jitter. Qualquer outro erro HTTP (401, 404, etc.)
  propaga imediatamente — não é um problema que retry resolve.
- **Throttle proativo entre páginas + orçamento de retry maior — ajustado
  após validar contra a API real**: a primeira versão (sem pausa entre
  páginas, 6 tentativas, backoff até 20s) disparou ~30 requisições em
  menos de 3s sem nenhum intervalo, acionou um 429 persistente, e esgotou
  as tentativas mesmo com o backoff crescendo até ~17s. Corrigido com um
  intervalo de 0.3s entre páginas (evita provocar o rate limit) e um
  orçamento de retry maior (10 tentativas, teto de backoff 60s — pra
  aguentar quando o rate limit acontece mesmo assim). Documentado com
  mais detalhe no docstring de `ingestion/api_vendas.py`.
- **Sem parsing de `Retry-After`**: a especificação do teste não documenta
  esse header sendo enviado pela API; backoff exponencial com jitter é o
  padrão de mercado quando isso não está disponível.
- **`records_to_dataframe()` força string em todo campo — segundo achado
  validado contra a API real**: depois de resolver o rate limit, a
  extração completou (96 páginas, 48.000 registros) mas o load quebrou:
  `valor_unitario` vinha como `462.99` (float) em um pedido e
  `"462.99 BRL"` (texto) em outro — Parquet exige tipo homogêneo por
  coluna. Diferente do SQLite (schema do banco garante tipo consistente
  por coluna), JSON de API é semi-estruturado e pode variar por registro.
  Solução: converter todo valor não-nulo pra string antes de gravar
  (nulos preservados como `None`, não a string `"None"`) — a raw desta
  fonte fica inteiramente em texto; tipagem de verdade é trabalho de uma
  staging futura.
- **Raw fiel à fonte, com uma exceção estrutural**: assim como no SQLite,
  não há limpeza/tipagem de campo aqui além da stringificação acima
  (necessária só pra garantir que o load não quebra). A única defesa
  semântica é estrutural — um registro que não seja um dict válido é
  isolado (logado, contado) em vez de derrubar a página inteira.
- **Fallback de schema**: a extração dos registros tenta primeiro as
  chaves documentadas (`pedidos`, `items`, `data`, `results`); se a API
  usar um nome diferente, cai para a primeira lista encontrada no payload
  em vez de retornar vazio silenciosamente.
- Reaproveita os mesmos `loaders.py` (Parquet no GCS + `WRITE_TRUNCATE` no
  BigQuery) já validados na ingestão do SQLite — mesmo padrão de
  idempotência em toda a camada raw.
- 9 testes unitários cobrindo paginação, retry em 429 e em 500, ausência
  de retry em erro real (401), isolamento de registro malformado,
  fallback de schema, throttle entre páginas, e a stringificação segura
  pro Parquet (incluindo o caso real do `valor_unitario` misto).
  Validado de ponta a ponta contra a API real: 96 páginas, 48.000
  registros extraídos, gravados no GCS e carregados em `raw_pedidos` no
  BigQuery — com múltiplos retries reais acontecendo ao longo da
  execução (429 e 500), todos absorvidos com sucesso pelo orçamento de
  retry configurado.

### Ingestão do scraping — decisões desta etapa

- **Parsing por padrão de texto, não por seletor CSS** — a decisão
  central desta etapa. Uma estratégia baseada em `soup.find(".preco")` ou
  `.select("table.precos tr")` quebra exatamente quando a estrutura muda
  (o problema que o desafio simula de propósito). Em vez disso, o parser
  ancora no **preço** (único campo com formato reconhecível de forma
  confiável em qualquer estrutura de tags) e extrai categoria/status pelo
  texto ao redor dele — funciona igual numa `<table>`, numa lista de
  `<div>`, ou em qualquer outra marcação.
- **Duas estratégias em cascata**: tenta `<table>` primeiro (caminho mais
  direto quando existe); cai pro parsing por padrão de texto se não
  houver tabela. Nunca lança exceção — retorna lista vazia e loga o
  motivo na pior hipótese (página fora do ar, estrutura irreconhecível).
- **Bug real encontrado e corrigido durante o desenvolvimento**: extrair
  categoria e status de forma independente causava sobreposição (o texto
  "no meio" entre dois preços contém o status do item anterior *e* a
  categoria do próximo, ambos começando com maiúscula — uma extração
  ingênua pega os dois juntos, ex. `"Em estoque Acessórios"` em vez de só
  `"Em estoque"`). Corrigido processando o texto sequencialmente: o que
  sobra depois de extrair o status do item N vira a categoria do item
  N+1. Há um teste de regressão específico pra isso.
- **Validado contra o HTML real da página do desafio** (inspecionado
  diretamente, fora deste ambiente de desenvolvimento): 12/12 itens
  extraídos corretamente, incluindo o cabeçalho fixo da página sendo
  descartado (ele também é Título-Case, vazaria pro primeiro item sem
  essa remoção explícita).
- 7 testes unitários cobrindo as duas estratégias, o bug de sobreposição
  acima (regressão), falha de rede, HTTP de erro, e estrutura totalmente
  desconhecida. Validado de ponta a ponta contra a página real: 12
  registros extraídos via `estrategia: text_pattern` — ou seja, a
  execução real caiu numa variante sem `<table>`, confirmando que o
  fallback por padrão de texto funciona fora dos testes mockados também.

## Uso de IA no desenvolvimento

> Seção obrigatória pelo teste — preenchida ao final, com o detalhamento
> real de ferramentas, metodologia e o que foi revisado/corrigido
> manualmente.

## Decisões de arquitetura e trade-offs

Ver `docs/architecture.md`.
