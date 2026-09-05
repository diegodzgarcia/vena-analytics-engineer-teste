# Pipeline de Dados — Teste Técnico Analytics Engineer Sênior (Vena BPO)

Pipeline ELT que consolida três fontes heterogêneas (API de vendas, scraping
de preços de concorrentes e banco transacional SQLite) em um dataset
analítico no BigQuery, para alimentar um dashboard diário de "saúde
comercial".

> **Status: completo.** Ingestão (SQLite, API de vendas, scraping),
> staging + mart (dbt), orquestração em Dagster e idempotência — todas
> as etapas implementadas e validadas de ponta a ponta contra o
> ambiente real (`vena-teste`), com o pipeline completo rodado duas
> vezes seguidas para comprovar que nada duplica. Ver
> `docs/architecture.md` para o diagrama de fluxo e as decisões de
> arquitetura, e a seção "Uso de IA no desenvolvimento" abaixo para a
> metodologia completa.

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
│   ├── dagster_defs/            # ✅ implementado — 5 assets + dbt_build + check + schedule
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

# Dagster UI — 5 assets de ingestão + dbt_build, com dependências reais,
# schedule diário, e 1 asset check
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

### Orquestração em Dagster — decisões desta etapa

- **Grafo**: 5 assets de ingestão (`raw_clientes`, `raw_produtos`,
  `raw_itens_pedido`, `raw_pedidos`, `raw_precos_concorrentes`) →
  `dbt_build` (staging + snapshot + mart + testes). Dependência real, não
  só ordem: `dbt_build` declara `deps=[...]` pros 5 assets — o dbt só
  materializa depois que a raw inteira existe no BigQuery.
- **`dbt build` via `subprocess`, não `@dbt_assets`/`DbtCliResource`**: a
  integração "oficial" do pacote `dagster-dbt` cria um asset por MODEL do
  dbt (dependências finas automáticas via manifest.json) — mais elegante,
  mas depende de detalhes de API que variam entre versões e exige gerar o
  manifest com credencial GCP real pra validar, algo que não consigo
  fazer neste ambiente de desenvolvimento. `subprocess` é mais grosseiro
  (1 asset = todo o `dbt build`), mas 100% previsível e testável sem
  depender de nenhuma versão específica de biblioteca. `dagster-gcp` e
  `dagster-dbt` foram removidos do `requirements.txt` por não serem
  usados — decisão documentada em `dagster_defs/resources.py`.
- **Asset com falha proposital**: `raw_precos_concorrentes` trata "zero
  registros extraídos" como falha real (levanta `Failure`), mesmo a
  função de extração em si nunca lançando exceção sozinha (ela é
  resiliente por design, ver etapa do scraping). A lógica: um schema
  totalmente irreconhecível ou a página fora do ar é informação que vale
  a pena parar e escalar, não silenciar. Tem `retry_policy` configurado
  (2 tentativas, 15s de intervalo) — cobre o caso comum (schema drift
  pontual que se resolve na próxima tentativa).
- **`retry_policy` também em `raw_pedidos`** (2 tentativas, 30s): camada
  extra de resiliência acima do retry/backoff que `extract_pedidos()` já
  faz internamente via `tenacity` — se mesmo assim a extração falhar de
  vez, o Dagster tenta o asset inteiro de novo.
- **Asset check reaproveitando `run_results.json`**: em vez de consultar
  o BigQuery de novo pra saber quantos testes do dbt passaram/avisaram/
  falharam, o check lê o artefato que o próprio `dbt build` já escreve —
  menos código, menos pontos de falha, e a métrica (taxa de linhas em
  WARN nos testes de integridade referencial) fica visível na UI do
  Dagster, não só no log do terminal.
- **Schedule diário (06:00, `America/Sao_Paulo`)**: alimenta o "dashboard
  diário de saúde comercial" citado no enunciado — roda de madrugada,
  antes do horário comercial.
- **Windows: `PYTHONLEGACYWINDOWSSTDIO=1` necessário antes do `dagster
  dev`** — sem essa variável, a materialização de qualquer asset trava
  indefinidamente (subprocess nunca retorna). É um problema conhecido de
  captura de I/O de subprocessos no Windows, não específico deste
  projeto. Documentado aqui porque custou tempo real de debug.
- **Limitação conhecida de observabilidade**: os logs estruturados
  (`structlog`) da camada de ingestão não aparecem no stderr capturado
  pela UI do Dagster — só os eventos internos do próprio Dagster
  (`STEP_START`, `STEP_OUTPUT`, etc.) aparecem lá. Os logs do
  `structlog` continuam funcionando normalmente fora do Dagster (scripts
  standalone). Correção natural pra uma próxima iteração: usar
  `context.log` do Dagster dentro dos assets em vez de (ou além de)
  `get_logger()`, ou configurar um handler que redireciona `structlog`
  pro logger do Dagster.
- **Validado de ponta a ponta contra o BigQuery/GCS real, via
  materialização de cada asset individualmente na UI do Dagster**:
  - `raw_clientes`: 6.180 linhas
  - `raw_produtos`: 800 linhas
  - `raw_itens_pedido`: 5.000.000 linhas em 50 chunks (1m41s)
  - `raw_pedidos`: 48.000 linhas (4m7s — tempo bem acima do normal,
    indício de retry real por rate limit absorvido com sucesso)
  - `raw_precos_concorrentes`: 12 linhas
  - `dbt_build`: 17 PASS, 2 WARN, 0 ERROR, 19 nodes totais
  - Asset check `dbt_test_warn_rate_within_tolerance`: **PASSOU**,
    149.840 linhas em WARN (74.639 + 75.201, as duas chaves órfãs
    somadas) — bem dentro do limite de 200.000 configurado.

### Idempotência — validação desta etapa

Rodei o pipeline completo duas vezes seguidas (via "Materialize all" no
Dagster) e comparei o estado do BigQuery antes/depois:

| Tabela | Antes | Depois |
| --- | --- | --- |
| raw_clientes | 6.180 | 6.180 |
| raw_produtos | 800 | 800 |
| raw_itens_pedido | 5.000.000 | 5.000.000 |
| raw_pedidos | 48.000 | 48.000 |
| raw_precos_concorrentes | 12 | 13 |
| **clientes_snapshot** | **6.168** | **6.168** |
| mart_saude_comercial | 5.000.000 | 5.000.000 |

Todas as tabelas raw ficaram idênticas — esperado, `WRITE_TRUNCATE`
garante isso por construção (ver `ingestion/loaders.py`). A única
variação (`raw_precos_concorrentes`: 12 → 13) não é falha de
idempotência: a API de scraping gera dados aleatórios a cada chamada, é
a fonte mudando de conteúdo entre as duas execuções, não o pipeline
duplicando nada.

O número mais importante da tabela é o `clientes_snapshot` continuar
**exatamente em 6.168** nas duas rodadas. Se a estratégia `check` do SCD2
estivesse comparando errado (achando "mudança" onde não tem), esse
número teria praticamente dobrado (uma segunda versão por cliente) — ele
não mudou, confirmando que rodar o pipeline duas vezes seguidas não
duplica nada, nem na raw nem no histórico versionado.

## Uso de IA no desenvolvimento

**Ferramenta usada:** Claude (Anthropic), via chat, do início ao fim do
desafio, desde a leitura inicial dos arquivos do teste até a última
etapa. Não usei Copilot, Cursor ou ferramentas de IA embutidas no editor
neste projeto sendo toda a interação feita de maneira conversacional.

**Metodologia/fluxo de trabalho:**

Não foi spec-first (não escrevi um plano detalhado e pedi pra IA
implementar tudo de uma vez) nem TDD assistido no sentido estrito. O
fluxo real foi interativo, etapa por etapa, com validação contra o
ambiente real antes de avançar:

1. Definimos juntos a ordem de execução das 7 etapas no início (do que
   eu já dominava (SQL, BigQuery, dbt Cloud) para oque era novo pra mim
   (Dagster, dbt Core via CLI)), e seguimos essa ordem do início ao fim.
2. Para cada etapa: a IA implementava o código com testes unitários
   mockados (sem acesso ao meu ambiente GCP real), eu copiava os
   arquivos pro meu projeto local, rodava os testes (`pytest`) e depois
   validava de verdade contra o BigQuery/GCS/APIs reais na minha
   máquina para então avançar pra próxima etapa.
3. Cada bug encontrado na validação real virava um ciclo de
   diagnóstico → correção → nova validação, documentado no lugar da
   etapa correspondente neste README.

**Como o código gerado foi revisado:** rodando de verdade contra o
ambiente real (não só lendo o código), foi exatamente esse processo que
expôs a maioria dos bugs abaixo, que não teriam aparecido só olhando o
código ou rodando testes mockados.

**Casos em que a IA "alucinou" ou o código gerado estava errado, e como
foi percebido/corrigido:**

- **`from __future__ import annotations` quebrando o Dagster**: o
  parâmetro `context` de cada asset passou a ser validado como string em
  vez do tipo de verdade (efeito colateral do PEP 563 que a IA não tinha
  previsto). Percebido rodando `dagster asset list` localmente antes de
  eu sequer testar, corrigido antes de eu perder tempo com isso.
- **Join point-in-time do mart retornando `NULL` em 100% das linhas**:
  o código compilava e os testes automatizados passavam, mas a lógica
  de negócio estava errada e o teste de qualidade do dbt cobria só
  unicidade/não-nulo das chaves, não os campos de cliente enriquecidos.
  Eu que encontrei isso, inspecionando os dados de verdade via
  `bq query` por curiosidade própria, não porque algum teste apontou.
- **Rate limit da API mais agressivo do que a primeira versão do
  retry aguentava**: só apareceu rodando contra a API real (48 mil
  registros, ~30 páginas até bater no limite), testes mockados não
  captam esse tipo de comportamento de ambiente real.
- **Tipo misto no Parquet** (`valor_unitario` como número em um pedido,
  texto com unidade em outro): mesma categoria só rodando contra dado
  real de verdade.
- **"Src layout" do projeto não resolvido pelo Dagster**: a IA validou
  contra `pytest` (que usa `pytest.ini`) mas não tinha como testar
  `dagster dev` de verdade no ambiente dela — só apareceu quando rodei
  na minha máquina.
- **Travamento no Windows sem `PYTHONLEGACYWINDOWSSTDIO=1`**: um
  problema de ambiente puramente meu (Windows + captura de subprocesso),
  que a IA não tinha como prever nem reproduzir, mas ajudou a
  diagnosticar por eliminação quando relatei o sintoma (travamento sem
  erro, sem consumo de CPU).
- **Diretórios temporários do Dagster commitados por engano**: erro
  meu, no fluxo de Git passou despercebido no `git add .` antes de eu
  conferir o `git status` com atenção.

**O que eu não deleguei para a IA:**

- **Toda execução real de código** contra BigQuery, GCS e as APIs do
  desafio a IA nunca teve (nem pode ter) acesso à minha credencial
  (`sa-candidato-ae-diego.json`). Rodei cada script, cada `dbt build`,
  cada materialização no Dagster, e colei os resultados de volta.
- **Toda a operação de Git** (branches, commits, PRs, merges) feitas
  por mim, comando por comando, no meu terminal.
- **A decisão de quando algo estava "bom o suficiente" pra avançar**
  por exemplo, insisti em rodar `dbt build` contra o ambiente real antes
  de seguir pra próxima etapa em vez de aceitar só a validação sintática
  (`dbt parse`) que era tudo que a IA conseguia fazer sozinha.
- **A inspeção exploratória dos dados** (queries `bq` por curiosidade,
  não só pra validar algo específico), foi assim que encontrei o bug do
  join do mart, que nenhum teste automatizado tinha pego.

## Decisões de arquitetura e trade-offs

Ver `docs/architecture.md`.
