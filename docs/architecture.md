# Arquitetura

## Visão geral do fluxo

```mermaid
flowchart LR
    subgraph Fontes
        A1[API de vendas<br/>Cloud Run]
        A2[Scraping concorrentes<br/>Cloud Run]
        A3[(SQLite<br/>banco transacional)]
    end

    subgraph Ingestao [Ingestão - Dagster assets]
        B1[raw_api_vendas<br/>retry/backoff]
        B2[raw_scraping_concorrentes<br/>parsing defensivo]
        B3[raw_clientes / raw_produtos]
        B4[raw_itens_pedido<br/>chunks -> Parquet]
    end

    subgraph GCS [GCS - landing]
        G[(gs://vena-teste-candidato-ae-diego)]
    end

    subgraph BQ [BigQuery - vena-teste.teste_tecnico_ae_diego]
        R[(raw_*)]
        S[(stg_* - dbt)]
        M[(mart_saude_comercial - dbt)]
        SN[(snapshot clientes - SCD2)]
    end

    D[Dashboard saúde comercial]

    A1 --> B1 --> G --> R
    A2 --> B2 --> G --> R
    A3 --> B3 --> R
    A3 --> B4 --> G --> R
    R --> S --> M --> D
    S --> SN
```

## Decisões de arquitetura (a documentar conforme o projeto avança)

- **Por que dbt para staging/mart em vez de SQL solto orquestrado direto no
  Dagster:** dedup, tipagem, SCD2 (snapshot) e testes de qualidade nativos,
  com lineage explícito. O Dagster orquestra o `dbt build` como um asset,
  preservando as dependências reais entre camadas no grafo.
- **Por que Parquet particionado em GCS antes do load para o BigQuery raw
  (e não INSERT linha a linha):** evita materializar `itens_pedido`
  (5M linhas) inteiro em memória e permite load incremental/idempotente.
- **Por que retry/backoff via `tenacity` na API e parsing defensivo por
  registro no scraping:** as duas fontes têm falhas propositais (rate
  limit/500 intermitente e schema drift) — o pipeline não pode falhar por
  completo por causa de uma página ou request pontual.

> Seções de trade-offs específicos de cada etapa (ingestão do SQLite, API,
> scraping, modelagem dbt, orquestração Dagster) serão preenchidas conforme
> cada uma for implementada.
