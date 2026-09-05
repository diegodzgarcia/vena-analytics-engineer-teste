"""
Assets do Dagster.

Cinco assets de ingestão (um por fonte/tabela raw) + um asset `dbt_build`
que depende de todos eles — a dependência real exigida pelo teste: o dbt
só roda depois que a raw inteira está carregada.

Cada asset de ingestão só chama as funções já implementadas e testadas
nas etapas anteriores (`ingestion/*.py` + `ingestion/loaders.py`) — o
Dagster aqui é orquestração pura, não lógica nova. Isso é proposital: a
lógica de negócio já foi validada isoladamente (pytest + execução real
contra GCP), então o risco fica concentrado só na "cola" de orquestração.
"""
# Nota: propositalmente SEM `from __future__ import annotations` aqui —
# diferente dos outros módulos do projeto. O Dagster faz introspecção de
# tipo em runtime pra validar o parâmetro `context` de cada asset (precisa
# ser `AssetExecutionContext` de verdade, não a STRING "AssetExecutionContext"
# que a anotação adiada do PEP 563 produziria). Como o projeto já roda em
# Python 3.12, `dict[str, Any]` funciona nativamente sem o future import.
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from vena_pipeline.ingestion.api_vendas import extract_pedidos, records_to_dataframe
from vena_pipeline.ingestion.loaders import load_raw_table_from_gcs, upload_chunk_to_gcs
from vena_pipeline.ingestion.scraping_concorrentes import extract_precos_concorrentes
from vena_pipeline.ingestion.sqlite_extract import (
    extract_itens_pedido_chunks,
    extract_small_table,
)

_DBT_PROJECT_DIR = Path(__file__).resolve().parents[3] / "dbt" / "vena_pipeline"


def _new_run_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# --- Fonte 3: SQLite (clientes, produtos, itens_pedido) ---------------------


@asset(group_name="raw", description="Clientes extraídos do SQLite (raw_clientes no BigQuery).")
def raw_clientes(context: AssetExecutionContext) -> MaterializeResult:
    run_ts = _new_run_ts()
    df = extract_small_table("clientes")
    upload_chunk_to_gcs(df, "clientes", chunk_number=1, run_ts=run_ts)
    load_raw_table_from_gcs("clientes", run_ts=run_ts)
    return MaterializeResult(metadata={"linhas": MetadataValue.int(len(df)), "run_ts": run_ts})


@asset(group_name="raw", description="Produtos extraídos do SQLite (raw_produtos no BigQuery).")
def raw_produtos(context: AssetExecutionContext) -> MaterializeResult:
    run_ts = _new_run_ts()
    df = extract_small_table("produtos")
    upload_chunk_to_gcs(df, "produtos", chunk_number=1, run_ts=run_ts)
    load_raw_table_from_gcs("produtos", run_ts=run_ts)
    return MaterializeResult(metadata={"linhas": MetadataValue.int(len(df)), "run_ts": run_ts})


@asset(
    group_name="raw",
    description="Itens de pedido extraídos do SQLite em chunks (5M linhas, raw_itens_pedido).",
)
def raw_itens_pedido(context: AssetExecutionContext) -> MaterializeResult:
    run_ts = _new_run_ts()
    total_linhas = 0
    total_chunks = 0

    for chunk_number, df_chunk in enumerate(extract_itens_pedido_chunks(), start=1):
        upload_chunk_to_gcs(df_chunk, "itens_pedido", chunk_number=chunk_number, run_ts=run_ts)
        total_linhas += len(df_chunk)
        total_chunks = chunk_number
        context.log.info(f"chunk {chunk_number} gravado ({len(df_chunk)} linhas)")

    load_raw_table_from_gcs("itens_pedido", run_ts=run_ts)

    return MaterializeResult(
        metadata={
            "linhas": MetadataValue.int(total_linhas),
            "chunks": MetadataValue.int(total_chunks),
            "run_ts": run_ts,
        }
    )


# --- Fonte 1: API de vendas --------------------------------------------------


@asset(
    group_name="raw",
    description="Pedidos extraídos da API de vendas (raw_pedidos no BigQuery).",
    # Camada extra de resiliência: extract_pedidos() já tem retry/backoff
    # interno (tenacity) pra 429/500 por requisição. Este retry_policy é
    # do Dagster, no nível do ASSET inteiro — se mesmo assim a extração
    # falhar de vez (orçamento de retry do tenacity esgotado, ou qualquer
    # outra falha), o Dagster tenta rodar o asset inteiro de novo depois
    # de um intervalo, em vez de simplesmente marcar como falho.
    retry_policy=RetryPolicy(max_retries=2, delay=30),
)
def raw_pedidos(context: AssetExecutionContext) -> MaterializeResult:
    run_ts = _new_run_ts()
    records = extract_pedidos()
    df = records_to_dataframe(records)
    upload_chunk_to_gcs(df, "pedidos", chunk_number=1, run_ts=run_ts)
    load_raw_table_from_gcs("pedidos", run_ts=run_ts)
    return MaterializeResult(metadata={"linhas": MetadataValue.int(len(df)), "run_ts": run_ts})


# --- Fonte 2: Scraping de concorrentes --------------------------------------


@asset(
    group_name="raw",
    description="Preços de concorrentes extraídos via scraping (raw_precos_concorrentes).",
    # Asset com falha proposital exigido pelo teste: extract_precos_concorrentes()
    # nunca lança exceção sozinha (é resiliente por design — ver docstring
    # do módulo), mas AQUI, no nível do asset, tratamos "zero registros
    # extraídos" como uma falha real que vale a pena parar e tentar de
    # novo — página fora do ar ou schema irreconhecível de verdade é
    # informação que o time deveria saber, não silenciar. retry_policy
    # cobre o caso comum (schema drift pontual que se resolve sozinho na
    # próxima tentativa); se persistir após as tentativas, falha de vez e
    # fica visível na UI do Dagster.
    retry_policy=RetryPolicy(max_retries=2, delay=15),
)
def raw_precos_concorrentes(context: AssetExecutionContext) -> MaterializeResult:
    run_ts = _new_run_ts()
    records = extract_precos_concorrentes()

    if not records:
        raise Failure(
            description=(
                "Nenhum registro extraído do scraping de concorrentes — "
                "página fora do ar ou estrutura não reconhecida por nenhuma "
                "das duas estratégias de parsing (ver scraping_concorrentes.py)."
            )
        )

    df = records_to_dataframe(records)
    upload_chunk_to_gcs(df, "precos_concorrentes", chunk_number=1, run_ts=run_ts)
    load_raw_table_from_gcs("precos_concorrentes", run_ts=run_ts)
    return MaterializeResult(metadata={"linhas": MetadataValue.int(len(df)), "run_ts": run_ts})


# --- dbt: staging + snapshot + mart -----------------------------------------


def _parse_dbt_run_results(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"pass": None, "warn": None, "error": None, "total": None}

    data = json.loads(path.read_text())
    statuses = [r.get("status") for r in data.get("results", [])]
    return {
        "pass": statuses.count("pass") + statuses.count("success"),
        "warn": statuses.count("warn"),
        "error": statuses.count("error") + statuses.count("fail"),
        "total": len(statuses),
    }


@asset(
    group_name="transform",
    deps=[raw_clientes, raw_produtos, raw_itens_pedido, raw_pedidos, raw_precos_concorrentes],
    description=(
        "Roda `dbt build` (staging + snapshot SCD2 + mart + testes). Depende dos "
        "5 assets de ingestão -- dependência real, não é só ordem de execução: "
        "as tabelas raw precisam existir no BigQuery antes do dbt rodar."
    ),
)
def dbt_build(context: AssetExecutionContext) -> MaterializeResult:
    """
    Decisão de design: chama `dbt build` via subprocess, não via
    `@dbt_assets`/`DbtCliResource` (a integração "oficial" do pacote
    `dagster-dbt`, que criaria um asset do Dagster por MODEL do dbt, com
    dependências finas automáticas geradas a partir do manifest.json).

    Motivo: `@dbt_assets` depende de detalhes de API que variam entre
    versões do `dagster-dbt` e exige gerar o manifest com credencial GCP
    real pra validar -- algo que não consigo fazer neste ambiente de
    desenvolvimento (sem acesso à sua service account). `subprocess` é
    mais grosseiro (1 asset = todo o `dbt build`, não 1 por model), mas é
    100% previsível, testável sem depender de nenhuma versão específica
    de biblioteca, e a dependência real entre ingestão e transformação
    continua existindo no grafo (`deps=[...]` acima). Ampliar pra
    `@dbt_assets` granular é a evolução natural, documentada no README,
    não implementada aqui por esse motivo.
    """
    result = subprocess.run(
        ["dbt", "build"],
        cwd=_DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.stderr:
        context.log.warning(result.stderr)

    summary = _parse_dbt_run_results(_DBT_PROJECT_DIR / "target" / "run_results.json")

    if result.returncode != 0:
        raise Failure(
            description=f"dbt build falhou (returncode={result.returncode}). Ver logs do asset.",
            metadata={k: MetadataValue.int(v) if v is not None else MetadataValue.text("N/A") for k, v in summary.items()},
        )

    return MaterializeResult(
        metadata={
            "dbt_pass": MetadataValue.int(summary["pass"] or 0),
            "dbt_warn": MetadataValue.int(summary["warn"] or 0),
            "dbt_error": MetadataValue.int(summary["error"] or 0),
            "dbt_total_nodes": MetadataValue.int(summary["total"] or 0),
        }
    )
