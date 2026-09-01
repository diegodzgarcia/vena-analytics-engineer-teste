"""
Script standalone para rodar a ingestão do SQLite (clientes, produtos,
itens_pedido) fora do Dagster — útil para desenvolvimento e debug isolado
de cada etapa antes de existir orquestração.

Quando a etapa de orquestração for implementada, os assets do Dagster vão
chamar exatamente essas mesmas funções (`extract_*` + `upload_chunk_to_gcs`
+ `load_raw_table_from_gcs`) — este script não é descartado, vira o corpo
de cada asset.

Uso:
    python scripts/ingest_sqlite.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vena_pipeline.ingestion.loaders import load_raw_table_from_gcs, upload_chunk_to_gcs  # noqa: E402
from vena_pipeline.ingestion.sqlite_extract import (  # noqa: E402
    extract_itens_pedido_chunks,
    extract_small_table,
)
from vena_pipeline.utils.logging import get_logger  # noqa: E402

log = get_logger(stage="ingestion", source="sqlite.orchestration")


def _new_run_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ingest_small_tables(run_ts: str) -> None:
    for table_name in ("clientes", "produtos"):
        df = extract_small_table(table_name)
        upload_chunk_to_gcs(df, table_name, chunk_number=1, run_ts=run_ts)
        load_raw_table_from_gcs(table_name, run_ts=run_ts)


def ingest_itens_pedido(run_ts: str) -> int:
    total_chunks = 0
    for chunk_number, df_chunk in enumerate(extract_itens_pedido_chunks(), start=1):
        upload_chunk_to_gcs(df_chunk, "itens_pedido", chunk_number=chunk_number, run_ts=run_ts)
        total_chunks = chunk_number

    load_raw_table_from_gcs("itens_pedido", run_ts=run_ts)
    return total_chunks


def run() -> None:
    run_ts = _new_run_ts()
    log.info("ingestao_sqlite_iniciada", run_ts=run_ts)

    ingest_small_tables(run_ts)
    total_chunks = ingest_itens_pedido(run_ts)

    log.info(
        "ingestao_sqlite_concluida",
        run_ts=run_ts,
        total_chunks_itens_pedido=total_chunks,
    )


if __name__ == "__main__":
    run()
