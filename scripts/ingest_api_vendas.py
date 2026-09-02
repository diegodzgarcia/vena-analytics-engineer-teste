"""
Script standalone para rodar a ingestão da API de vendas fora do Dagster —
mesmo espírito do scripts/ingest_sqlite.py: código isolado, fácil de
debugar agora, e que vira o corpo de um asset do Dagster na etapa de
orquestração.

Reaproveita os mesmos loaders genéricos (upload_chunk_to_gcs,
load_raw_table_from_gcs) usados na ingestão do SQLite — mesmo padrão de
landing em Parquet no GCS + load com WRITE_TRUNCATE no BigQuery raw, pelos
mesmos motivos de idempotência já documentados em ingestion/loaders.py.

Uso:
    python scripts/ingest_api_vendas.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vena_pipeline.ingestion.api_vendas import (  # noqa: E402
    extract_pedidos,
    records_to_dataframe,
)
from vena_pipeline.ingestion.loaders import (  # noqa: E402
    load_raw_table_from_gcs,
    upload_chunk_to_gcs,
)
from vena_pipeline.utils.logging import get_logger  # noqa: E402

log = get_logger(stage="ingestion", source="api_vendas.orchestration")

TABLE_NAME = "pedidos"


def run() -> None:
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log.info("ingestao_api_vendas_iniciada", run_ts=run_ts)

    records = extract_pedidos()
    df = records_to_dataframe(records)

    upload_chunk_to_gcs(df, TABLE_NAME, chunk_number=1, run_ts=run_ts)
    load_raw_table_from_gcs(TABLE_NAME, run_ts=run_ts)

    log.info(
        "ingestao_api_vendas_concluida", run_ts=run_ts, total_registros=len(df)
    )


if __name__ == "__main__":
    run()
