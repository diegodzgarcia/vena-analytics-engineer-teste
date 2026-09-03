"""
Script standalone para rodar a ingestão do scraping de concorrentes fora
do Dagster — mesmo espírito dos outros dois scripts de ingestão.

Reaproveita os mesmos loaders genéricos (upload_chunk_to_gcs,
load_raw_table_from_gcs) usados nas outras duas fontes. Diferente delas,
usa `records_to_dataframe` do módulo da API de vendas (não é específico
da API — o nome só reflete onde foi criado primeiro) porque o resultado
do scraping também é semi-estruturado: se a próxima execução pegar uma
estrutura de página diferente, os campos podem variar de tipo/presença
entre registros, mesmo risco que motivou essa função na ingestão da API.

Uso:
    python scripts/ingest_scraping_concorrentes.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vena_pipeline.ingestion.api_vendas import records_to_dataframe  # noqa: E402
from vena_pipeline.ingestion.loaders import (  # noqa: E402
    load_raw_table_from_gcs,
    upload_chunk_to_gcs,
)
from vena_pipeline.ingestion.scraping_concorrentes import (  # noqa: E402
    extract_precos_concorrentes,
)
from vena_pipeline.utils.logging import get_logger  # noqa: E402

log = get_logger(stage="ingestion", source="scraping_concorrentes.orchestration")

TABLE_NAME = "precos_concorrentes"


def run() -> None:
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log.info("ingestao_scraping_iniciada", run_ts=run_ts)

    records = extract_precos_concorrentes()
    if not records:
        log.warning(
            "ingestao_scraping_sem_registros",
            run_ts=run_ts,
            aviso="Nenhum registro extraído — página fora do ar ou schema não reconhecido. "
            "Não é tratado como erro fatal (ver docstring de scraping_concorrentes.py), "
            "mas o load é pulado.",
        )
        return

    df = records_to_dataframe(records)
    upload_chunk_to_gcs(df, TABLE_NAME, chunk_number=1, run_ts=run_ts)
    load_raw_table_from_gcs(TABLE_NAME, run_ts=run_ts)

    log.info(
        "ingestao_scraping_concluida", run_ts=run_ts, total_registros=len(df)
    )


if __name__ == "__main__":
    run()
