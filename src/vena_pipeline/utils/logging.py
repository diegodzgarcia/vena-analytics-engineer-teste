"""
Logging estruturado (JSON) usado por todos os assets de ingestão.

Cada linha de log carrega campos consistentes (stage, source, rows_processed,
error_rate, etc.) para permitir observabilidade tanto no stdout capturado
pelo Dagster quanto em ferramentas externas, caso o log seja encaminhado.

TODO (próxima etapa — ingestão): usar `get_logger(...)` dentro de cada asset
para reportar métricas de linhas processadas / taxa de erro, conforme
requisito de observabilidade do teste.
"""
from __future__ import annotations

import logging

import structlog

from vena_pipeline.config import settings


def configure_logging() -> None:
    logging.basicConfig(
        format="%(message)s",
        level=settings.log_level,
    )
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
    )


def get_logger(stage: str, source: str) -> structlog.BoundLogger:
    """Logger pré-anexado com o estágio do pipeline e a fonte de dados.

    Uso típico dentro de um asset:
        log = get_logger(stage="ingestion", source="api_vendas")
        log.info("pagina_processada", pagina=3, linhas=500)
    """
    configure_logging()
    return structlog.get_logger().bind(stage=stage, source=source)
