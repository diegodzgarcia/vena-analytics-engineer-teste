"""
Fábricas de clientes GCP (BigQuery e GCS), reaproveitadas pelos resources
do Dagster e pelos scripts de ingestão.

TODO (próxima etapa — ingestão do SQLite): usar `get_gcs_client()` para o
upload dos arquivos Parquet particionados e `get_bq_client()` para o load
job da camada raw.
"""
from __future__ import annotations

from functools import lru_cache

from google.cloud import bigquery, storage

from vena_pipeline.config import settings


@lru_cache(maxsize=1)
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(
        project=settings.gcp.project_id,
        credentials=_credentials(),
    )


@lru_cache(maxsize=1)
def get_gcs_client() -> storage.Client:
    return storage.Client(
        project=settings.gcp.project_id,
        credentials=_credentials(),
    )


def _credentials():
    from google.oauth2 import service_account

    return service_account.Credentials.from_service_account_file(
        settings.gcp.credentials_path
    )


def raw_table_ref(table_name: str) -> str:
    """Referência completa `projeto.dataset.raw_<tabela>`."""
    return f"{settings.gcp.project_id}.{settings.gcp.dataset}.raw_{table_name}"
