"""
Resources do Dagster (clientes BigQuery/GCS injetados nos assets).

TODO (etapa de orquestração): envolver `utils.gcp.get_bq_client()` /
`get_gcs_client()` como `ConfigurableResource`, e o projeto dbt como
`DbtCliResource` (via `dagster-dbt`), apontando para `dbt/vena_pipeline`.
"""
from __future__ import annotations
