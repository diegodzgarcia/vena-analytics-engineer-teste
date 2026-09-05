"""
Resources do Dagster.

Vazio de propósito. A dependência natural aqui seria um `DbtCliResource`
(do pacote `dagster-dbt`) injetado no asset `dbt_build` — mas esse asset
chama `dbt build` via `subprocess` direto, sem usar `DbtCliResource`. Ver
a docstring de `assets.py::dbt_build` para o motivo (evitar depender de
detalhes de API do `dagster-dbt` que eu não conseguiria validar sem
acesso à sua service account real).

Os clientes GCP (BigQuery/GCS) também não viram `ConfigurableResource`
aqui — os assets chamam direto as funções de `ingestion/loaders.py`, que
já encapsulam esses clientes (`utils/gcp.py`) e já foram testadas
isoladamente nas etapas anteriores. Introduzir mais uma camada de resource
do Dagster por cima disso seria indireção sem ganho real neste projeto.
"""
from __future__ import annotations
