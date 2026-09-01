"""
Assets do Dagster.

Cada fonte de dados vira um asset `raw_*`, e a camada staging/mart é
representada por um asset dbt (via `dagster-dbt`), garantindo dependências
reais no grafo: raw_* -> staging (dbt) -> mart (dbt).

TODO: implementado nas próximas etapas, uma fonte por vez, e só depois
decorado com `@asset` (a lógica de extração vive em `ingestion/*.py` e é
chamada por cada asset — assim ela pode ser testada isoladamente sem subir
o Dagster).
"""
from __future__ import annotations
