"""
Ponto de entrada do Dagster (`dagster dev -f src/vena_pipeline/dagster_defs/definitions.py`).

Vazio de propósito nesta etapa — o esqueleto só declara onde cada peça vai
entrar. Assets, resources, schedules/sensors e asset checks são adicionados
nas próximas etapas (ingestão de cada fonte, e por fim a etapa de
orquestração, que costura as dependências reais entre eles).
"""
from __future__ import annotations

from dagster import Definitions

# TODO (etapas de ingestão): importar os assets de cada fonte, ex.:
# from vena_pipeline.dagster_defs.assets import (
#     raw_api_vendas,
#     raw_scraping_concorrentes,
#     raw_itens_pedido,
#     raw_clientes,
#     raw_produtos,
# )

# TODO (etapa de orquestração): resources (BigQuery, GCS), schedules/sensors,
# retry_policy no asset da API, e os asset checks de observabilidade.

defs = Definitions(
    assets=[],
    resources={},
    schedules=[],
    sensors=[],
)
