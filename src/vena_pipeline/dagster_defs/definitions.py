"""
Ponto de entrada do Dagster.

    dagster dev -f src/vena_pipeline/dagster_defs/definitions.py

Grafo: 5 assets de ingestão (raw_clientes, raw_produtos, raw_itens_pedido,
raw_pedidos, raw_precos_concorrentes) -> dbt_build (staging + snapshot +
mart + testes), com 1 asset check em cima do dbt_build.

Schedule diário: alimenta o "dashboard diário de saúde comercial" citado
no enunciado do teste -- roda de madrugada, antes do horário comercial,
pra dados frescos estarem prontos quando o time de vendas chegar.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Mesmo ajuste usado em scripts/ingest_*.py: o projeto usa "src layout"
# (o pacote vena_pipeline vive dentro de src/, não na raiz), então
# precisa entrar explicitamente no sys.path. O pytest resolve isso sozinho
# via pytest.ini (pythonpath = src), mas o Dagster não lê esse arquivo —
# sem esta linha, `dagster dev -f definitions.py` falha com
# "ModuleNotFoundError: No module named 'vena_pipeline'".
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dagster import Definitions, ScheduleDefinition, define_asset_job

from vena_pipeline.dagster_defs.asset_checks import dbt_test_warn_rate_within_tolerance
from vena_pipeline.dagster_defs.assets import (
    dbt_build,
    raw_clientes,
    raw_itens_pedido,
    raw_pedidos,
    raw_precos_concorrentes,
    raw_produtos,
)

pipeline_job = define_asset_job(name="vena_pipeline_job", selection="*")

daily_schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/Sao_Paulo",
)

defs = Definitions(
    assets=[
        raw_clientes,
        raw_produtos,
        raw_itens_pedido,
        raw_pedidos,
        raw_precos_concorrentes,
        dbt_build,
    ],
    asset_checks=[dbt_test_warn_rate_within_tolerance],
    jobs=[pipeline_job],
    schedules=[daily_schedule],
)
