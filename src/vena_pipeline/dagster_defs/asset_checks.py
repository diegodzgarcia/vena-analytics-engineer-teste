"""
Asset checks — observabilidade exigida pelo teste (métricas como linhas
processadas e taxa de erro).

Reaproveita o `target/run_results.json` que o próprio `dbt build` já
escreve — não precisamos consultar o BigQuery de novo pra saber quantos
testes de qualidade passaram/avisaram/falharam, a informação já está no
artefato que o dbt gera.
"""
from __future__ import annotations

import json
from pathlib import Path

from dagster import AssetCheckResult, MetadataValue, asset_check

from vena_pipeline.dagster_defs.assets import dbt_build

_RUN_RESULTS_PATH = (
    Path(__file__).resolve().parents[3]
    / "dbt"
    / "vena_pipeline"
    / "target"
    / "run_results.json"
)

# Limite tolerado de linhas WARN nos testes de integridade referencial.
# O desafio injeta ~75k/~74k chaves órfãs propositais em 5M linhas
# (~1.5%) -- um número de WARN muito acima disso seria sinal de um
# problema de dado real, diferente do órfão já conhecido e documentado.
_MAX_WARN_ROWS_TOLERATED = 200_000


@asset_check(
    asset=dbt_build,
    description="Taxa de linhas WARN nos testes de integridade referencial do dbt.",
)
def dbt_test_warn_rate_within_tolerance() -> AssetCheckResult:
    if not _RUN_RESULTS_PATH.exists():
        return AssetCheckResult(
            passed=False,
            description="run_results.json não encontrado — dbt build pode não ter rodado ainda.",
        )

    data = json.loads(_RUN_RESULTS_PATH.read_text())
    total_warn_rows = sum(
        result.get("failures") or 0
        for result in data.get("results", [])
        if result.get("status") == "warn"
    )

    passed = total_warn_rows <= _MAX_WARN_ROWS_TOLERATED
    return AssetCheckResult(
        passed=passed,
        metadata={
            "total_linhas_warn": MetadataValue.int(total_warn_rows),
            "limite_tolerado": MetadataValue.int(_MAX_WARN_ROWS_TOLERATED),
        },
        description=(
            f"{total_warn_rows} linhas em WARN (limite tolerado: {_MAX_WARN_ROWS_TOLERATED})"
        ),
    )
