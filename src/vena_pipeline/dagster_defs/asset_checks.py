"""
Asset checks — observabilidade e o "asset com falha proposital" exigido
pelo teste (ex.: taxa de erro de parsing do scraping acima de um threshold,
ou linhas órfãs além do esperado na ingestão do SQLite).

TODO (etapa de orquestração): implementar com `@asset_check`, reportando
métricas (linhas processadas, taxa de erro) como `AssetCheckResult.metadata`.
"""
from __future__ import annotations
