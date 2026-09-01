"""
Ingestão da Fonte 1 — API de vendas (paginada, com rate limit e falhas
intermitentes simuladas).

Contrato: `extract_pedidos()` deve retornar todos os pedidos paginados,
tratando 429 (rate limit) e 500 (falha intermitente) com retry/backoff,
e ser resiliente a campos nulos/inconsistentes no payload.

TODO (próxima etapa — Ingestão da API de vendas):
- Cliente HTTP com `tenacity` (retry exponencial em 429/500, respeitando
  eventual header Retry-After).
- Paginação até `has_next=False`.
- Parsing defensivo por registro (não derrubar o lote inteiro por causa de
  um item malformado — logar e isolar).
- Persistir raw em GCS (JSON/Parquet) antes do load para BigQuery.
"""
from __future__ import annotations

from typing import Any


def extract_pedidos() -> list[dict[str, Any]]:
    raise NotImplementedError("Implementar na etapa de ingestão da API de vendas")
