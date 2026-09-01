"""
Ingestão da Fonte 2 — Web scraping de preços de concorrentes.

A estrutura do HTML muda a cada request (schema drift proposital). O parser
não pode lançar exceção fatal por causa disso — precisa degradar de forma
graciosa (registrar o que conseguiu extrair, logar o que não bateu com
nenhum seletor conhecido).

Contrato: `extract_precos_concorrentes()` retorna uma lista de dicts com,
no mínimo, produto/preço — mesmo que parcial.

TODO (próxima etapa — Ingestão do scraping):
- BeautifulSoup com múltiplas estratégias de seleção (fallback em cascata:
  tenta seletor A, depois B, depois heurística por texto).
- Nunca deixar uma mudança de estrutura derrubar o asset inteiro — registrar
  taxa de linhas não parseadas como métrica de observabilidade.
- Persistir raw (HTML bruto + resultado parseado) em GCS, versionado por
  timestamp, para auditoria/debug do schema drift.
"""
from __future__ import annotations

from typing import Any


def extract_precos_concorrentes() -> list[dict[str, Any]]:
    raise NotImplementedError("Implementar na etapa de ingestão do scraping")
