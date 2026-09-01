"""
Ingestão da Fonte 3 — banco transacional SQLite (`clientes`, `produtos`,
`itens_pedido`).

`itens_pedido` tem 5.000.000 de linhas — não pode ser carregado inteiro em
memória. `clientes` e `produtos` são pequenos (~6k e ~800 linhas), mas têm
duplicatas, nulos e tipos inconsistentes propositais (tratados na camada
staging do dbt, não aqui — aqui só extraímos o raw).

Contrato:
- `extract_small_table(table_name)` — leitura direta para `clientes`/`produtos`.
- `extract_itens_pedido_chunks(chunk_size)` — generator que produz DataFrames
  em lotes (via `pandas.read_sql` com cursor/`LIMIT-OFFSET` ou leitura via
  `sqlite3` cursor), cada um gravado como um arquivo Parquet particionado
  em GCS antes do load incremental para o BigQuery raw.

TODO (próxima etapa — Ingestão do SQLite):
- Implementar o generator de chunks sem materializar a tabela inteira.
- Escrever cada chunk como Parquet em
  `gs://<bucket>/raw/itens_pedido/part-<n>.parquet`.
- Load incremental (append) para `raw_itens_pedido` no BigQuery, com coluna
  de auditoria `_ingested_at`.
"""
from __future__ import annotations

from collections.abc import Iterator

import pandas as pd


def extract_small_table(table_name: str) -> pd.DataFrame:
    raise NotImplementedError("Implementar na etapa de ingestão do SQLite")


def extract_itens_pedido_chunks(chunk_size: int) -> Iterator[pd.DataFrame]:
    raise NotImplementedError("Implementar na etapa de ingestão do SQLite")
