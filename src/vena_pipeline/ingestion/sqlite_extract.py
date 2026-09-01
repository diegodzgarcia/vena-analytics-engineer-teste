"""
Ingestão da Fonte 3 — banco transacional SQLite (`clientes`, `produtos`,
`itens_pedido`).

`itens_pedido` tem 5.000.000 de linhas — não pode ser carregado inteiro em
memória. `clientes` e `produtos` são pequenos (~6k e ~800 linhas), mas têm
duplicatas, nulos e tipos inconsistentes propositais.

Decisão de design: esta camada extrai o dado **exatamente como está na
fonte**, sem nenhuma limpeza/tipagem (isso é responsabilidade da staging no
dbt). Ex.: `preco_tabela` continua como texto ("R$ 533.46" misturado com
"533.46"), `data_item` continua como string. Manter o raw fiel à fonte
facilita auditoria e permite reprocessar a staging sem precisar re-extrair.

Contrato:
- `extract_small_table(table_name, db_path=None)` — leitura direta para
  `clientes`/`produtos`.
- `extract_itens_pedido_chunks(chunk_size=None, db_path=None)` — generator
  que produz DataFrames em lotes.

Ambos aceitam `db_path` opcional (além de ler de `settings` por padrão) para
facilitar testes unitários com um SQLite de fixture, sem precisar mockar
configuração global.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator

import pandas as pd

from vena_pipeline.config import settings
from vena_pipeline.utils.logging import get_logger

_SMALL_TABLES = {"clientes", "produtos"}


def extract_small_table(table_name: str, db_path: str | None = None) -> pd.DataFrame:
    """Lê uma tabela pequena (`clientes` ou `produtos`) inteira para memória.

    São ~6k e ~800 linhas respectivamente — tamanho seguro para carregar de
    uma vez. Para `itens_pedido` (5M linhas), use
    `extract_itens_pedido_chunks`.
    """
    if table_name not in _SMALL_TABLES:
        raise ValueError(
            f"'{table_name}' não é uma tabela pequena esperada ({sorted(_SMALL_TABLES)}). "
            "Para itens_pedido, use extract_itens_pedido_chunks()."
        )

    path = db_path or settings.sqlite.db_path
    log = get_logger(stage="ingestion", source=f"sqlite.{table_name}")

    with sqlite3.connect(path) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    log.info("tabela_extraida", linhas=len(df))
    return df


def extract_itens_pedido_chunks(
    chunk_size: int | None = None, db_path: str | None = None
) -> Iterator[pd.DataFrame]:
    """Generator que produz `itens_pedido` em lotes de `chunk_size` linhas,
    sem materializar a tabela inteira em memória.

    Usa um único cursor `sqlite3` com `fetchmany`, que faz streaming direto
    do B-tree do SQLite — deliberadamente NÃO usa paginação via
    `LIMIT/OFFSET`, porque o OFFSET re-varre a tabela desde o início a cada
    página, degradando para O(n²) em tabelas grandes (5M linhas tornaria
    isso proibitivamente lento). `fetchmany` mantém a posição do cursor
    aberto entre chamadas, então cada chunk custa O(chunk_size), não O(n).
    """
    size = chunk_size or settings.sqlite.chunk_size
    path = db_path or settings.sqlite.db_path
    log = get_logger(stage="ingestion", source="sqlite.itens_pedido")

    conn = sqlite3.connect(path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM itens_pedido")
        columns = [col[0] for col in cursor.description]

        chunk_number = 0
        total_rows = 0
        while True:
            rows = cursor.fetchmany(size)
            if not rows:
                break

            chunk_number += 1
            total_rows += len(rows)
            df = pd.DataFrame.from_records(rows, columns=columns)

            log.info(
                "chunk_extraido",
                chunk=chunk_number,
                linhas_chunk=len(df),
                linhas_acumuladas=total_rows,
            )
            yield df

        log.info(
            "extracao_concluida", total_chunks=chunk_number, total_linhas=total_rows
        )
    finally:
        conn.close()
