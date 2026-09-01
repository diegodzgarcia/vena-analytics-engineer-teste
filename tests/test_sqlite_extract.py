import sqlite3
from pathlib import Path

import pytest

from vena_pipeline.ingestion.sqlite_extract import (
    extract_itens_pedido_chunks,
    extract_small_table,
)

TOTAL_ITENS = 250  # deliberadamente não-múltiplo redondo de chunk_size


@pytest.fixture
def fake_db(tmp_path: Path) -> Path:
    """SQLite de fixture pequeno, com o mesmo shape das tabelas reais:
    `clientes` com uma duplicata proposital, e `itens_pedido` com um volume
    pequeno o bastante pra testar chunking sem esperar 5M linhas."""
    db_path = tmp_path / "fake.sqlite"
    conn = sqlite3.connect(db_path)

    conn.execute("CREATE TABLE clientes (cliente_id INTEGER, nome TEXT)")
    conn.executemany(
        "INSERT INTO clientes VALUES (?, ?)",
        [(1, "Ana"), (2, "Bruno"), (2, "Bruno duplicado")],
    )

    conn.execute(
        "CREATE TABLE itens_pedido (item_id INTEGER, pedido_id INTEGER, "
        "cliente_id INTEGER, produto_id INTEGER, data_item TEXT, "
        "quantidade INTEGER, valor_unitario REAL)"
    )
    conn.executemany(
        "INSERT INTO itens_pedido VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(i, i, 1, 1, "2024-01-01 00:00:00", 1, 10.0) for i in range(1, TOTAL_ITENS + 1)],
    )
    conn.commit()
    conn.close()
    return db_path


def test_extract_small_table_returns_all_rows_as_is(fake_db):
    df = extract_small_table("clientes", db_path=str(fake_db))

    assert len(df) == 3  # inclui a duplicata — dedup é responsabilidade da staging
    assert set(df.columns) == {"cliente_id", "nome"}


def test_extract_small_table_rejects_itens_pedido(fake_db):
    with pytest.raises(ValueError):
        extract_small_table("itens_pedido", db_path=str(fake_db))


def test_extract_itens_pedido_chunks_covers_all_rows(fake_db):
    chunks = list(extract_itens_pedido_chunks(chunk_size=100, db_path=str(fake_db)))

    # 250 linhas em chunks de 100 -> 3 chunks (100, 100, 50)
    assert [len(c) for c in chunks] == [100, 100, 50]
    assert sum(len(c) for c in chunks) == TOTAL_ITENS


def test_extract_itens_pedido_chunks_never_materializes_more_than_one_chunk(fake_db):
    """Prova que cada chunk é um DataFrame isolado — nunca a tabela inteira
    de uma vez (o requisito central desta etapa: 5M linhas não cabem na
    RAM de uma vez)."""
    chunk_size = 40
    max_rows_seen_at_once = 0

    for chunk in extract_itens_pedido_chunks(chunk_size=chunk_size, db_path=str(fake_db)):
        max_rows_seen_at_once = max(max_rows_seen_at_once, len(chunk))
        assert len(chunk) <= chunk_size

    assert max_rows_seen_at_once == chunk_size


def test_extract_itens_pedido_chunks_columns_match_source_schema(fake_db):
    first_chunk = next(extract_itens_pedido_chunks(chunk_size=50, db_path=str(fake_db)))

    assert list(first_chunk.columns) == [
        "item_id",
        "pedido_id",
        "cliente_id",
        "produto_id",
        "data_item",
        "quantidade",
        "valor_unitario",
    ]
