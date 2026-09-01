"""
Funções genéricas de "landing" — gravam DataFrames como Parquet no GCS e
carregam (load job) para tabelas raw no BigQuery. Usadas por todas as
fontes de ingestão (não só o SQLite), para manter o mesmo padrão de
idempotência em toda a camada raw.

Padrão de idempotência adotado: cada execução do pipeline grava seus
arquivos Parquet sob um prefixo próprio (`run_ts`, timestamp da execução) e
o load job usa `WRITE_TRUNCATE` — ou seja, cada execução completa sobrescreve
o conteúdo da tabela raw correspondente, em vez de fazer append. Isso
garante que rodar o pipeline duas vezes seguidas não duplica nenhum dado.

Essa escolha é adequada porque as fontes hoje não expõem uma coluna de
watermark/CDC confiável (o SQLite é um dump completo; a API não documenta
filtro incremental). Se no futuro alguma fonte passar a suportar extração
incremental de verdade, o load muda para `WRITE_APPEND` + chave de dedup na
staging (dbt) — a raw continua um espelho fiel do que foi extraído em cada
run, só que parcial em vez de completo.
"""
from __future__ import annotations

import io

import pandas as pd
from google.cloud import bigquery

from vena_pipeline.config import settings
from vena_pipeline.utils.gcp import get_bq_client, get_gcs_client, raw_table_ref
from vena_pipeline.utils.logging import get_logger


def upload_chunk_to_gcs(
    df: pd.DataFrame, table_name: str, chunk_number: int, run_ts: str
) -> str:
    """Grava um chunk como Parquet em GCS, sob um prefixo por execução
    (`run_ts`), e retorna o caminho `gs://` gravado.

    Adiciona `_ingested_at` como coluna de auditoria (quando o chunk foi
    extraído), sem alterar nenhum outro valor original — a raw permanece
    fiel à fonte.
    """
    log = get_logger(stage="ingestion", source=f"gcs.{table_name}")

    df = df.copy()
    df["_ingested_at"] = pd.Timestamp.now(tz="UTC")

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob_path = f"raw/{table_name}/{run_ts}/part-{chunk_number:05d}.parquet"
    bucket = get_gcs_client().bucket(settings.gcp.bucket)
    blob = bucket.blob(blob_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")

    gcs_uri = f"gs://{settings.gcp.bucket}/{blob_path}"
    log.info("chunk_gravado_gcs", uri=gcs_uri, linhas=len(df))
    return gcs_uri


def load_raw_table_from_gcs(table_name: str, run_ts: str) -> bigquery.LoadJob:
    """Carrega todos os arquivos Parquet de uma execução (`run_ts`) para a
    tabela `raw_<table_name>` no BigQuery, sobrescrevendo o conteúdo
    anterior (ver docstring do módulo sobre a escolha de `WRITE_TRUNCATE`).

    O schema é inferido do próprio Parquet (que já carrega o schema do
    DataFrame de origem) — não usamos autodetect de CSV/JSON, que é bem
    menos confiável para colunas com tipos mistos como `preco_tabela`.
    """
    log = get_logger(stage="ingestion", source=f"bigquery.raw_{table_name}")

    uri = f"gs://{settings.gcp.bucket}/raw/{table_name}/{run_ts}/*.parquet"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    client = get_bq_client()
    job = client.load_table_from_uri(uri, raw_table_ref(table_name), job_config=job_config)
    job.result()  # bloqueia até concluir; propaga exceção se o load falhar

    destination = client.get_table(raw_table_ref(table_name))
    log.info(
        "load_concluido",
        uri=uri,
        destino=raw_table_ref(table_name),
        linhas_carregadas=destination.num_rows,
    )
    return job
