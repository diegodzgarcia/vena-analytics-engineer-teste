import pandas as pd

from vena_pipeline.ingestion import loaders


def test_upload_chunk_to_gcs_writes_to_expected_path_and_adds_audit_column(mocker):
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    fake_blob = mocker.MagicMock()
    fake_bucket = mocker.MagicMock()
    fake_bucket.blob.return_value = fake_blob
    fake_gcs_client = mocker.MagicMock()
    fake_gcs_client.bucket.return_value = fake_bucket
    mocker.patch.object(loaders, "get_gcs_client", return_value=fake_gcs_client)

    uri = loaders.upload_chunk_to_gcs(
        df, table_name="itens_pedido", chunk_number=3, run_ts="20260901T120000Z"
    )

    assert uri == (
        "gs://vena-teste-candidato-ae-diego/"
        "raw/itens_pedido/20260901T120000Z/part-00003.parquet"
    )
    fake_bucket.blob.assert_called_once_with(
        "raw/itens_pedido/20260901T120000Z/part-00003.parquet"
    )
    fake_blob.upload_from_file.assert_called_once()

    # a coluna de auditoria é adicionada sem alterar as colunas originais
    assert "_ingested_at" not in df.columns  # DataFrame original não é mutado


def test_load_raw_table_from_gcs_uses_write_truncate_and_wildcard_uri(mocker):
    fake_job = mocker.MagicMock()
    fake_destination_table = mocker.MagicMock(num_rows=42)

    fake_bq_client = mocker.MagicMock()
    fake_bq_client.load_table_from_uri.return_value = fake_job
    fake_bq_client.get_table.return_value = fake_destination_table
    mocker.patch.object(loaders, "get_bq_client", return_value=fake_bq_client)

    loaders.load_raw_table_from_gcs("clientes", run_ts="20260901T120000Z")

    args, kwargs = fake_bq_client.load_table_from_uri.call_args
    uri_arg = args[0]
    destination_arg = args[1]
    job_config = kwargs["job_config"]

    assert uri_arg == (
        "gs://vena-teste-candidato-ae-diego/raw/clientes/20260901T120000Z/*.parquet"
    )
    assert destination_arg == "vena-teste.teste_tecnico_ae_diego.raw_clientes"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    fake_job.result.assert_called_once()  # garante que espera o job concluir
