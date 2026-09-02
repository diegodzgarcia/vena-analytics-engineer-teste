import requests

from vena_pipeline.ingestion.api_vendas import extract_pedidos, records_to_dataframe


class FakeResponse:
    """Simula um requests.Response o suficiente pra exercitar o código sem
    precisar de uma API real ou de uma biblioteca extra de mocking HTTP."""

    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")


def test_extract_pedidos_paginates_until_has_next_false(mocker):
    responses = [
        FakeResponse(200, {"pedidos": [{"id": 1}, {"id": 2}], "has_next": True, "total_pages": 2}),
        FakeResponse(200, {"pedidos": [{"id": 3}], "has_next": False, "total_pages": 2}),
    ]
    mocker.patch.object(requests.Session, "get", side_effect=responses)

    result = extract_pedidos(base_url="https://fake-api.invalid", token="fake-token", page_size=2)

    assert [r["id"] for r in result] == [1, 2, 3]


def test_extract_pedidos_retries_on_429_then_succeeds(mocker):
    responses = [
        FakeResponse(429),  # rate limit — deve tentar de novo
        FakeResponse(200, {"pedidos": [{"id": 1}], "has_next": False}),
    ]
    mocker.patch.object(requests.Session, "get", side_effect=responses)

    result = extract_pedidos(base_url="https://fake-api.invalid", token="fake-token")

    assert [r["id"] for r in result] == [1]


def test_extract_pedidos_retries_on_intermittent_500(mocker):
    responses = [
        FakeResponse(500),  # falha intermitente — deve tentar de novo
        FakeResponse(500),  # de novo
        FakeResponse(200, {"pedidos": [{"id": 42}], "has_next": False}),
    ]
    mocker.patch.object(requests.Session, "get", side_effect=responses)

    result = extract_pedidos(base_url="https://fake-api.invalid", token="fake-token")

    assert [r["id"] for r in result] == [42]


def test_extract_pedidos_does_not_retry_on_real_client_error(mocker):
    """401 (token inválido) não é um erro transitório — não deve haver
    retry, o erro deve propagar imediatamente."""
    mocker.patch.object(requests.Session, "get", return_value=FakeResponse(401))

    try:
        extract_pedidos(base_url="https://fake-api.invalid", token="bad-token")
        assert False, "esperava que um 401 propagasse um erro"
    except requests.HTTPError:
        pass


def test_extract_pedidos_isolates_malformed_records_instead_of_crashing(mocker):
    payload = {
        "pedidos": [{"id": 1}, "isso-nao-deveria-estar-aqui", {"id": 2}],
        "has_next": False,
    }
    mocker.patch.object(requests.Session, "get", return_value=FakeResponse(200, payload))

    result = extract_pedidos(base_url="https://fake-api.invalid", token="fake-token")

    # o registro malformado é descartado, os dois válidos são preservados
    assert [r["id"] for r in result] == [1, 2]


def test_extract_pedidos_falls_back_to_first_list_when_key_is_unexpected(mocker):
    """Se a API usar um nome de chave diferente do documentado, ainda
    conseguimos extrair os registros em vez de retornar lista vazia."""
    payload = {"page": 1, "resultado_inesperado": [{"id": 99}], "has_next": False}
    mocker.patch.object(requests.Session, "get", return_value=FakeResponse(200, payload))

    result = extract_pedidos(base_url="https://fake-api.invalid", token="fake-token")

    assert [r["id"] for r in result] == [99]


def test_extract_pedidos_throttles_between_pages(mocker):
    """Validado contra a API real: sem pausa entre páginas, o cliente
    martela requisições rápido demais e aciona o rate limit de propósito.
    Esse teste trava que a pausa entre páginas existe (não valida o valor
    exato, só que time.sleep é chamado uma vez por página extra)."""
    responses = [
        FakeResponse(200, {"pedidos": [{"id": 1}], "has_next": True, "total_pages": 2}),
        FakeResponse(200, {"pedidos": [{"id": 2}], "has_next": False, "total_pages": 2}),
    ]
    mocker.patch.object(requests.Session, "get", side_effect=responses)
    sleep_mock = mocker.patch("vena_pipeline.ingestion.api_vendas.time.sleep")

    extract_pedidos(base_url="https://fake-api.invalid", token="fake-token")

    # 2 páginas -> 1 pausa (antes da 2ª, não antes da 1ª)
    assert sleep_mock.call_count == 1


def test_records_to_dataframe_stringifies_mixed_type_column():
    """Reproduz o caso real: valor_unitario vem como float em um pedido e
    como texto com unidade embutida em outro. O DataFrame resultante
    precisa ter tudo como string nessa coluna (senão o Parquet quebra na
    conversão, como aconteceu contra a API real)."""
    records = [
        {"id": 1, "valor_unitario": 462.99},
        {"id": 2, "valor_unitario": "462.99 BRL"},
    ]

    df = records_to_dataframe(records)

    assert df["valor_unitario"].tolist() == ["462.99", "462.99 BRL"]
    assert all(isinstance(v, str) for v in df["valor_unitario"])


def test_records_to_dataframe_preserves_nulls_as_none_not_string():
    records = [{"id": 1, "observacao": None}, {"id": 2}]  # 2º nem tem o campo

    df = records_to_dataframe(records)

    for value in df["observacao"]:
        assert value is None
        assert value != "None"
