import requests

from vena_pipeline.ingestion.scraping_concorrentes import extract_precos_concorrentes

REAL_ITEMS = [
    ("Calçados", "333.22", "Em estoque"),
    ("Calçados", "374.28", "Últimas unidades"),
    ("Calçados", "127.46", "Indisponível"),
    ("Acessórios", "205.87", "Indisponível"),
    ("Acessórios", "61.14", "Em estoque"),
    ("Calçados", "188.25", "Últimas unidades"),
]


def _html_table(items):
    rows = "".join(
        f"<tr><td>{cat}</td><td>{preco}</td><td>{status}</td></tr>"
        for cat, preco, status in items
    )
    return f"<html><body><h1>Painel de Monitoramento de Preços</h1><table>{rows}</table></body></html>"


def _html_divs(items):
    divs = "".join(
        f"<div class='item'><span>{cat}</span><span>{preco}</span><span>{status}</span></div>"
        for cat, preco, status in items
    )
    return f"<html><body><div class='header'>Painel de Monitoramento de Preços</div>{divs}</body></html>"


class FakeResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")


def test_extract_parses_html_table_structure(mocker):
    mocker.patch("requests.get", return_value=FakeResponse(_html_table(REAL_ITEMS)))

    result = extract_precos_concorrentes(url="https://fake-scraping.invalid")

    assert len(result) == len(REAL_ITEMS)
    # estratégia de tabela extrai por posição de coluna (col_0, col_1, col_2)
    assert result[0] == {"col_0": "Calçados", "col_1": "333.22", "col_2": "Em estoque"}


def test_extract_falls_back_to_text_pattern_when_no_table(mocker):
    """O caso central do desafio: schema drift — sem <table>, o parser
    ainda precisa extrair os dados corretamente pelo padrão de texto."""
    mocker.patch("requests.get", return_value=FakeResponse(_html_divs(REAL_ITEMS)))

    result = extract_precos_concorrentes(url="https://fake-scraping.invalid")

    assert len(result) == len(REAL_ITEMS)
    for extracted, (categoria, preco, status) in zip(result, REAL_ITEMS):
        assert extracted == {"categoria": categoria, "preco": preco, "status": status}


def test_extract_text_pattern_does_not_leak_status_into_next_categoria(mocker):
    """Regressão do bug encontrado durante o desenvolvimento: status e
    categoria do próximo item compartilham o mesmo texto 'no meio' entre
    dois preços — extraí-los de forma independente causava sobreposição
    ('Em estoque Acessórios' em vez de 'Em estoque')."""
    items = [("Calçados", "333.22", "Em estoque"), ("Acessórios", "205.87", "Indisponível")]
    mocker.patch("requests.get", return_value=FakeResponse(_html_divs(items)))

    result = extract_precos_concorrentes(url="https://fake-scraping.invalid")

    assert result[0]["status"] == "Em estoque"
    assert result[1]["categoria"] == "Acessórios"


def test_extract_strips_known_page_heading_noise(mocker):
    """O cabeçalho fixo da página ('Painel de Monitoramento de Preços') é
    Título-Case, igual uma categoria — sem removê-lo, vazaria pro
    primeiro item."""
    items = [("Calçados", "333.22", "Em estoque")]
    mocker.patch("requests.get", return_value=FakeResponse(_html_divs(items)))

    result = extract_precos_concorrentes(url="https://fake-scraping.invalid")

    assert result[0]["categoria"] == "Calçados"


def test_extract_returns_empty_list_on_network_failure_instead_of_raising(mocker):
    mocker.patch("requests.get", side_effect=requests.ConnectionError("falha de rede"))

    result = extract_precos_concorrentes(url="https://fake-scraping.invalid")

    assert result == []


def test_extract_returns_empty_list_when_page_has_no_recognizable_data(mocker):
    """Estrutura completamente desconhecida (nem tabela, nem padrão de
    texto reconhecível) — não pode lançar exceção, só retorna vazio."""
    mocker.patch(
        "requests.get",
        return_value=FakeResponse("<html><body><p>Site em manutenção.</p></body></html>"),
    )

    result = extract_precos_concorrentes(url="https://fake-scraping.invalid")

    assert result == []


def test_extract_raises_nothing_on_http_error_status(mocker):
    mocker.patch("requests.get", return_value=FakeResponse("", status_code=500))

    result = extract_precos_concorrentes(url="https://fake-scraping.invalid")

    assert result == []
