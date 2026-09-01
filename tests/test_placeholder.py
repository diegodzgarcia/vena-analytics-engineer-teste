"""
Placeholder — garante que o pytest roda no CI/local desde o primeiro commit.

TODO: testes reais chegam junto com cada etapa de ingestão, ex.:
- test_api_vendas.py: retry em 429/500 (mock com `responses` ou `pytest-mock`),
  paginação, parsing defensivo de payload malformado.
- test_scraping_concorrentes.py: parser tolera variações de HTML (fixtures
  com 2-3 estruturas diferentes simulando o schema drift).
- test_sqlite_extract.py: generator de chunks não materializa a tabela
  inteira (checar com um sqlite de teste pequeno).
"""


def test_smoke():
    assert True
