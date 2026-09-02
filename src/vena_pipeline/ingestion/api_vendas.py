"""
Ingestão da Fonte 1 — API de vendas (paginada, com rate limit e falhas
intermitentes simuladas).

Decisão de design: assim como na ingestão do SQLite, esta função extrai o
dado **como veio da API**, sem limpeza/tipagem (isso é responsabilidade da
staging no dbt, num próximo passo fora do escopo desta etapa — hoje só
existe staging pra clientes/produtos/itens_pedido do SQLite). A única coisa
"defensiva" feita aqui é estrutural: um registro que não seja um dict
válido é isolado (logado e descartado) em vez de derrubar a extração
inteira — não fazemos coerção de tipo de campo individual.

Resiliência a 429 (rate limit) e 500 (falha intermitente): retry com
backoff exponencial + jitter via `tenacity`. Não implementamos parsing de
header `Retry-After` porque a especificação do teste não documenta esse
header sendo enviado — backoff exponencial com jitter é a prática padrão
de mercado quando isso não está disponível/documentado.

Validado contra a API real do desafio: a primeira versão desta função
(sem pausa entre páginas, `stop_after_attempt(6)`, backoff até 20s) disparou
~30 requisições em menos de 3 segundos sem nenhum intervalo — martelou a
API o suficiente pra derrubar num 429 persistente, que esgotou as 6
tentativas mesmo com backoff crescendo até ~17s. Duas mudanças resolveram:
um pequeno intervalo proativo entre páginas (`_INTER_PAGE_DELAY_SECONDS`,
evita provocar o rate limit em primeiro lugar) e um orçamento de retry
maior (`_MAX_ATTEMPTS` mais alto, teto de backoff maior — pra aguentar
quando o rate limit acontece mesmo assim). Isso é um trade-off deliberado
de latência por confiabilidade, aceitável pra um job batch que não precisa
ser rápido, só precisa terminar com sucesso.
Achado adicional, validado contra a API real: o mesmo campo pode vir com
tipo diferente entre registros (`valor_unitario` como `462.99` em um
pedido e `"462.99 BRL"` em outro) — JSON é semi-estruturado, diferente do
SQLite, onde cada coluna já vem com tipo consistente garantido pelo schema
do banco. Parquet exige tipo homogêneo por coluna, então o load quebrava
na conversão. `records_to_dataframe()` resolve isso convertendo todo valor
não-nulo pra string antes de gravar — a raw desta fonte fica inteiramente
em texto (nulos preservados como nulos), e a tipagem de verdade (extrair
o número de "462.99 BRL", por exemplo) fica pra uma staging futura, fora
do escopo desta etapa.
"""
from __future__ import annotations

import math
import time
from typing import Any

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from vena_pipeline.config import settings
from vena_pipeline.utils.logging import get_logger

_KNOWN_RECORD_KEYS = ("pedidos", "items", "data", "results")
_MAX_ATTEMPTS = 10
_MAX_BACKOFF_SECONDS = 60
_TIMEOUT_SECONDS = 30
_INTER_PAGE_DELAY_SECONDS = 0.3


class TransientAPIError(Exception):
    """Erro transitório (429 rate limit ou 500 falha intermitente) — sinal
    pro tenacity de que vale a pena tentar de novo. Qualquer outro erro
    HTTP (401, 404, etc.) é um problema real e não é retentado."""


def _log_before_sleep(retry_state) -> None:
    log = get_logger(stage="ingestion", source="api_vendas.retry")
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    sleep_s = getattr(retry_state.next_action, "sleep", None)
    log.warning(
        "retry_apos_erro_transitorio",
        tentativa=retry_state.attempt_number,
        erro=str(exc),
        proxima_espera_segundos=round(sleep_s, 2) if sleep_s is not None else None,
    )


def _raise_if_retryable(response: requests.Response) -> None:
    if response.status_code in (429, 500):
        raise TransientAPIError(f"status={response.status_code}")
    response.raise_for_status()


@retry(
    retry=retry_if_exception_type(TransientAPIError),
    wait=wait_exponential_jitter(initial=1, max=_MAX_BACKOFF_SECONDS),
    stop=stop_after_attempt(_MAX_ATTEMPTS),
    before_sleep=_log_before_sleep,
    reraise=True,
)
def _fetch_page(
    session: requests.Session, base_url: str, token: str, page: int, page_size: int
) -> dict[str, Any]:
    response = session.get(
        f"{base_url}/api/pedidos",
        params={"page": page, "page_size": page_size},
        headers={"Authorization": f"Bearer {token}"},
        timeout=_TIMEOUT_SECONDS,
    )
    _raise_if_retryable(response)
    return response.json()


def _extract_records(payload: dict[str, Any]) -> list[Any]:
    """Localiza a lista de registros dentro do payload. Tenta as chaves
    conhecidas/documentadas primeiro; se a API usar um nome diferente do
    esperado, cai para a primeira lista encontrada no payload em vez de
    falhar — melhor extrair algo e logar o formato do que quebrar."""
    for key in _KNOWN_RECORD_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return value

    for value in payload.values():
        if isinstance(value, list):
            return value

    return []


def extract_pedidos(
    base_url: str | None = None,
    token: str | None = None,
    page_size: int | None = None,
) -> list[dict[str, Any]]:
    """Extrai todos os pedidos da API, paginando até `has_next=False`.

    Aceita `base_url`/`token`/`page_size` opcionais (além de ler de
    `settings` por padrão) para facilitar testes unitários sem precisar
    mockar configuração global — mesmo padrão usado em
    `sqlite_extract.py`.
    """
    url = (base_url or settings.api_vendas.base_url).rstrip("/")
    auth_token = token or settings.api_vendas.token
    size = page_size or settings.api_vendas.page_size
    log = get_logger(stage="ingestion", source="api_vendas")

    session = requests.Session()
    all_records: list[dict[str, Any]] = []
    malformed_count = 0
    page = 1

    while True:
        if page > 1:
            time.sleep(_INTER_PAGE_DELAY_SECONDS)

        payload = _fetch_page(session, url, auth_token, page, size)
        raw_records = _extract_records(payload)

        for record in raw_records:
            if isinstance(record, dict):
                all_records.append(record)
            else:
                malformed_count += 1
                log.warning(
                    "registro_malformado_ignorado",
                    pagina=page,
                    tipo=type(record).__name__,
                )

        has_next = bool(payload.get("has_next", False))
        log.info(
            "pagina_processada",
            pagina=page,
            total_pages=payload.get("total_pages"),
            registros_pagina=len(raw_records),
            registros_acumulados=len(all_records),
        )

        if not has_next:
            break
        page += 1

    if malformed_count:
        log.warning(
            "extracao_concluida_com_registros_malformados",
            total_malformados=malformed_count,
        )
    log.info(
        "extracao_concluida", total_paginas=page, total_registros=len(all_records)
    )

    return all_records


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return str(value)


def records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Converte os registros extraídos pra um DataFrame seguro de gravar
    como Parquet, forçando todo valor não-nulo pra string.

    Necessário porque campos de uma API JSON podem variar de tipo entre
    registros — ver docstring do módulo pro caso real que motivou isso
    (`valor_unitario` como número em um pedido, texto com unidade em
    outro). Nulos são preservados como `None`, não viram a string
    `"None"`.
    """
    df = pd.DataFrame(records)
    return df.map(_stringify)
