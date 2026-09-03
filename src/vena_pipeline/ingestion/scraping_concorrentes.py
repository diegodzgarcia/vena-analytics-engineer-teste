"""
Ingestão da Fonte 2 — Web scraping de preços de concorrentes.

A estrutura do HTML muda a cada request (schema drift proposital). O
parser não pode lançar exceção fatal por causa disso.

Decisão central de design, baseada em inspeção real da página do desafio:
em vez de depender de seletores CSS/classes específicos (que são
exatamente o que quebra quando a estrutura muda), o parser extrai pelo
**padrão de texto visível**, que é mais estável que a marcação em volta
dele. Cada item aparece como "Categoria Preço Status" no texto renderizado
— isso não muda mesmo que a categoria vá de <table><tr><td> pra
<div><span>, <ul><li>, ou qualquer outra coisa.

Duas estratégias, em cascata:
1. `_parse_table()` — se a página tiver uma <table> de verdade, é o
   caminho mais direto (uma linha = um registro, uma célula = um campo).
2. `_parse_text_pattern()` — fallback quando não há <table> (ou ela não
   rendeu nada): âncora no PREÇO (único campo com formato reconhecível de
   forma confiável — dígitos + separador decimal) e caminha
   sequencialmente pelo texto: a categoria do item N é o que sobra do
   texto do item N-1 depois de extrair o status; o status do item N é a
   primeira sequência "Palavra-Título + palavras-minúsculas" logo após o
   preço. Processar sequencialmente (em vez de extrair categoria e status
   de forma independente) evita que um "vaze" pro outro — os dois
   começam com maiúscula, então uma extração ingênua e independente dos
   dois se sobrepõe.

Validado (fora deste sandbox, contra o HTML real da página do desafio)
com 12/12 itens extraídos corretamente.
"""
from __future__ import annotations

import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from vena_pipeline.config import settings
from vena_pipeline.utils.logging import get_logger

_TIMEOUT_SECONDS = 30

# Preço: âncora mais confiável do texto (formato reconhecível), tanto
# "1.234,56" (pt-BR com milhar) quanto "333.22" (ponto decimal simples,
# o formato observado na página real).
_PRICE_PATTERN = re.compile(r"\d{1,3}(?:\.\d{3})*,\d{2}|\d+\.\d{1,2}")

# Cabeçalho fixo da página, observado na inspeção real — removido antes do
# parsing pra não vazar pro texto do primeiro item (ele também é
# Título-Case, indistinguível de uma categoria pela heurística abaixo).
_KNOWN_NOISE_PHRASES = ("Painel de Monitoramento de Preços",)

# Categoria: 1-2 palavras Título-Case no FINAL do texto que precede o preço.
_CATEGORIA_PATTERN = re.compile(r"([A-ZÀ-Ý][a-zà-ÿ]+(?:\s[A-ZÀ-Ý][a-zà-ÿ]+)?)\s*$")

# Status: uma palavra Título-Case seguida de zero ou mais palavras
# minúsculas, a partir do INÍCIO do texto que segue o preço — pára
# naturalmente antes da próxima categoria (que começa com maiúscula de
# novo, mas não é precedida por espaço+minúscula).
_STATUS_PATTERN = re.compile(r"^[A-ZÀ-Ý][a-zà-ÿ]*(?:\s[a-zà-ÿ]+)*")


def _fetch_html(url: str) -> str:
    response = requests.get(url, timeout=_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def _parse_table(soup: BeautifulSoup) -> list[dict[str, Any]]:
    """Estratégia 1: se existir uma <table> de verdade, usa ela — mais
    direto. Não assume nome/ordem de coluna (isso é justamente o que pode
    variar no drift), só nomeia genericamente por posição."""
    table = soup.find("table")
    if table is None:
        return []

    records = []
    for row in table.find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        cells = [c for c in cells if c]
        if not cells:
            continue
        records.append({f"col_{i}": value for i, value in enumerate(cells)})
    return records


def _parse_text_pattern(soup: BeautifulSoup) -> list[dict[str, Any]]:
    """Estratégia 2 (fallback): extrai pelo padrão de texto, não pela
    estrutura de tags. Ver docstring do módulo para a lógica completa."""
    text = soup.get_text(separator=" ", strip=True)
    for noise in _KNOWN_NOISE_PHRASES:
        text = text.replace(noise, " ")

    prices = list(_PRICE_PATTERN.finditer(text))
    if not prices:
        return []

    records: list[dict[str, Any]] = []
    pending_before = text[: prices[0].start()]

    for i, price_match in enumerate(prices):
        cat_match = _CATEGORIA_PATTERN.search(pending_before)
        categoria = cat_match.group(1) if cat_match else (pending_before.strip() or None)

        window_end = prices[i + 1].start() if i + 1 < len(prices) else len(text)
        after = text[price_match.end() : window_end].strip()
        status_match = _STATUS_PATTERN.match(after) if after else None
        status = status_match.group(0) if status_match else (after or None)

        records.append(
            {"categoria": categoria, "preco": price_match.group(), "status": status}
        )

        pending_before = after[len(status) :].strip() if status_match else ""

    return records


def extract_precos_concorrentes(url: str | None = None) -> list[dict[str, Any]]:
    """Extrai os preços de concorrentes da página HTML. Nunca lança
    exceção por causa de mudança de estrutura ou falha de rede — na pior
    hipótese, retorna lista vazia e loga o motivo (a página seguinte do
    pipeline decide o que fazer com um resultado vazio; não é
    responsabilidade desta função interromper o pipeline inteiro)."""
    target_url = url or settings.scraping.url
    log = get_logger(stage="ingestion", source="scraping_concorrentes")

    try:
        html = _fetch_html(target_url)
    except requests.RequestException as exc:
        log.warning("falha_ao_buscar_pagina", erro=str(exc))
        return []

    soup = BeautifulSoup(html, "html.parser")

    records = _parse_table(soup)
    strategy = "table"
    if not records:
        records = _parse_text_pattern(soup)
        strategy = "text_pattern"

    if not records:
        log.warning("nenhum_registro_extraido_estrutura_desconhecida")
    else:
        log.info("pagina_parseada", estrategia=strategy, total_registros=len(records))

    return records
