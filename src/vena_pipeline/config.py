"""
Configuração central do pipeline.

Todas as constantes de ambiente (GCP, credenciais, URLs das fontes) são lidas
uma única vez aqui e reutilizadas pelos assets do Dagster, scripts de ingestão
e pelo dbt (via profiles.yml, que lê as mesmas env vars).
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Variável de ambiente obrigatória não definida: {name}. "
            f"Verifique seu .env (veja .env.example)."
        )
    return value


@dataclass(frozen=True)
class GCPConfig:
    project_id: str
    dataset: str
    bucket: str
    region: str
    credentials_path: str


@dataclass(frozen=True)
class ApiVendasConfig:
    base_url: str
    token: str
    page_size: int


@dataclass(frozen=True)
class ScrapingConfig:
    url: str


@dataclass(frozen=True)
class SqliteConfig:
    db_path: str
    chunk_size: int


@dataclass(frozen=True)
class Settings:
    gcp: GCPConfig
    api_vendas: ApiVendasConfig
    scraping: ScrapingConfig
    sqlite: SqliteConfig
    log_level: str


def load_settings() -> Settings:
    return Settings(
        gcp=GCPConfig(
            project_id=_require("GCP_PROJECT_ID"),
            dataset=_require("BQ_DATASET"),
            bucket=_require("GCS_BUCKET"),
            region=os.getenv("GCP_REGION", "southamerica-east1"),
            credentials_path=_require("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        api_vendas=ApiVendasConfig(
            base_url=_require("API_VENDAS_BASE_URL"),
            token=_require("API_VENDAS_TOKEN"),
            page_size=int(os.getenv("API_VENDAS_PAGE_SIZE", "500")),
        ),
        scraping=ScrapingConfig(
            url=_require("SCRAPING_URL"),
        ),
        sqlite=SqliteConfig(
            db_path=os.getenv("SQLITE_DB_PATH", "./data/banco_transacional.sqlite"),
            chunk_size=int(os.getenv("SQLITE_CHUNK_SIZE", "100000")),
        ),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


settings = load_settings()
