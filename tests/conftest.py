"""
Env vars dummy para permitir que `vena_pipeline.config.settings` carregue em
qualquer teste, sem exigir um `.env` real ou credenciais GCP de verdade.

Testes que envolvem GCP mockam os clientes (`get_bq_client`/`get_gcs_client`)
e nunca fazem chamada de rede real — essas env vars só existem para o
`load_settings()` não levantar `RuntimeError` de variável obrigatória
ausente.
"""
import os

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/fake-credentials.json")
os.environ.setdefault("GCP_PROJECT_ID", "vena-teste")
os.environ.setdefault("BQ_DATASET", "teste_tecnico_ae_diego")
os.environ.setdefault("GCS_BUCKET", "vena-teste-candidato-ae-diego")
os.environ.setdefault("API_VENDAS_BASE_URL", "https://example.invalid")
os.environ.setdefault("API_VENDAS_TOKEN", "fake-token")
os.environ.setdefault("SCRAPING_URL", "https://example.invalid/scraping")
