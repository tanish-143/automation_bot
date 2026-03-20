"""
Centralised configuration — loaded from environment variables.
"""

from pathlib import Path

from pydantic_settings import BaseSettings

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    # ── Environment ──
    env: str = "development"  # development | staging | production
    log_level: str = "INFO"

    # ── Database ──
    database_url: str = "postgresql+asyncpg://scanner:scanner@localhost:5432/scanner"
    database_url_sync: str = "postgresql://scanner:scanner@localhost:5432/scanner"

    # ── Redis (Celery broker + result backend + cache) ──
    redis_url: str = "redis://localhost:6379/0"

    # ── CoinGecko REST API (Demo key) ──
    coingecko_rest_base: str = "https://api.coingecko.com/api/v3"
    coingecko_api_key: str = ""  # Demo API key (x-cg-demo-api-key header)
    coingecko_rate_limit_per_min: int = 30  # demo tier: ~30 calls/min

    # ── CoinCap WebSocket (free, no key required) ──
    coincap_ws_url: str = "wss://ws.coincap.io/prices?assets=ALL"

    # ── Scanner ──
    scan_interval_seconds: int = 60
    snapshot_tick_seconds: int = 10
    min_volume_floor_usd: float = 10_000.0
    stale_data_seconds: int = 120

    # ── Worker Scaling ──
    worker_id: int = 0
    total_workers: int = 1

    # ── Alerting ──
    slack_webhook_url: str = ""
    telegram_bot_token: str = ""
    telegram_default_chat_id: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = "scanner@example.com"

    # ── Groq AI ──
    groq_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"

    # ── API ──
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: list[str] = ["http://localhost:3000"]

    model_config = {"env_prefix": "SCANNER_", "env_file": str(_ENV_FILE), "extra": "ignore"}


settings = Settings()
