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

    # ── Binance REST API ──
    binance_rest_base: str = "https://api.binance.com"

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
    telegram_summary_interval_minutes: int = 60
    telegram_top_n: int = 10
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = "scanner@example.com"

    # ── Groq AI ──
    groq_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"

    # ── Alerts CSV Export ──
    alerts_export_dir: str = "output/alerts"

    # ── Chandelier Exit ──
    ce_atr_period: int = 22
    ce_atr_mult: float = 3.0

    # ── API ──
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: list[str] = ["http://localhost:3000"]

    model_config = {"env_prefix": "SCANNER_", "env_file": str(_ENV_FILE), "extra": "ignore"}


settings = Settings()
