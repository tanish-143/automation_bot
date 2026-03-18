"""
Centralised configuration — loaded from environment variables.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Environment ──
    env: str = "development"  # development | staging | production
    log_level: str = "INFO"

    # ── Database ──
    database_url: str = "postgresql+asyncpg://scanner:scanner@localhost:5432/scanner"
    database_url_sync: str = "postgresql://scanner:scanner@localhost:5432/scanner"

    # ── Redis (Celery broker + result backend + cache) ──
    redis_url: str = "redis://localhost:6379/0"

    # ── Binance API ──
    binance_rest_base: str = "https://api.binance.com"
    binance_ws_base: str = "wss://stream.binance.com:9443"
    binance_rate_limit_per_min: int = 1200
    binance_ws_max_streams_per_conn: int = 200  # Binance hard limit: 1024

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

    # ── API ──
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: list[str] = ["http://localhost:3000"]

    model_config = {"env_prefix": "SCANNER_", "env_file": ".env"}


settings = Settings()
