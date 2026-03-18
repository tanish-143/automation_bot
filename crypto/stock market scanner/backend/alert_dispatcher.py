"""
Alert Dispatcher — Fan-out notifications to multiple channels.

Channels: Slack, Telegram, Email, generic Webhook.
Each channel fails independently — a Slack failure won't block Telegram delivery.
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.text import MIMEText

import httpx

from config import settings

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(10.0, connect=5.0)


class AlertDispatcher:
    """
    Sends alert notifications to all configured channels.
    Each method is fire-and-forget with error isolation.
    """

    def send_all(self, symbol: str, rule: str, message: str):
        """Send to every configured channel. Failures are logged, not raised."""
        if settings.slack_webhook_url:
            self._send_slack(symbol, rule, message)
        if settings.telegram_bot_token:
            self._send_telegram(symbol, rule, message)
        if settings.smtp_host:
            self._send_email(symbol, rule, message)

    # ── Slack ─────────────────────────────────────────────────────────────

    def _send_slack(self, symbol: str, rule: str, message: str):
        """
        POST to Slack incoming webhook.

        Retry: 1 retry on 5xx, then give up (Slack is best-effort).
        """
        payload = {
            "text": f":rotating_light: *{rule.upper()}* — {symbol}",
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*{symbol}* triggered `{rule}`"},
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": message},
                },
            ],
        }

        for attempt in range(2):
            try:
                resp = httpx.post(
                    settings.slack_webhook_url,
                    json=payload,
                    timeout=TIMEOUT,
                )
                if resp.status_code < 300:
                    return
                if resp.status_code >= 500 and attempt == 0:
                    continue
                logger.warning("Slack %d: %s", resp.status_code, resp.text[:200])
                return
            except httpx.HTTPError as e:
                logger.warning("Slack error: %s", e)

    # ── Telegram ──────────────────────────────────────────────────────────

    def _send_telegram(self, symbol: str, rule: str, message: str):
        """
        Send via Telegram Bot API (sendMessage).

        Retry: 1 retry on 5xx or timeout.
        """
        url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
        text = f"🚨 <b>{rule.upper()}</b> — {symbol}\n\n{message}"

        for attempt in range(2):
            try:
                resp = httpx.post(
                    url,
                    json={
                        "chat_id": settings.telegram_default_chat_id,
                        "text": text,
                        "parse_mode": "HTML",
                    },
                    timeout=TIMEOUT,
                )
                if resp.status_code < 300:
                    return
                if resp.status_code >= 500 and attempt == 0:
                    continue
                logger.warning("Telegram %d: %s", resp.status_code, resp.text[:200])
                return
            except httpx.HTTPError as e:
                logger.warning("Telegram error: %s", e)

    # ── Email (SMTP) ─────────────────────────────────────────────────────

    def _send_email(self, symbol: str, rule: str, message: str):
        """
        Send alert email via SMTP with TLS.

        No retry — SMTP failures are logged. Email is the lowest-priority channel.
        """
        try:
            msg = MIMEText(f"{rule.upper()} — {symbol}\n\n{message}")
            msg["Subject"] = f"[Scanner Alert] {rule} — {symbol}"
            msg["From"] = settings.smtp_from
            msg["To"] = settings.smtp_from  # override per-user in production

            with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
                server.starttls()
                server.login(settings.smtp_user, settings.smtp_password)
                server.send_message(msg)

        except Exception as e:
            logger.warning("Email error: %s", e)

    # ── Generic Webhook ───────────────────────────────────────────────────

    @staticmethod
    def send_webhook(url: str, payload: dict):
        """
        POST JSON to an arbitrary webhook URL.

        Used for user-configured webhook subscriptions.
        Retry: 2 attempts with 2s gap on 5xx.
        """
        for attempt in range(2):
            try:
                resp = httpx.post(url, json=payload, timeout=TIMEOUT)
                if resp.status_code < 300:
                    return True
                if resp.status_code >= 500 and attempt == 0:
                    import time
                    time.sleep(2)
                    continue
                logger.warning("Webhook %s returned %d", url, resp.status_code)
                return False
            except httpx.HTTPError as e:
                logger.warning("Webhook %s error: %s", url, e)
                return False
        return False
