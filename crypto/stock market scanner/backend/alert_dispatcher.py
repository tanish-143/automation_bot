"""
Alert Dispatcher — Fan-out notifications to multiple channels.

Channels: Slack, Telegram, Email, generic Webhook.
Each channel fails independently — a Slack failure won't block Telegram delivery.
"""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape
import logging
import smtplib
from email.mime.text import MIMEText

import httpx
import redis

from config import settings

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(10.0, connect=5.0)
STABLECOIN_BASES = {
    "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "DAI", "PYUSD", "USDE", "USDS",
    "GUSD", "FRAX", "LUSD", "USDP", "EURC", "EURS",
}
RULE_PRIORITY = {
    "combined": 40,
    "volume_spike": 30,
    "volatility_breakout": 25,
    "price_change_pct": 20,
    "custom": 10,
}


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

    def send_batch(self, alert_payloads: list[dict]):
        """Send per-alert Slack/email plus one throttled Telegram summary."""
        if settings.slack_webhook_url or settings.smtp_host:
            for payload in alert_payloads:
                if settings.slack_webhook_url:
                    self._send_slack(payload["symbol"], payload["rule"], payload["message"])
                if settings.smtp_host:
                    self._send_email(payload["symbol"], payload["rule"], payload["message"])

        if settings.telegram_bot_token:
            self._send_telegram_summary(alert_payloads)

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

    def _send_telegram_summary(self, alert_payloads: list[dict]):
        """Send one hourly Telegram message with the top non-stablecoin setups."""
        if not settings.telegram_default_chat_id:
            return

        if not self._telegram_summary_slot_available():
            return

        filtered = [
            payload for payload in alert_payloads
            if not self._is_stablecoin_symbol(payload.get("symbol", ""))
        ]
        if not filtered:
            return

        ranked = sorted(filtered, key=self._payload_rank, reverse=True)
        top_payloads = ranked[:settings.telegram_top_n]
        text = self._format_telegram_summary(top_payloads)
        url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"

        for attempt in range(2):
            try:
                resp = httpx.post(
                    url,
                    json={
                        "chat_id": settings.telegram_default_chat_id,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                    timeout=TIMEOUT,
                )
                if resp.status_code < 300:
                    return
                if resp.status_code >= 500 and attempt == 0:
                    continue
                logger.warning("Telegram summary %d: %s", resp.status_code, resp.text[:200])
                return
            except httpx.HTTPError as e:
                logger.warning("Telegram summary error: %s", e)

    def _telegram_summary_slot_available(self) -> bool:
        interval_minutes = max(1, settings.telegram_summary_interval_minutes)
        now = datetime.now(timezone.utc)
        bucket_minute = (now.minute // interval_minutes) * interval_minutes
        slot_key = now.strftime(f"scanner:telegram-summary:%Y%m%d%H:{bucket_minute:02d}")

        try:
            client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
            ttl = interval_minutes * 60 + 300
            allowed = client.set(slot_key, "1", ex=ttl, nx=True)
            client.close()
            return bool(allowed)
        except Exception as exc:
            logger.warning("Telegram summary throttle failed, sending anyway: %s", exc)
            return True

    @staticmethod
    def _is_stablecoin_symbol(symbol: str) -> bool:
        base = symbol.split("/")[0].upper()
        return base in STABLECOIN_BASES

    @staticmethod
    def _payload_rank(payload: dict) -> float:
        volume_ratio = float(payload.get("trigger_volume_ratio") or 0)
        change_pct = abs(float(payload.get("price_change_pct_24h") or 0))
        rule_bonus = RULE_PRIORITY.get(str(payload.get("rule")), 0)
        return rule_bonus + (volume_ratio * 10) + change_pct

    @staticmethod
    def _trade_levels(payload: dict) -> tuple[str, float, float]:
        price = float(payload.get("trigger_price") or 0)
        atr = float(payload.get("atr_14") or 0)
        change_pct = float(payload.get("price_change_pct_24h") or 0)

        direction = "LONG" if change_pct >= 0 else "SHORT"
        entry_offset = max(price * 0.003, atr * 0.25 if atr > 0 else 0.0)
        target_offset = max(price * 0.015, atr * 1.5 if atr > 0 else 0.0)

        if direction == "LONG":
            entry = max(price - entry_offset, 0.0)
            take_profit = price + target_offset
        else:
            entry = price + entry_offset
            take_profit = max(price - target_offset, 0.0)

        return direction, entry, take_profit

    def _format_telegram_summary(self, payloads: list[dict]) -> str:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines = [
            f"📊 <b>Hourly Top {len(payloads)} Scanner Setups</b>",
            f"<i>{now}</i>",
            "",
        ]

        for index, payload in enumerate(payloads, start=1):
            direction, entry, take_profit = self._trade_levels(payload)
            symbol = escape(str(payload.get("symbol", "")))
            rule = escape(str(payload.get("rule", "")))
            change_pct = float(payload.get("price_change_pct_24h") or 0)
            volume_ratio = float(payload.get("trigger_volume_ratio") or 0)

            lines.append(f"{index}. <b>{symbol}</b> {direction}")
            lines.append(
                "   "
                f"Rule <code>{rule}</code> | Δ24h {change_pct:+.2f}% | Vol {volume_ratio:.1f}x"
            )
            lines.append(
                "   "
                f"Entry <code>{entry:.4f}</code> | TP <code>{take_profit:.4f}</code>"
            )

        return "\n".join(lines)

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
