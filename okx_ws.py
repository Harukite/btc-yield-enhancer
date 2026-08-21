"""
OKX WebSocket client.

The public/private OKX channels are normalized to the callback shape already
used by StrategyEngine, so trading logic can remain unchanged.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import ssl
import threading
import time
from typing import Any, Callable, Optional

import certifi

logger = logging.getLogger(__name__)


def _f(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        parsed = float(value)
        return default if parsed != parsed else parsed
    except (TypeError, ValueError):
        return default


class OKXWSClient:
    """OKX WS client compatible with the existing strategy engine."""

    WS_PUBLIC_MAINNET = "wss://ws.okx.com:8443/ws/v5/public"
    WS_PRIVATE_MAINNET = "wss://ws.okx.com:8443/ws/v5/private"
    WS_PUBLIC_TESTNET = "wss://wspap.okx.com:8443/ws/v5/public"
    WS_PRIVATE_TESTNET = "wss://wspap.okx.com:8443/ws/v5/private"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        testnet: bool = False,
        instrument_name: Optional[str] = None,
        callback: Optional[Callable[[dict], None]] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testnet = testnet
        self.instrument_name = instrument_name or os.environ.get("OKX_INSTRUMENT_NAME", "BTC-USDC")
        self.base_currency, self.quote_currency = self._split_instrument(self.instrument_name)
        self.index_instrument_name = os.environ.get("OKX_WS_INDEX_INSTRUMENT_NAME")
        if not self.index_instrument_name:
            self.index_instrument_name = self.instrument_name
            if self.testnet and self.quote_currency == "USDC":
                self.index_instrument_name = f"{self.base_currency}-USDT"
        self._user_callback = callback

        self.ws_public_url = (
            os.environ.get("OKX_WS_PUBLIC_URL")
            or (self.WS_PUBLIC_TESTNET if testnet else self.WS_PUBLIC_MAINNET)
        )
        self.ws_private_url = (
            os.environ.get("OKX_WS_PRIVATE_URL")
            or (self.WS_PRIVATE_TESTNET if testnet else self.WS_PRIVATE_MAINNET)
        )

        self._cache_lock = threading.Lock()
        self._cached_usdc_balance = 0.0
        self._cached_btc_balance = 0.0
        self._cached_usdc_available = 0.0
        self._cached_btc_available = 0.0
        self._cached_index_price = 0.0

        self.connected = False
        self.authenticated = False
        self.error: Optional[str] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._public_connected = False
        self._private_connected = False
        self._reconnect_delay = 5.0
        self._max_reconnect_delay = 60.0

    @staticmethod
    def _split_instrument(instrument_name: str) -> tuple[str, str]:
        parts = instrument_name.replace("_", "-").upper().split("-")
        if len(parts) >= 2:
            return parts[0], parts[1]
        return "BTC", "USDC"

    @staticmethod
    def _ssl_context() -> ssl.SSLContext:
        return ssl.create_default_context(cafile=certifi.where())

    @property
    def cached_usdc_balance(self) -> float:
        with self._cache_lock:
            return self._cached_usdc_balance

    @cached_usdc_balance.setter
    def cached_usdc_balance(self, value: float):
        with self._cache_lock:
            self._cached_usdc_balance = value

    @property
    def cached_btc_balance(self) -> float:
        with self._cache_lock:
            return self._cached_btc_balance

    @cached_btc_balance.setter
    def cached_btc_balance(self, value: float):
        with self._cache_lock:
            self._cached_btc_balance = value

    @property
    def cached_usdc_available(self) -> float:
        with self._cache_lock:
            return self._cached_usdc_available

    @cached_usdc_available.setter
    def cached_usdc_available(self, value: float):
        with self._cache_lock:
            self._cached_usdc_available = value

    @property
    def cached_btc_available(self) -> float:
        with self._cache_lock:
            return self._cached_btc_available

    @cached_btc_available.setter
    def cached_btc_available(self, value: float):
        with self._cache_lock:
            self._cached_btc_available = value

    @property
    def cached_index_price(self) -> float:
        with self._cache_lock:
            return self._cached_index_price

    @cached_index_price.setter
    def cached_index_price(self, value: float):
        with self._cache_lock:
            self._cached_index_price = value

    def set_callback(self, callback: Optional[Callable[[dict], None]]):
        self._user_callback = callback

    def start(self):
        if self._running:
            logger.warning("OKX WS already running")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("OKX WS client starting...")

    def stop(self):
        self._running = False
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        logger.info("OKX WS client stopping...")

    def is_running(self) -> bool:
        return self._running

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._ws_main())
        except Exception as exc:
            logger.error("OKX WS loop fatal: %s", exc)
        finally:
            try:
                self._loop.close()
            except Exception:
                pass
            self.connected = False
            self.authenticated = False

    async def _ws_main(self):
        while self._running:
            try:
                await asyncio.gather(self._public_main(), self._private_main())
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.error = str(exc)
                self.connected = False
                self.authenticated = False
                logger.error("OKX WS error: %s, reconnecting in %.0fs...", exc, self._reconnect_delay)
                if self._running:
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(self._reconnect_delay * 1.5, self._max_reconnect_delay)

    async def _public_main(self):
        import websockets

        async with websockets.connect(
            self.ws_public_url,
            ssl=self._ssl_context(),
            ping_interval=None,
            close_timeout=5,
        ) as ws:
            self._public_connected = True
            self._update_connected()
            self._reconnect_delay = 5.0
            await self._subscribe_public(ws)
            await self._message_loop(ws, authenticated=False)

    async def _private_main(self):
        import websockets

        async with websockets.connect(
            self.ws_private_url,
            ssl=self._ssl_context(),
            ping_interval=None,
            close_timeout=5,
        ) as ws:
            self._private_connected = True
            self._update_connected()
            self._reconnect_delay = 5.0
            await self._login_private(ws)
            await self._subscribe_private(ws)
            await self._message_loop(ws, authenticated=True)

    def _update_connected(self):
        self.connected = self._public_connected or self._private_connected

    async def _subscribe_public(self, ws):
        msg = {
            "id": "indexticker",
            "op": "subscribe",
            "args": [{"channel": "index-tickers", "instId": self.index_instrument_name}],
        }
        await ws.send(json.dumps(msg))
        logger.info("OKX WS index ticker subscribed: %s", self.index_instrument_name)

    async def _login_private(self, ws):
        timestamp = str(int(time.time()))
        sign = self._sign_ws(timestamp)
        msg = {
            "op": "login",
            "args": [{
                "apiKey": self.api_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": sign,
            }],
        }
        await ws.send(json.dumps(msg))
        while self._running:
            raw = await asyncio.wait_for(ws.recv(), timeout=15)
            if raw in ("ping", "pong"):
                await self._handle_ping_pong(ws, raw)
                continue
            msg = json.loads(raw)
            event = msg.get("event")
            if event == "login" and str(msg.get("code", "0")) == "0":
                self.authenticated = True
                logger.info("OKX WS private authenticated")
                return
            if event == "error" or msg.get("code"):
                self.authenticated = False
                self.error = msg.get("msg") or str(msg)
                raise ConnectionError(f"OKX WS login failed: {self.error}")

    def _sign_ws(self, timestamp: str) -> str:
        payload = f"{timestamp}GET/users/self/verify"
        digest = hmac.new(self.api_secret.encode(), payload.encode(), hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    async def _subscribe_private(self, ws):
        args = [
            {"channel": "account", "ccy": self.base_currency},
            {"channel": "account", "ccy": self.quote_currency},
            {"channel": "orders", "instType": "SPOT", "instId": self.instrument_name},
        ]
        await ws.send(json.dumps({"id": "account", "op": "subscribe", "args": args}))
        logger.info("OKX WS private subscribed: account %s/%s", self.base_currency, self.quote_currency)

    async def _message_loop(self, ws, authenticated: bool):
        import websockets

        while self._running:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=25)
                if raw in ("ping", "pong"):
                    await self._handle_ping_pong(ws, raw)
                    continue
                self._handle_message(json.loads(raw))
            except asyncio.TimeoutError:
                try:
                    await ws.send("ping")
                except Exception:
                    break
            except websockets.exceptions.ConnectionClosed:
                break
            except Exception as exc:
                logger.warning("OKX WS recv error: %s", exc)
                self.error = str(exc)

        if authenticated:
            self._private_connected = False
            self.authenticated = False
        else:
            self._public_connected = False
        self._update_connected()

    async def _handle_ping_pong(self, ws, raw: str):
        if raw == "ping":
            await ws.send("pong")

    def _handle_message(self, msg: dict):
        if not isinstance(msg, dict):
            return
        event = msg.get("event")
        if event == "error":
            self.error = msg.get("msg") or str(msg)
            logger.warning("OKX WS error event: %s", self.error)
            return
        if event in ("subscribe", "login"):
            return

        arg = msg.get("arg", {})
        channel = arg.get("channel", "")
        rows = msg.get("data") or []
        if not rows:
            return

        if channel == "index-tickers":
            self._handle_index_ticker(rows[0])
        elif channel == "account":
            for row in rows:
                self._handle_account(row)
        elif channel == "orders":
            for row in rows:
                self._handle_order(row)

    def _handle_index_ticker(self, row: dict):
        price = _f(row.get("idxPx"))
        if price <= 0:
            return
        self.cached_index_price = price
        self._emit({
            "channel": f"ticker.{self.instrument_name}.index",
            "data": {"index_price": price, "idx": price},
        })

    def _handle_account(self, row: dict):
        for detail in row.get("details", []):
            currency = detail.get("ccy", "").upper()
            available = _f(detail.get("availBal"), _f(detail.get("cashBal")))
            equity = _f(detail.get("eq"), _f(detail.get("cashBal"), available))
            if currency == self.base_currency:
                self.cached_btc_balance = equity
                self.cached_btc_available = available
                self._emit({
                    "channel": "user.portfolio.btc",
                    "data": {"balance": equity, "available": available, "equity": equity},
                })
            elif currency == self.quote_currency:
                self.cached_usdc_balance = equity
                self.cached_usdc_available = available
                self._emit({
                    "channel": "user.portfolio.usdc",
                    "data": {"balance": equity, "available": available, "equity": equity},
                })

    def _handle_order(self, row: dict):
        """Normalize an OKX orders-channel update for StrategyEngine."""
        self._emit({
            "channel": "user.order",
            "data": {
                "order_id": row.get("ordId", ""),
                "state": row.get("state", ""),
                "side": row.get("side", ""),
                "instrument_name": row.get("instId", self.instrument_name),
                "label": row.get("clOrdId", ""),
                "amount": _f(row.get("sz")),
                "price": _f(row.get("px")),
                "filled_amount": _f(row.get("accFillSz")),
                "fill_amount": _f(row.get("fillSz")),
                "fill_price": _f(row.get("fillPx")),
                "average_price": _f(row.get("avgPx")),
                "timestamp": row.get("uTime") or row.get("fillTime") or row.get("cTime", ""),
            },
        })

    def _emit(self, msg: dict):
        if self._user_callback:
            try:
                self._user_callback(msg)
            except Exception as exc:
                logger.error("OKX WS callback error: %s", exc)

    def get_snapshot(self) -> dict:
        return {
            "connected": self.connected,
            "authenticated": self.authenticated,
            "error": self.error,
            "cached_usdc_balance": self.cached_usdc_balance,
            "cached_btc_balance": self.cached_btc_balance,
            "cached_usdc_available": self.cached_usdc_available,
            "cached_btc_available": self.cached_btc_available,
            "cached_index_price": self.cached_index_price,
            "index_instrument_name": self.index_instrument_name,
        }
