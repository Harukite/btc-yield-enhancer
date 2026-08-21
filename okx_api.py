"""
OKX REST API client.

This module keeps the existing strategy engine contract intact while using
OKX spot REST as the exchange transport.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)


def _f(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        parsed = float(value)
        if parsed != parsed:
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _format_decimal(value: float) -> str:
    return f"{float(value):.12f}".rstrip("0").rstrip(".")


OKX_ERROR_CATEGORY = {
    "50011": "rate_limit",
    "50013": "permission_denied",
    "50026": "exchange_not_available",
    "50100": "auth",
    "50101": "auth",
    "50102": "auth",
    "50103": "auth",
    "50104": "auth",
    "50105": "auth",
    "50106": "auth",
    "50107": "auth",
    "50108": "auth",
    "50109": "auth",
    "50110": "auth",
    "50111": "auth",
    "50112": "auth",
    "50113": "auth",
    "50114": "auth",
    "50115": "auth",
    "51000": "bad_request",
    "51008": "insufficient_funds",
    "51131": "insufficient_funds",
    "51603": "order_not_found",
    "51604": "order_not_found",
}

RETRYABLE_CATEGORIES = {"rate_limit", "exchange_not_available", "exchange_error"}


def _backoff(attempt: int) -> float:
    return min(8.0, 2.0 ** attempt)


def _map_order_state(state: str) -> str:
    if state in ("live", "partially_filled"):
        return "open"
    if state == "filled":
        return "filled"
    if state in ("canceled", "mmp_canceled"):
        return "cancelled"
    return state or ""


def _data_error(row: dict) -> Optional[dict]:
    code = str(row.get("sCode") or row.get("code") or "")
    if not code or code == "0":
        return None
    category = OKX_ERROR_CATEGORY.get(code, "unknown")
    return {
        "success": False,
        "error": row,
        "error_code": code,
        "error_category": category,
        "is_retryable": category in RETRYABLE_CATEGORIES,
    }


class OKXClient:
    """OKX v5 REST client with the methods expected by StrategyEngine."""

    BASE_URL = "https://openapi.okx.com"
    supports_ws = False

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        testnet: bool = False,
        instrument_name: Optional[str] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not api_key or not api_secret or not passphrase:
            raise ValueError("api_key, api_secret and passphrase are required")

        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testnet = testnet
        self.instrument_name = instrument_name or os.environ.get("OKX_INSTRUMENT_NAME", "BTC-USDC")
        self.base_url = os.environ.get("OKX_REST_BASE_URL", self.BASE_URL).rstrip("/")
        self._session = session or requests.Session()
        self._last_auth_error: Optional[str] = None

        base_ccy, quote_ccy = self._split_instrument(self.instrument_name)
        self.base_currency = base_ccy
        self.quote_currency = quote_ccy

    @staticmethod
    def _split_instrument(instrument_name: str) -> tuple[str, str]:
        parts = instrument_name.replace("_", "-").upper().split("-")
        if len(parts) >= 2:
            return parts[0], parts[1]
        return "BTC", "USDC"

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        payload = f"{timestamp}{method.upper()}{request_path}{body}"
        digest = hmac.new(self.api_secret.encode(), payload.encode(), hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    def _headers(self, timestamp: str, method: str, request_path: str, body: str = "") -> dict:
        headers = {
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, request_path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
        }
        if self.testnet:
            headers["x-simulated-trading"] = "1"
        return headers

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict] = None,
        body: Optional[dict] = None,
        need_auth: bool = False,
        max_retries: int = 4,
    ) -> dict:
        method = method.upper()
        params = params or {}
        body_text = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False) if body else ""
        query = urlencode(params)
        request_path = f"{path}?{query}" if query else path
        url = f"{self.base_url}{path}"

        last_result = {"success": False, "error": "unknown", "error_category": "unknown"}
        for attempt in range(max_retries):
            try:
                headers = {}
                if need_auth:
                    timestamp = self._timestamp()
                    headers = self._headers(timestamp, method, request_path, body_text)
                response = self._session.request(
                    method,
                    url,
                    headers=headers,
                    params=params if method == "GET" else None,
                    data=body_text if body_text else None,
                    timeout=20,
                )
                if response.status_code != 200:
                    last_result = {
                        "success": False,
                        "error": f"HTTP {response.status_code}",
                        "error_category": "exchange_error",
                        "is_retryable": True,
                    }
                    if attempt < max_retries - 1:
                        time.sleep(_backoff(attempt))
                        continue
                    return last_result

                payload = response.json()
                code = str(payload.get("code", ""))
                if code == "0":
                    self._last_auth_error = None
                    return {"success": True, "result": payload.get("data", [])}

                category = OKX_ERROR_CATEGORY.get(code, "unknown")
                is_retryable = category in RETRYABLE_CATEGORIES
                message = payload.get("msg") or str(payload)
                if category == "auth":
                    self._last_auth_error = message
                last_result = {
                    "success": False,
                    "error": payload,
                    "error_code": code,
                    "error_category": category,
                    "is_retryable": is_retryable,
                }
                logger.warning("OKX API error [%s %s] code=%s cat=%s: %s", method, path, code, category, message)
                if is_retryable and attempt < max_retries - 1:
                    time.sleep(_backoff(attempt))
                    continue
                return last_result
            except (requests.exceptions.RequestException, ValueError) as exc:
                last_result = {
                    "success": False,
                    "error": str(exc),
                    "error_category": "exchange_error",
                    "is_retryable": True,
                }
                if attempt < max_retries - 1:
                    time.sleep(_backoff(attempt))
                    continue
                return last_result

        return last_result

    def check_connection(self) -> dict:
        result = {
            "connected": False,
            "testnet": self.testnet,
            "auth_error": self._last_auth_error,
            "exchange": "okx",
        }
        response = self._request("GET", "/api/v5/account/balance", need_auth=True)
        if response["success"]:
            result["connected"] = True
            result["auth_error"] = None
        else:
            result["error"] = str(response.get("error", "unknown"))
            result["auth_error"] = self._last_auth_error
        return result

    def get_index_price(self, index_name: str = "BTC-USDC") -> Optional[float]:
        instrument_name = (index_name or self.instrument_name).replace("_", "-").upper()
        response = self._request(
            "GET", "/api/v5/market/index-tickers", params={"instId": instrument_name}
        )
        if not response["success"] or not response["result"]:
            return None
        return _f(response["result"][0].get("idxPx"))

    def get_instruments(self, currency: str = "BTC", kind: str = "spot") -> list[dict]:
        params = {"instType": "SPOT"}
        if self.instrument_name:
            params["instId"] = self.instrument_name
        response = self._request("GET", "/api/v5/public/instruments", params=params)
        if not response["success"]:
            return []
        instruments = []
        for instrument in response["result"]:
            if currency and instrument.get("baseCcy") != currency.upper():
                continue
            instruments.append({
                "instrument_name": instrument.get("instId", ""),
                "contract_size": _f(instrument.get("lotSz"), 0.00000001),
                "min_trade_amount": _f(instrument.get("minSz"), 0.00000001),
                "tick_size": _f(instrument.get("tickSz"), 0.01),
                "base_currency": instrument.get("baseCcy", ""),
                "quote_currency": instrument.get("quoteCcy", ""),
                "raw": instrument,
            })
        return instruments

    def get_ticker(self, instrument_name: str) -> Optional[dict]:
        response = self._request("GET", "/api/v5/market/ticker", params={"instId": instrument_name})
        if response["success"] and response["result"]:
            return response["result"][0]
        return None

    def get_tradingview_chart_data(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str = "1D",
    ) -> Optional[dict]:
        bar = self._normalize_bar(resolution)
        response = self._request(
            "GET",
            "/api/v5/market/candles",
            params={"instId": instrument_name, "bar": bar, "limit": "100"},
        )
        if response["success"]:
            return self._normalize_candles(response["result"])
        return None

    def get_index_chart_data(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str = "5",
        limit: int = 100,
    ) -> Optional[dict]:
        """Get OKX index candles in the normalized strategy data shape."""
        params = {
            "instId": instrument_name.replace("_", "-").upper(),
            "bar": self._normalize_bar(resolution),
            "after": str(int(end_timestamp)),
            "before": str(int(start_timestamp)),
            "limit": str(max(1, min(int(limit), 100))),
        }
        response = self._request(
            "GET", "/api/v5/market/index-candles", params=params,
        )
        if response["success"]:
            start_ms = int(start_timestamp)
            end_ms = int(end_timestamp)
            candles = [
                candle for candle in response["result"]
                if candle and start_ms <= int(candle[0]) <= end_ms
            ]
            return self._normalize_index_candles(candles)
        return None

    @staticmethod
    def _normalize_bar(resolution: str) -> str:
        mapping = {
            "1": "1m",
            "3": "3m",
            "5": "5m",
            "15": "15m",
            "30": "30m",
            "60": "1H",
            "1D": "1D",
        }
        return mapping.get(str(resolution), str(resolution))

    @staticmethod
    def _normalize_candles(candles: list[list]) -> dict:
        sorted_candles = sorted(candles, key=lambda candle: int(candle[0]))
        ticks = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        for candle in sorted_candles:
            if len(candle) < 6:
                continue
            ticks.append(int(candle[0]))
            opens.append(_f(candle[1]))
            highs.append(_f(candle[2]))
            lows.append(_f(candle[3]))
            closes.append(_f(candle[4]))
            volumes.append(_f(candle[5]))
        return {
            "ticks": ticks,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "status": "ok",
        }

    @staticmethod
    def _normalize_index_candles(candles: list[list]) -> dict:
        sorted_candles = sorted(candles, key=lambda candle: int(candle[0]))
        ticks = []
        opens = []
        highs = []
        lows = []
        closes = []
        confirmations = []
        for candle in sorted_candles:
            if len(candle) < 6:
                continue
            ticks.append(int(candle[0]))
            opens.append(_f(candle[1]))
            highs.append(_f(candle[2]))
            lows.append(_f(candle[3]))
            closes.append(_f(candle[4]))
            confirmations.append(str(candle[5]) == "1")
        return {
            "ticks": ticks,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "confirm": confirmations,
            "status": "ok",
        }

    def get_account_summary(self, currency: str = "USDC", extended: bool = True) -> Optional[dict]:
        response = self._request(
            "GET",
            "/api/v5/account/balance",
            params={"ccy": currency.upper()},
            need_auth=True,
        )
        if not response["success"] or not response["result"]:
            return None
        details = response["result"][0].get("details", [])
        for detail in details:
            if detail.get("ccy") == currency.upper():
                available = _f(detail.get("availBal"), _f(detail.get("cashBal")))
                cash_balance = _f(detail.get("cashBal"), available)
                equity = _f(detail.get("eq"), cash_balance)
                return {
                    "currency": currency.upper(),
                    "balance": available,
                    "available": available,
                    "total_balance": equity,
                    "equity": equity,
                    "raw": detail,
                }
        return {
            "currency": currency.upper(),
            "balance": 0.0,
            "available": 0.0,
            "total_balance": 0.0,
            "equity": 0.0,
        }

    def get_positions(self, currency: str = "BTC", kind: str = "any") -> list[dict]:
        return []

    def buy(
        self,
        instrument_name: str,
        amount: float,
        order_type: str = "market",
        label: Optional[str] = None,
        price: Optional[float] = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> dict:
        return self._place_order("buy", instrument_name, amount, order_type, label, price, post_only)

    def sell(
        self,
        instrument_name: str,
        amount: float,
        order_type: str = "market",
        label: Optional[str] = None,
        price: Optional[float] = None,
        reduce_only: bool = False,
        post_only: bool = False,
    ) -> dict:
        return self._place_order("sell", instrument_name, amount, order_type, label, price, post_only)

    def _place_order(
        self,
        side: str,
        instrument_name: str,
        amount: float,
        order_type: str,
        label: Optional[str],
        price: Optional[float],
        post_only: bool,
    ) -> dict:
        self._set_instrument(instrument_name)
        body = {
            "instId": instrument_name,
            "tdMode": "cash",
            "side": side,
            "ordType": "post_only" if post_only else order_type,
            "sz": _format_decimal(amount),
        }
        if label:
            body["clOrdId"] = self._normalize_client_order_id(label)
        if price is not None and body["ordType"] in ("limit", "post_only", "fok", "ioc"):
            body["px"] = _format_decimal(price)
        response = self._request("POST", "/api/v5/trade/order", body=body, need_auth=True)
        if response["success"]:
            order = response["result"][0] if response["result"] else {}
            error = _data_error(order)
            if error:
                return error
            order.setdefault("instId", instrument_name)
            order.setdefault("side", side)
            order.setdefault("sz", body["sz"])
            order.setdefault("px", body.get("px", ""))
            order.setdefault("clOrdId", body.get("clOrdId", ""))
            return {"success": True, "result": order}
        return response

    @staticmethod
    def _normalize_client_order_id(label: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9]", "", label)[:32]
        return normalized or f"okx{int(time.time())}"

    def get_order_state(self, order_id: str) -> dict:
        response = self._request(
            "GET",
            "/api/v5/trade/order",
            params={"instId": self.instrument_name, "ordId": order_id},
            need_auth=True,
        )
        if response["success"] and response["result"]:
            return {"success": True, "result": self._normalize_order(response["result"][0])}
        return response

    def cancel_order(self, order_id: str) -> dict:
        response = self._request(
            "POST",
            "/api/v5/trade/cancel-order",
            body={"instId": self.instrument_name, "ordId": order_id},
            need_auth=True,
        )
        if response["success"]:
            result = response["result"][0] if response["result"] else {}
            error = _data_error(result)
            if error:
                return error
            return {"success": True, "result": result}
        return response

    def cancel_all(self, instrument_name: str = "") -> dict:
        return self.cancel_all_by_instrument(instrument_name or self.instrument_name)

    def cancel_all_by_instrument(self, instrument_name: str) -> dict:
        self._set_instrument(instrument_name)
        orders = self.get_open_orders(instrument_name)
        failures = []
        for order in orders:
            result = self.cancel_order(order["order_id"])
            if not result["success"]:
                failures.append({"order_id": order["order_id"], "result": result})
        if failures:
            return {"success": False, "error": failures, "error_category": "exchange_error"}
        return {"success": True, "result": {"cancelled": len(orders)}}

    def get_open_orders(self, instrument_name: Optional[str] = None) -> list[dict]:
        if instrument_name:
            self._set_instrument(instrument_name)
        response = self._request(
            "GET",
            "/api/v5/trade/orders-pending",
            params={"instType": "SPOT", "instId": self.instrument_name},
            need_auth=True,
        )
        if not response["success"]:
            return []
        return [self._normalize_order(order) for order in response["result"]]

    def get_order_book(self, instrument_name: str, depth: int = 5) -> Optional[dict]:
        response = self._request(
            "GET",
            "/api/v5/market/books",
            params={"instId": instrument_name, "sz": str(depth)},
        )
        if response["success"] and response["result"]:
            return response["result"][0]
        return None

    def _set_instrument(self, instrument_name: str) -> None:
        self.instrument_name = instrument_name
        self.base_currency, self.quote_currency = self._split_instrument(instrument_name)

    @staticmethod
    def _normalize_order(order: dict) -> dict:
        return {
            "order_id": order.get("ordId", order.get("order_id", "")),
            "order_state": _map_order_state(order.get("state", order.get("order_state", ""))),
            "filled_amount": _f(order.get("accFillSz", order.get("filled_amount"))),
            "average_price": _f(order.get("avgPx", order.get("average_price"))),
            "direction": order.get("side", order.get("direction", "")),
            "instrument_name": order.get("instId", order.get("instrument_name", "")),
            "amount": _f(order.get("sz", order.get("amount"))),
            "price": _f(order.get("px", order.get("price"))),
            "label": order.get("clOrdId", order.get("label", "")),
            "creation_timestamp": order.get("cTime", order.get("creation_timestamp", "")),
            "raw_order": order,
        }

    @staticmethod
    def parse_order_result(result_data: dict) -> dict:
        order = result_data.get("order", result_data)
        normalized = OKXClient._normalize_order(order)
        average_price = normalized["average_price"]
        if average_price <= 0:
            average_price = _f(order.get("fillPx"), normalized["price"])
        filled_amount = normalized["filled_amount"]
        fills = []
        if _f(order.get("fillSz")) > 0:
            fills.append({
                "trade_id": order.get("tradeId"),
                "amount": _f(order.get("fillSz")),
                "price": _f(order.get("fillPx")),
                "fee": _f(order.get("fee")),
                "fee_currency": order.get("feeCcy"),
                "timestamp": order.get("fillTime"),
            })
        return {
            "order_id": normalized["order_id"],
            "state": normalized["order_state"],
            "filled_amount": filled_amount,
            "average_price": average_price,
            "total_cost": round(filled_amount * average_price, 2),
            "label": normalized["label"],
            "direction": normalized["direction"],
            "instrument_name": normalized["instrument_name"],
            "fills": fills,
            "raw_order": order,
        }
