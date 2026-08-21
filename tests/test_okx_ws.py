import ssl
import asyncio
import json
import unittest

import certifi

from okx_ws import OKXWSClient


class OKXWSClientTests(unittest.TestCase):
    def test_websocket_tls_uses_certifi_ca_bundle(self):
        client = OKXWSClient("key", "secret", "passphrase")

        context = client._ssl_context()

        self.assertIsInstance(context, ssl.SSLContext)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertEqual(context.get_ca_certs(), ssl.create_default_context(cafile=certifi.where()).get_ca_certs())

    def test_index_ticker_message_emits_existing_index_channel_shape(self):
        messages = []
        client = OKXWSClient("key", "secret", "passphrase", instrument_name="BTC-USDC", callback=messages.append)

        client._handle_message({
            "arg": {"channel": "index-tickers", "instId": "BTC-USDC"},
            "data": [{"idxPx": "64250.5", "last": "64200.0"}],
        })

        self.assertEqual(client.cached_index_price, 64250.5)
        self.assertEqual(messages, [{
            "channel": "ticker.BTC-USDC.index",
            "data": {"index_price": 64250.5, "idx": 64250.5},
        }])

    def test_public_subscription_uses_alphanumeric_request_id(self):
        class FakeWebSocket:
            def __init__(self):
                self.messages = []

            async def send(self, message):
                self.messages.append(json.loads(message))

        client = OKXWSClient("key", "secret", "passphrase", instrument_name="BTC-USDC")
        websocket = FakeWebSocket()

        asyncio.run(client._subscribe_public(websocket))

        self.assertRegex(websocket.messages[0]["id"], r"^[A-Za-z0-9]+$")

    def test_demo_public_subscription_uses_available_usdt_index(self):
        class FakeWebSocket:
            def __init__(self):
                self.messages = []

            async def send(self, message):
                self.messages.append(json.loads(message))

        client = OKXWSClient(
            "key", "secret", "passphrase",
            testnet=True,
            instrument_name="BTC-USDC",
        )
        websocket = FakeWebSocket()

        asyncio.run(client._subscribe_public(websocket))

        self.assertEqual(
            websocket.messages[0]["args"][0]["instId"], "BTC-USDT",
        )

    def test_account_message_emits_existing_portfolio_channel_shape(self):
        messages = []
        client = OKXWSClient("key", "secret", "passphrase", instrument_name="BTC-USDC", callback=messages.append)

        client._handle_message({
            "arg": {"channel": "account"},
            "data": [{
                "details": [
                    {"ccy": "BTC", "availBal": "0.125", "eq": "0.2"},
                    {"ccy": "USDC", "availBal": "2500.75", "eq": "2600.0"},
                ]
            }],
        })

        self.assertEqual(client.cached_btc_balance, 0.2)
        self.assertEqual(client.cached_btc_available, 0.125)
        self.assertEqual(client.cached_usdc_balance, 2600.0)
        self.assertEqual(client.cached_usdc_available, 2500.75)
        self.assertEqual(messages, [
            {"channel": "user.portfolio.btc", "data": {
                "balance": 0.2, "available": 0.125, "equity": 0.2,
            }},
            {"channel": "user.portfolio.usdc", "data": {
                "balance": 2600.0, "available": 2500.75, "equity": 2600.0,
            }},
        ])

    def test_order_message_emits_normalized_fill_event(self):
        messages = []
        client = OKXWSClient("key", "secret", "passphrase", callback=messages.append)

        client._handle_message({
            "arg": {"channel": "orders", "instId": "BTC-USDC"},
            "data": [{
                "ordId": "order-1",
                "state": "partially_filled",
                "side": "buy",
                "instId": "BTC-USDC",
                "clOrdId": "maker_buy",
                "sz": "0.002",
                "px": "64000",
                "accFillSz": "0.001",
                "fillSz": "0.001",
                "fillPx": "63990",
                "avgPx": "63990",
                "uTime": "12345",
            }],
        })

        self.assertEqual(messages, [{
            "channel": "user.order",
            "data": {
                "order_id": "order-1",
                "state": "partially_filled",
                "side": "buy",
                "instrument_name": "BTC-USDC",
                "label": "maker_buy",
                "amount": 0.002,
                "price": 64000.0,
                "filled_amount": 0.001,
                "fill_amount": 0.001,
                "fill_price": 63990.0,
                "average_price": 63990.0,
                "timestamp": "12345",
            },
        }])

    def test_private_subscription_includes_spot_orders(self):
        class FakeWebSocket:
            def __init__(self):
                self.messages = []

            async def send(self, message):
                self.messages.append(json.loads(message))

        client = OKXWSClient("key", "secret", "passphrase", instrument_name="BTC-USDC")
        websocket = FakeWebSocket()

        asyncio.run(client._subscribe_private(websocket))

        self.assertIn({
            "channel": "orders",
            "instType": "SPOT",
            "instId": "BTC-USDC",
        }, websocket.messages[0]["args"])


if __name__ == "__main__":
    unittest.main()
