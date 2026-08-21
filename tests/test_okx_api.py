import json
import unittest

from okx_api import OKXClient


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self):
        self.calls = []

    def request(self, method, url, headers=None, params=None, data=None, timeout=None):
        self.calls.append({
            "method": method,
            "url": url,
            "headers": headers or {},
            "params": params or {},
            "data": data,
            "timeout": timeout,
        })
        return FakeResponse({"code": "0", "msg": "", "data": [{"ordId": "123"}]})


class RejectSession:
    def request(self, method, url, headers=None, params=None, data=None, timeout=None):
        return FakeResponse({
            "code": "0",
            "msg": "",
            "data": [{"sCode": "51008", "sMsg": "Insufficient balance"}],
        })


class IndexSession:
    def __init__(self):
        self.calls = []

    def request(self, method, url, headers=None, params=None, data=None, timeout=None):
        self.calls.append({"method": method, "url": url, "params": params or {}})
        if url.endswith("/market/index-tickers"):
            return FakeResponse({
                "code": "0",
                "msg": "",
                "data": [{"instId": "BTC-USDC", "idxPx": "64250.5"}],
            })
        if url.endswith("/market/index-candles"):
            return FakeResponse({
                "code": "0",
                "msg": "",
                "data": [
                    ["3000", "103", "104", "102", "103.5", "1"],
                    ["2000", "101", "102", "100", "101.5", "1"],
                    ["1000", "100", "101", "99", "100.5", "1"],
                    ["5000", "105", "106", "104", "105.5", "1"],
                ],
            })
        raise AssertionError(f"unexpected URL: {url}")


class BalanceSession:
    def request(self, method, url, headers=None, params=None, data=None, timeout=None):
        return FakeResponse({
            "code": "0",
            "msg": "",
            "data": [{
                "details": [{
                    "ccy": "USDC",
                    "availBal": "90.5",
                    "cashBal": "100.0",
                    "eq": "125.75",
                }],
            }],
        })


class OKXClientTests(unittest.TestCase):
    def test_index_price_uses_okx_index_ticker(self):
        session = IndexSession()
        client = OKXClient("key", "secret", "passphrase", session=session)

        price = client.get_index_price("BTC-USDC")

        self.assertEqual(price, 64250.5)
        self.assertEqual(session.calls[0]["url"], "https://openapi.okx.com/api/v5/market/index-tickers")
        self.assertEqual(session.calls[0]["params"], {"instId": "BTC-USDC"})

    def test_index_candles_use_index_endpoint_and_confirm_field(self):
        session = IndexSession()
        client = OKXClient("key", "secret", "passphrase", session=session)

        normalized = client.get_index_chart_data(
            "BTC-USDC", 1000, 4000, "5", limit=3,
        )

        self.assertEqual(normalized["ticks"], [1000, 2000, 3000])
        self.assertEqual(normalized["open"], [100.0, 101.0, 103.0])
        self.assertEqual(normalized["confirm"], [True, True, True])
        self.assertEqual(session.calls[0]["url"], "https://openapi.okx.com/api/v5/market/index-candles")
        self.assertEqual(session.calls[0]["params"], {
            "instId": "BTC-USDC",
            "bar": "5m",
            "after": "4000",
            "before": "1000",
            "limit": "3",
        })

    def test_account_summary_preserves_available_and_equity_balances(self):
        client = OKXClient("key", "secret", "passphrase", session=BalanceSession())

        summary = client.get_account_summary("USDC")

        self.assertEqual(summary["balance"], 90.5)
        self.assertEqual(summary["available"], 90.5)
        self.assertEqual(summary["total_balance"], 125.75)
        self.assertEqual(summary["equity"], 125.75)

    def test_buy_post_only_order_uses_okx_maker_order_type(self):
        session = FakeSession()
        client = OKXClient("key", "secret", "passphrase", session=session)

        result = client.buy(
            "BTC-USDT",
            amount=0.001,
            order_type="limit",
            label="maker_buy",
            price=60000,
            post_only=True,
        )

        body = json.loads(session.calls[0]["data"])
        self.assertTrue(result["success"])
        self.assertEqual(session.calls[0]["method"], "POST")
        self.assertEqual(body["instId"], "BTC-USDT")
        self.assertEqual(body["tdMode"], "cash")
        self.assertEqual(body["side"], "buy")
        self.assertEqual(body["ordType"], "post_only")
        self.assertEqual(body["px"], "60000")
        self.assertEqual(body["sz"], "0.001")
        self.assertEqual(body["clOrdId"], "makerbuy")

    def test_order_level_rejection_is_not_treated_as_success(self):
        client = OKXClient("key", "secret", "passphrase", session=RejectSession())

        result = client.buy("BTC-USDC", amount=0.001, order_type="limit", price=60000, post_only=True)

        self.assertFalse(result["success"])
        self.assertEqual(result["error_code"], "51008")
        self.assertEqual(result["error_category"], "insufficient_funds")

    def test_order_result_is_normalized_for_existing_strategy_engine(self):
        normalized = OKXClient.parse_order_result({
            "ordId": "abc",
            "instId": "BTC-USDT",
            "side": "sell",
            "state": "partially_filled",
            "accFillSz": "0.002",
            "avgPx": "65000.5",
            "px": "65100",
            "sz": "0.003",
            "clOrdId": "maker_sell",
        })

        self.assertEqual(normalized["order_id"], "abc")
        self.assertEqual(normalized["state"], "open")
        self.assertEqual(normalized["filled_amount"], 0.002)
        self.assertEqual(normalized["average_price"], 65000.5)
        self.assertEqual(normalized["direction"], "sell")
        self.assertEqual(normalized["instrument_name"], "BTC-USDT")

    def test_candles_are_normalized_to_existing_chart_shape(self):
        candles = [
            ["3000", "103", "104", "102", "103.5", "1", "103.5", "103.5", "1"],
            ["2000", "101", "102", "100", "101.5", "1", "101.5", "101.5", "1"],
            ["1000", "100", "101", "99", "100.5", "1", "100.5", "100.5", "1"],
        ]

        normalized = OKXClient._normalize_candles(candles)

        self.assertEqual(normalized["ticks"], [1000, 2000, 3000])
        self.assertEqual(normalized["open"], [100.0, 101.0, 103.0])
        self.assertEqual(normalized["close"], [100.5, 101.5, 103.5])


if __name__ == "__main__":
    unittest.main()
