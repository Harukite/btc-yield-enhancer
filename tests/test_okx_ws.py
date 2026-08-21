import unittest

from okx_ws import OKXWSClient


class OKXWSClientTests(unittest.TestCase):
    def test_ticker_message_emits_existing_index_channel_shape(self):
        messages = []
        client = OKXWSClient("key", "secret", "passphrase", instrument_name="BTC-USDC", callback=messages.append)

        client._handle_message({
            "arg": {"channel": "tickers", "instId": "BTC-USDC"},
            "data": [{"last": "64250.5"}],
        })

        self.assertEqual(client.cached_index_price, 64250.5)
        self.assertEqual(messages, [{
            "channel": "ticker.BTC-USDC.index",
            "data": {"index_price": 64250.5, "idx": 64250.5},
        }])

    def test_account_message_emits_existing_portfolio_channel_shape(self):
        messages = []
        client = OKXWSClient("key", "secret", "passphrase", instrument_name="BTC-USDC", callback=messages.append)

        client._handle_message({
            "arg": {"channel": "account"},
            "data": [{
                "details": [
                    {"ccy": "BTC", "availBal": "0.125"},
                    {"ccy": "USDC", "availBal": "2500.75"},
                ]
            }],
        })

        self.assertEqual(client.cached_btc_balance, 0.125)
        self.assertEqual(client.cached_usdc_balance, 2500.75)
        self.assertEqual(messages, [
            {"channel": "user.portfolio.btc", "data": {"balance": 0.125}},
            {"channel": "user.portfolio.usdc", "data": {"balance": 2500.75}},
        ])


if __name__ == "__main__":
    unittest.main()
