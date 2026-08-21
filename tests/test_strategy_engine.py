import math
import tempfile
import unittest
from unittest.mock import patch

from strategy_engine import StrategyEngine


class StrategyEngineTests(unittest.TestCase):
    def test_daily_rv_uses_confirmed_index_candles(self):
        class IndexOnlyClient:
            def get_index_chart_data(self, instrument, start, end, resolution):
                return {
                    "open": [100.0] * 12,
                    "close": [101.0] * 12,
                    "confirm": [True] * 12,
                }

            def get_tradingview_chart_data(self, *args, **kwargs):
                raise AssertionError("RV must use index candles, not market candles")

        with tempfile.NamedTemporaryFile() as state_file, patch(
            "strategy_engine.STATE_FILE", state_file.name
        ):
            engine = StrategyEngine("key", "secret", "passphrase")
            engine.api = IndexOnlyClient()

            rv = engine._calculate_daily_rv()

        self.assertAlmostEqual(rv, 0.01 * math.sqrt(24), places=12)

    def test_balance_protection_uses_available_balance_but_valuation_uses_equity(self):
        class BalanceClient:
            def get_account_summary(self, currency):
                if currency == "USDC":
                    return {"balance": 90.0, "available": 90.0, "equity": 125.0}
                return {"balance": 0.2, "available": 0.125, "equity": 0.2}

        with tempfile.NamedTemporaryFile() as state_file, patch(
            "strategy_engine.STATE_FILE", state_file.name
        ):
            engine = StrategyEngine("key", "secret", "passphrase")
            engine.api = BalanceClient()
            engine._ws_enabled = False
            engine.btc_index_price = 1000.0

            balances = engine._fetch_balances()
            engine.cfg["min_poll_balance_usdc"] = 100.0
            engine._check_funds()

        self.assertEqual(balances["usdc_balance"], 125.0)
        self.assertEqual(balances["usdc_available"], 90.0)
        self.assertEqual(engine.usdc_balance, 125.0)
        self.assertEqual(engine.usdc_available, 90.0)
        self.assertTrue(engine.usdc_insufficient)

    def test_price_rounding_supports_non_decimal_tick_sizes(self):
        with tempfile.NamedTemporaryFile() as state_file, patch(
            "strategy_engine.STATE_FILE", state_file.name
        ):
            engine = StrategyEngine("key", "secret", "passphrase")
            engine.tick_size = 0.05

            rounded = engine._round_price(100.03)

        self.assertEqual(rounded, 100.05)

    def test_order_fill_event_is_idempotent(self):
        with tempfile.NamedTemporaryFile() as state_file, patch(
            "strategy_engine.STATE_FILE", state_file.name
        ):
            engine = StrategyEngine("key", "secret", "passphrase")
            engine._trading_enabled = True
            engine.anchor_price = 64000.0
            engine.btc_index_price = 64000.0
            engine.daily_rv = 0.01
            engine._update_rv = lambda: None
            engine._recalc_thresholds = lambda: None
            engine._fetch_balances = lambda: None
            engine._save_state = lambda: None
            engine.api.cancel_order = lambda order_id: {"success": True}

            event = {
                "channel": "user.order",
                "data": {
                    "order_id": "order-1",
                    "state": "filled",
                    "side": "buy",
                    "instrument_name": "BTC-USDC",
                    "label": "makerbuy",
                    "filled_amount": 0.001,
                    "average_price": 63900.0,
                },
            }
            engine._on_ws_message(event)
            engine._on_ws_message(event)

        self.assertEqual(engine.total_trades, 1)
        self.assertEqual(len(engine.trades), 1)
        self.assertEqual(engine.trades[0]["amount_btc"], 0.001)
        self.assertEqual(engine.anchor_price, 63900.0)

    def test_rest_reconciliation_consumes_new_cumulative_partial_fill(self):
        with tempfile.NamedTemporaryFile() as state_file, patch(
            "strategy_engine.STATE_FILE", state_file.name
        ):
            engine = StrategyEngine("key", "secret", "passphrase")
            engine._trading_enabled = True
            engine._our_buy_id = "order-1"
            engine.anchor_price = 64000.0
            engine.btc_index_price = 64000.0
            engine.daily_rv = 0.01
            engine._update_rv = lambda: None
            engine._recalc_thresholds = lambda: None
            engine._fetch_balances = lambda: None
            engine._save_state = lambda: None
            engine.api.cancel_order = lambda order_id: {"success": True}
            engine.open_orders = [{
                "order_id": "order-1",
                "side": "buy",
                "filled": 0.001,
                "price": 63900.0,
                "average_price": 63890.0,
            }]

            engine._reconcile_open_order_fills()
            engine.open_orders[0]["filled"] = 0.002
            engine.open_orders[0]["average_price"] = 63990.0
            engine._reconcile_open_order_fills()

        self.assertEqual(engine.total_trades, 2)
        self.assertAlmostEqual(
            sum(trade["amount_btc"] for trade in engine.trades), 0.002, places=8,
        )
        self.assertEqual(engine.trades[0]["price"], 63890.0)
        self.assertEqual(engine.trades[1]["price"], 64090.0)

    def test_rest_reconciliation_records_partial_fill_before_cancel(self):
        with tempfile.NamedTemporaryFile() as state_file, patch(
            "strategy_engine.STATE_FILE", state_file.name
        ):
            engine = StrategyEngine("key", "secret", "passphrase")
            engine._our_buy_id = "order-2"
            engine.anchor_price = 64000.0
            engine.btc_index_price = 64000.0
            engine.daily_rv = 0.01
            engine._update_rv = lambda: None
            engine._recalc_thresholds = lambda: None
            engine._fetch_balances = lambda: None
            engine._save_state = lambda: None
            engine.api.get_order_state = lambda order_id: {
                "success": True,
                "result": {
                    "ordId": order_id,
                    "instId": "BTC-USDC",
                    "side": "buy",
                    "state": "canceled",
                    "accFillSz": "0.001",
                    "avgPx": "63880",
                },
            }

            state = engine._reconcile_missing_order("buy", "order-2", 63900.0)

        self.assertEqual(state, "cancelled")
        self.assertEqual(engine.total_trades, 1)
        self.assertEqual(engine.trades[0]["amount_btc"], 0.001)
        self.assertEqual(engine.trades[0]["price"], 63880.0)


if __name__ == "__main__":
    unittest.main()
