import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch


class AppConfigTests(unittest.TestCase):
    def test_app_imports_without_okx_credentials(self):
        env = os.environ.copy()
        for key in ("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"):
            env.pop(key, None)
        with tempfile.TemporaryDirectory() as data_dir:
            env["DATA_DIR"] = data_dir
            script = textwrap.dedent(
                """
                import app
                client = app.app.test_client()
                resp = client.post('/btc-enhancer/api/init')
                print(resp.status_code)
                print(resp.get_json()['success'])
                """
            )
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("503", result.stdout)
        self.assertIn("False", result.stdout)

    def test_init_reports_failure_when_engine_is_not_ready(self):
        import app as app_module

        class FailedEngine:
            def __init__(self, *args, **kwargs):
                self._running = False
                self.status = "stopped"

            def initialize(self):
                self._running = True
                self.status = "error"
                return True

        previous_engine = app_module.engine
        try:
            app_module.engine = None
            with patch.object(app_module, "StrategyEngine", FailedEngine), \
                    patch.object(app_module, "CONFIG_ERROR", ""), \
                    patch.object(app_module, "API_TOKEN", ""), \
                    patch.object(app_module.pytime, "sleep", return_value=None):
                response = app_module.app.test_client().post("/btc-enhancer/api/init")
        finally:
            app_module.engine = previous_engine

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.get_json(), {
            "success": False,
            "message": "Initialization failed",
            "status": "error",
        })

    def test_connection_reports_equity_and_available_balances(self):
        import app as app_module

        class ConnectedClient:
            def __init__(self, *args, **kwargs):
                pass

            def check_connection(self):
                return {"connected": True}

            def get_index_price(self, instrument_name):
                return 64000.0

            def get_account_summary(self, currency):
                if currency == "USDC":
                    return {"equity": 125.0, "available": 90.0}
                return {"equity": 0.2, "available": 0.125}

        with patch.object(app_module, "OKXClient", ConnectedClient), \
                patch.object(app_module, "_credentials_ready", return_value=True):
            response = app_module.app.test_client().get(
                "/btc-enhancer/api/test-connection"
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["mainnet"]["usdc_balance"], 125.0)
        self.assertEqual(payload["mainnet"]["usdc_available"], 90.0)
        self.assertEqual(payload["mainnet"]["btc_balance"], 0.2)
        self.assertEqual(payload["mainnet"]["btc_available"], 0.125)


if __name__ == "__main__":
    unittest.main()
