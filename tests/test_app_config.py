import os
import subprocess
import sys
import tempfile
import textwrap
import unittest


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


if __name__ == "__main__":
    unittest.main()
