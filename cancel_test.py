"""临时压测脚本: 取消指定 order_id, 验证策略自愈。testnet 零风险。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def load_env():
    d = {}
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    d[k.strip()] = v.strip()
    return d


e = load_env()
from okx_api import OKXClient

testnet = e.get("OKX_TESTNET", "1") == "1"
instrument_name = e.get("OKX_INSTRUMENT_NAME", "BTC-USDC")
c = OKXClient(
    e["OKX_API_KEY"],
    e["OKX_API_SECRET"],
    e["OKX_PASSPHRASE"],
    testnet=testnet,
    instrument_name=instrument_name,
)
oid = sys.argv[1] if len(sys.argv) > 1 else ""
if not oid:
    raise SystemExit("Usage: python cancel_test.py <okx_order_id>")
r = c.cancel_order(oid)
print("CANCEL_RESULT:", r)
