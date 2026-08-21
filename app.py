"""
BTC 收益增强策略 - Flask Web 应用

- REST API：策略启停、连接测试
- WebSocket /ws：实时推送策略状态
- 前端仪表盘 HTML
"""

import os
import json
import logging
import logging.handlers
import sys
import threading
import time as pytime

from flask import Flask, jsonify, request, make_response
from flask_sock import Sock

from strategy_engine import StrategyEngine
from okx_api import OKXClient
from runtime_paths import get_runtime_paths


RUNTIME_PATHS = get_runtime_paths()


def _setup_logging():
    """配置日志：同时落盘(logs/btc.log, 滚动)与控制台。

    pythonw 下 stdout 被重定向到 os.devnull, 控制台 handler 无效但无害；
    关键是所有模块日志都通过 root logger 写入 btc.log, 解决可观测性缺口。
    """
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # 避免 reload 场景叠加重复 handler
    try:
        log_dir = RUNTIME_PATHS.log_dir
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "btc.log")
        if not any(isinstance(h, logging.handlers.RotatingFileHandler) for h in root.handlers):
            fh = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
            )
            fh.setFormatter(fmt)
            root.addHandler(fh)
    except Exception as e:
        root.warning("File logging disabled: %s", e)
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) for h in root.handlers):
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        root.addHandler(sh)


_setup_logging()
logger = logging.getLogger(__name__)

ENV_FILE = RUNTIME_PATHS.env_file


def _load_env_file():
    """从 .env 文件加载环境变量（在读取 os.environ 之前调用）"""
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())


_load_env_file()


def _save_env(api_key, api_secret, passphrase):
    """将 OKX 凭证写回 .env 文件，保证重启后不丢失（原子写入，防写一半崩溃损坏）"""
    try:
        os.makedirs(os.path.dirname(ENV_FILE), exist_ok=True)
        lines = []
        if os.path.exists(ENV_FILE):
            with open(ENV_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
        found_key = found_secret = found_passphrase = False
        for i, line in enumerate(lines):
            if line.startswith("OKX_API_KEY="):
                lines[i] = f"OKX_API_KEY={api_key}\n"
                found_key = True
            elif line.startswith("OKX_API_SECRET="):
                lines[i] = f"OKX_API_SECRET={api_secret}\n"
                found_secret = True
            elif line.startswith("OKX_PASSPHRASE="):
                lines[i] = f"OKX_PASSPHRASE={passphrase}\n"
                found_passphrase = True
        if not found_key:
            lines.append(f"OKX_API_KEY={api_key}\n")
        if not found_secret:
            lines.append(f"OKX_API_SECRET={api_secret}\n")
        if not found_passphrase:
            lines.append(f"OKX_PASSPHRASE={passphrase}\n")
        import tempfile
        tmp = tempfile.NamedTemporaryFile(
            mode="w", dir=os.path.dirname(ENV_FILE), delete=False,
            suffix=".tmp", encoding="utf-8",
        )
        try:
            tmp.writelines(lines)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp.close()
            os.replace(tmp.name, ENV_FILE)
        except Exception:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
            raise
        logger.info(".env file updated")
    except Exception as e:
        logger.error("Failed to save .env: %s", e)

# ---------------------------------------------------------------------------
# API 密钥从环境变量读取（不写入代码明文）
# ---------------------------------------------------------------------------
OKX_API_KEY = os.environ.get("OKX_API_KEY", "")
OKX_API_SECRET = os.environ.get("OKX_API_SECRET", "")
OKX_PASSPHRASE = os.environ.get("OKX_PASSPHRASE", "")
OKX_INSTRUMENT_NAME = os.environ.get("OKX_INSTRUMENT_NAME", "BTC-USDC")
USE_TESTNET = os.environ.get("OKX_TESTNET", "1") == "1"

# 可选 API 访问令牌：设置环境变量 API_TOKEN 后，所有写操作(POST)需带 X-API-Token 头
API_TOKEN = os.environ.get("API_TOKEN", "").strip()

CONFIG_ERROR = ""
if not OKX_API_KEY or not OKX_API_SECRET or not OKX_PASSPHRASE:
    CONFIG_ERROR = (
        "请设置环境变量 OKX_API_KEY、OKX_API_SECRET 和 OKX_PASSPHRASE，"
        "或在 DATA_DIR/.env / 项目根目录 .env 文件中配置。"
    )
    logger.error(CONFIG_ERROR)


def _credentials_ready():
    return not CONFIG_ERROR


def _credentials_denied():
    if _credentials_ready():
        return None
    return jsonify({"success": False, "message": CONFIG_ERROR}), 503

# ---------------------------------------------------------------------------
# Flask + WebSocket
# ---------------------------------------------------------------------------
app = Flask(__name__, static_url_path='/btc-enhancer/static', static_folder='static')
sock = Sock(app)

engine: StrategyEngine = None
engine_lock = threading.Lock()
ws_clients = set()         # 已连接的 WebSocket 客户端
ws_clients_lock = threading.Lock()

# ---------------------------------------------------------------------------
# WebSocket 广播
# ---------------------------------------------------------------------------

def broadcast_state(state: dict):
    """向所有连接的 WebSocket 客户端推送状态"""
    payload = json.dumps(state, ensure_ascii=False, default=str)
    # 复制客户端列表后立即解锁，避免发送时阻塞其他操作
    with ws_clients_lock:
        clients = list(ws_clients)
    dead = []
    for client in clients:
        try:
            client.send(payload)
        except Exception:
            dead.append(client)
    if dead:
        with ws_clients_lock:
            for c in dead:
                ws_clients.discard(c)


@sock.route("/btc-enhancer/ws")
def ws_handler(ws):
    """WebSocket 连接处理"""
    with ws_clients_lock:
        ws_clients.add(ws)
    logger.info("WebSocket client connected (%d total)", len(ws_clients))

    # 首次连接立即发送当前状态
    if engine:
        try:
            state = engine.get_state()
            ws.send(json.dumps(state, ensure_ascii=False, default=str))
        except Exception:
            pass

    # 保持连接，等待服务端推送
    try:
        while True:
            msg = ws.receive()
            if msg is None:
                break
    except Exception:
        pass
    finally:
        with ws_clients_lock:
            ws_clients.discard(ws)
        logger.info("WebSocket client disconnected (%d left)", len(ws_clients))


# ---------------------------------------------------------------------------
# 策略状态变更回调（每个策略循环结束时触发）
# ---------------------------------------------------------------------------

def on_state_update(state: dict):
    """策略引擎状态变更时，广播给所有 WebSocket 客户端"""
    broadcast_state(state)


def _require_token():
    """写操作鉴权：配置了 API_TOKEN 时，请求须带 X-API-Token 头且匹配，否则 403。
    未配置 API_TOKEN 则放行（保持本地无鉴权行为）。"""
    if not API_TOKEN:
        return None
    token = request.headers.get("X-API-Token", "").strip()
    if token != API_TOKEN:
        return jsonify({"success": False, "message": "Invalid or missing API token"}), 403
    return None


# ---------------------------------------------------------------------------
# 页面路由
# ---------------------------------------------------------------------------

@app.route("/")
@app.route("/btc-enhancer/")
def index():
    """仪表盘主页"""
    html_path = os.path.join(os.path.dirname(__file__), "static", "dashboard.html")
    with open(html_path, "r", encoding="utf-8") as f:
        content = f.read()
    # 若启用了 API_TOKEN，向前端注入令牌 + fetch 拦截器（自动给所有请求加 X-API-Token 头）
    if API_TOKEN:
        inject = (
            "<script>window.API_TOKEN=" + json.dumps(API_TOKEN) + ";"
            "(function(){var o=window.fetch;window.fetch=function(u,op){op=op||{};op.headers=op.headers||{};"
            "op.headers['X-API-Token']=window.API_TOKEN;return o(u,op);};})();</script>"
        )
        content = content.replace("</body>", inject + "</body>", 1) if "</body>" in content else content + inject
    resp = make_response(content)
    resp.headers["Content-Type"] = "text/html; charset=utf-8"
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------

@app.route("/btc-enhancer/api/status")
def api_status():
    if engine is None:
        return jsonify({"error": "Strategy engine not initialized"}), 503
    with engine_lock:
        state = engine.get_state()
    return jsonify(state)


@app.route("/btc-enhancer/api/init", methods=["POST"])
def api_init():
    """初始化引擎（连接+拉数据）。如果引擎已在运行，保持不动。"""
    global engine
    denied = _require_token()
    if denied:
        return denied
    denied = _credentials_denied()
    if denied:
        return denied
    with engine_lock:
        if engine and engine._running:
            # 引擎已经在跑（不论是否交易中）→ 保持现状，不碰
            return jsonify({"success": True, "message": "Engine already running", "status": engine.status})
        # 没有引擎 → 新建并初始化（就绪状态，不交易）
        body = request.get_json(silent=True) or {}
        use_testnet = body.get("testnet", USE_TESTNET)
        engine = StrategyEngine(
            OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE,
            config={"instrument_name": OKX_INSTRUMENT_NAME, "index_name": OKX_INSTRUMENT_NAME},
            testnet=use_testnet,
            state_callback=on_state_update,
        )
        if not engine.initialize():
            return jsonify({"success": False, "message": "Initialization failed"}), 500
    # 等数据就绪再返回
    import time as pytime
    for _ in range(20):
        if engine.status == "ready":
            break
        pytime.sleep(0.5)
    return jsonify({"success": True, "message": "Engine initialized", "status": engine.status})


@app.route("/btc-enhancer/api/start", methods=["POST"])
def api_start():
    """启动交易"""
    global engine
    denied = _require_token()
    if denied:
        return denied
    denied = _credentials_denied()
    if denied:
        return denied
    with engine_lock:
        # 如果引擎不存在或已停止，先初始化
        if engine is None or not engine._running:
            if engine:
                old_anchor = engine.anchor_price
            else:
                old_anchor = None
            engine = StrategyEngine(
                OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE,
                config={"instrument_name": OKX_INSTRUMENT_NAME, "index_name": OKX_INSTRUMENT_NAME},
                testnet=USE_TESTNET,
                state_callback=on_state_update,
            )
            if old_anchor and old_anchor > 0:
                engine.anchor_price = old_anchor
            if not engine.initialize():
                return jsonify({"success": False, "message": "Initialization failed"}), 500
        if engine._trading_enabled:
            return jsonify({"success": False, "message": "Already trading"})
        success = engine.start()
    return jsonify({
        "success": success,
        "message": "Trading started" if success else "Failed to start trading",
    })


@app.route("/btc-enhancer/api/stop", methods=["POST"])
def api_stop():
    denied = _require_token()
    if denied:
        return denied
    if engine is None:
        return jsonify({"error": "No strategy running"}), 400
    engine.stop()
    return jsonify({"success": True, "message": "Strategy stopping"})


@app.route("/btc-enhancer/api/credentials", methods=["GET", "POST"])
def api_credentials():
    """GET: 返回当前 API 凭证（ID 脱敏）；POST: 更新凭证并重建连接"""
    global OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE, engine

    if request.method == "GET":
        masked = OKX_API_KEY[:4] + "****" if len(OKX_API_KEY) > 4 else "****"
        return jsonify({
            "client_id_masked": masked,
            "testnet": USE_TESTNET,
            "exchange": "okx",
            "instrument_name": OKX_INSTRUMENT_NAME,
        })

    # POST — 更新凭证，停旧引擎，重建连接
    denied = _require_token()
    if denied:
        return denied
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "No data"}), 400

    new_id = data.get("client_id", "").strip()
    new_secret = data.get("client_secret", "").strip()
    new_passphrase = data.get("passphrase", "").strip()

    if not new_id or not new_secret or not new_passphrase:
        return jsonify({"success": False, "message": "API Key、Secret 和 Passphrase 不能为空"}), 400

    with engine_lock:
        if engine:
            engine.stop()
            engine = None

        OKX_API_KEY = new_id
        OKX_API_SECRET = new_secret
        OKX_PASSPHRASE = new_passphrase

        # 写回 .env 文件，保证重启后不丢失
        _save_env(new_id, new_secret, new_passphrase)

        logger.info("API credentials updated, reinitializing...")
        engine = StrategyEngine(
            OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE,
            config={"instrument_name": OKX_INSTRUMENT_NAME, "index_name": OKX_INSTRUMENT_NAME},
            testnet=USE_TESTNET,
            state_callback=on_state_update,
        )
        if not engine.initialize():
            return jsonify({"success": False, "message": "新凭证连接失败"}), 500

    return jsonify({"success": True, "message": "已切换凭证并重新连接"})


@app.route("/btc-enhancer/api/params", methods=["GET", "POST"])
def api_params():
    """GET: 返回当前可编辑参数列表  POST: 运行时修改参数"""
    global engine

    if request.method == "GET":
        if engine is None:
            return jsonify({"editable": False, "message": "Engine not initialized"}), 503
        with engine_lock:
            cfg = engine.cfg
            return jsonify({
                "editable": True,
                "anchor_price": engine.anchor_price,
                "trade_size_usdc": cfg["trade_size_usdc"],
                "rv_min": cfg["rv_min"],
                "rv_max": cfg["rv_max"],
                "rv_update_interval_minutes": cfg.get("rv_update_interval_minutes", 60),
                "poll_interval": cfg["poll_interval"],
                "cooldown_seconds": cfg.get("cooldown_seconds", 60),
                "stale_threshold": cfg.get("stale_threshold", 0.5),
                "min_poll_balance_usdc": cfg["min_poll_balance_usdc"],
            })

    # POST — 修改参数
    denied = _require_token()
    if denied:
        return denied
    if engine is None:
        return jsonify({"error": "Engine not initialized"}), 503
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "No data"}), 400

    changed = []
    with engine_lock:
        # 整数型参数（带合理性校验）
        param_ranges_int = {
            "poll_interval": (5, 300),
            "cooldown_seconds": (10, 600),
            "rv_update_interval_minutes": (5, 1440),
        }
        for key, (lo, hi) in param_ranges_int.items():
            if key in data:
                val = int(data[key])
                if lo <= val <= hi:
                    engine.cfg[key] = val
                    changed.append(f"{key}={val}")

        # 浮点型参数（带合理性校验）
        param_ranges_float = {
            "trade_size_usdc": (10, 10000),
            "rv_min": (0.0001, 0.05),
            "rv_max": (0.001, 0.1),
            "min_poll_balance_usdc": (10, 10000),
            "stale_threshold": (0.05, 5.0),
        }
        for key, (lo, hi) in param_ranges_float.items():
            if key in data:
                val = float(data[key])
                if lo <= val <= hi:
                    engine.cfg[key] = val
                    changed.append(f"{key}={val}")

        # 锚点（特殊处理：需要重算阈值）
        if "anchor_price" in data:
            val = float(data["anchor_price"])
            if val > 0:
                engine.anchor_price = val
                engine._recalc_thresholds()
                changed.append(f"anchor=${val:.2f}")

    if changed:
        logger.info("Params updated: %s", ", ".join(changed))
        broadcast_state(engine.get_state())
    return jsonify({"success": True, "changed": changed})


@app.route("/btc-enhancer/api/kline")
def api_kline():
    """拉 OKX 当前现货标的 K 线"""
    denied = _credentials_denied()
    if denied:
        return denied
    try:
        end = int(pytime.time() * 1000)
        start = end - 7 * 86400 * 1000
        client = OKXClient(
            OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE,
            testnet=USE_TESTNET,
            instrument_name=OKX_INSTRUMENT_NAME,
        )
        data = client.get_tradingview_chart_data(OKX_INSTRUMENT_NAME, start, end, "5")
        return jsonify(data or {"error": "no data"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/btc-enhancer/api/test-connection")
def api_test_connection():
    if not _credentials_ready():
        return jsonify({
            "mainnet": {"connected": False, "auth_error": CONFIG_ERROR},
            "testnet": {"connected": False, "auth_error": CONFIG_ERROR},
        })
    results = {}
    for label, testnet in [("mainnet", False), ("testnet", True)]:
        client = OKXClient(
            OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE,
            testnet=testnet,
            instrument_name=OKX_INSTRUMENT_NAME,
        )
        info = client.check_connection()
        if info["connected"]:
            price = client.get_index_price(OKX_INSTRUMENT_NAME)
            info["btc_index_price"] = price
            try:
                quote_currency = OKX_INSTRUMENT_NAME.replace("_", "-").split("-")[1]
                quote = client.get_account_summary(currency=quote_currency)
                if quote:
                    info["usdc_balance"] = quote.get("balance", 0)
            except Exception:
                pass
            try:
                btc = client.get_account_summary(currency="BTC")
                if btc:
                    info["btc_balance"] = btc.get("balance", 0)
            except Exception:
                pass
        results[label] = info
    return jsonify(results)


@app.route("/btc-enhancer/api/config", methods=["GET", "POST"])
def api_config():
    global engine
    if request.method == "GET":
        if engine is None:
            return jsonify({"error": "Engine not initialized"}), 503
        return jsonify(engine.cfg)
    denied = _require_token()
    if denied:
        return denied
    denied = _credentials_denied()
    if denied:
        return denied
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "message": "No data"}), 400
    if engine and engine._running:
        return jsonify({"success": False, "message": "Cannot modify config while running"}), 400
    changed = []
    with engine_lock:
        if engine is None:
            use_testnet = data.get("testnet", USE_TESTNET)
            engine = StrategyEngine(
                OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE,
                config={"instrument_name": OKX_INSTRUMENT_NAME, "index_name": OKX_INSTRUMENT_NAME},
                testnet=use_testnet,
                state_callback=on_state_update,
            )
        # 整数型参数（带范围校验）
        for key, (lo, hi) in {"poll_interval": (5, 300), "cooldown_seconds": (10, 600),
                               "rv_update_interval_minutes": (5, 1440)}.items():
            if key in data:
                try:
                    val = int(data[key])
                    if lo <= val <= hi:
                        engine.cfg[key] = val
                        changed.append(f"{key}={val}")
                except (TypeError, ValueError):
                    pass
        # 浮点型参数（带范围校验）
        for key, (lo, hi) in {"trade_size_usdc": (10, 10000), "rv_min": (0.0001, 0.05),
                              "rv_max": (0.001, 0.1), "stale_threshold": (0.01, 1000),
                              "min_poll_balance_usdc": (10, 10000)}.items():
            if key in data:
                try:
                    val = float(data[key])
                    if lo <= val <= hi:
                        engine.cfg[key] = val
                        changed.append(f"{key}={val}")
                except (TypeError, ValueError):
                    pass
        # 标的（仅未运行时可改，限定取值）
        if "instrument_name" in data and data["instrument_name"] in ("BTC-USDC", "BTC-USDT"):
            engine.cfg["instrument_name"] = data["instrument_name"]
            engine.cfg["index_name"] = data["instrument_name"]
            changed.append(f"instrument_name={data['instrument_name']}")
    return jsonify({"success": True, "changed": changed})


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("  BTC 收益增强策略 - Dashboard + WebSocket")
    print("  http://localhost:5050")
    print("=" * 60)
    app.run(host="127.0.0.1", port=5050, debug=False)
