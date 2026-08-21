"""
BTC 收益增强策略引擎 v2

核心逻辑（产品定义）：
1. 启动时扫描 USDC + BTC 余额，记录初始总值
2. 以当前指数价作为价格锚（不额外买入 BTC）
3. 计算近 1 小时日化 RV，限幅 0.5%~5%，每 15 分钟更新
4. 价格涨过锚 × (1 + RV) → 卖出 200U 等值 BTC
   价格跌破锚 × (1 - RV) → 买入 200U 等值 BTC
5. 成交后更新锚为成交均价
6. 资金不足时暂停对应方向，恢复后自动恢复
7. 买卖方向独立，一方不足不影响另一方
"""

from __future__ import annotations

import time
import math
import json
import os
import queue
import logging
import threading
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from statistics import stdev
from typing import Optional

from okx_api import OKXClient
from okx_ws import OKXWSClient
from runtime_paths import get_runtime_paths

logger = logging.getLogger(__name__)

BJT = timezone(timedelta(hours=8))
RUNTIME_PATHS = get_runtime_paths()
STATE_FILE = RUNTIME_PATHS.state_file

DEFAULT_CONFIG = {
    "trade_size_usdc": 100.0,
    "rv_min": 0.005,       # 日化 RV 下限 0.5%
    "rv_max": 0.05,        # 日化 RV 上限 5.0%
    "rv_update_interval_minutes": 15,  # RV 更新间隔（分钟，兜底用；成交/方案A触发实时更新）
    "poll_interval": 30,
    "cooldown_seconds": 180,
    "stale_threshold": 0.5,          # 挂单偏移撤单阈值（USDC）
    "instrument_name": "BTC-USDC",
    "index_name": "BTC-USDC",
    "min_poll_balance_usdc": 200,   # 资金保护阈值 $200
}


class StrategyEngine:
    """策略引擎 - 在后台线程运行"""

    def __init__(self, api_key, api_secret, passphrase="", config=None, testnet=False, state_callback=None):
        self.cfg = {**DEFAULT_CONFIG, **(config or {})}
        self.api = OKXClient(
            api_key,
            api_secret,
            passphrase,
            testnet=testnet,
            instrument_name=self.cfg["instrument_name"],
        )
        self.testnet = testnet
        # 从 state.json 恢复运行时修改的配置（只恢复用户可调的键，不覆盖新默认值）
        _saved = self._load_state()
        if _saved and isinstance(_saved.get("config"), dict):
            for _k in ["rv_min", "rv_max", "trade_size_usdc", "cooldown_seconds"]:
                if _k in _saved["config"]:
                    self.cfg[_k] = _saved["config"][_k]
        self._state_callback = state_callback  # 状态变更回调（用于 WebSocket 推送）

        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

        # 合约规格
        self.contract_size = 0.0001
        self.min_trade_amount = 0.0001
        self.tick_size = 1.0

        # 策略状态
        self.status = "stopped"
        self.initial_total_usdc = 0.0     # 启动时总资产 USDC 价值
        self.initial_usdc = 0.0
        self.initial_btc = 0.0
        self.usdc_balance = 0.0
        self.btc_balance = 0.0
        self.usdc_available = 0.0
        self.btc_available = 0.0
        self.btc_value_usdc = 0.0
        self.total_value_usdc = 0.0
        self.btc_index_price = 0.0
        self.anchor_price = 0.0
        self.daily_rv = self.cfg["rv_min"]
        self.upper_threshold = 0.0
        self.lower_threshold = 0.0
        self.rv_updated_today = False
        self.last_rv_update: Optional[str] = None
        self.usdc_insufficient = False
        self.btc_insufficient = False
        self.api_connected = False
        self.trades: list[dict] = []
        self.errors: list[dict] = []
        self.last_update: Optional[str] = None
        self.start_time: Optional[str] = None
        self.total_pnl = 0.0
        self.total_trades = 0
        self.btc_cost_basis = 0.0
        self.realized_pnl = 0.0          # 累计已实现交易盈亏（增量维护，不依赖 trades 历史长度）
        self.buy_inventory = []          # 未平仓买入库存 FIFO: [[amount, price], ...]
        self._cooldown_until = 0.0        # 防频繁交易的冷却时间（秒时间戳）
        # cooldown 直接读 self.cfg.get("cooldown_seconds", 180)（方案2 已移除冗余缓存变量 _cooldown_seconds）
        self._trading_enabled = False      # 交易开关：就绪后默认不交易，用户点击"启动"才开
        self.open_orders: list[dict] = []  # 当前挂单列表
        self._our_buy_id: Optional[str] = None   # 我们挂的买入单 ID
        self._our_sell_id: Optional[str] = None  # 我们挂的卖出单 ID
        self._processed_fill_amounts: dict[str, float] = {}
        self._processed_fill_costs: dict[str, float] = {}
        self._fill_lock = threading.Lock()
        self._order_event_queue: queue.Queue = queue.Queue()
        self._order_event_worker: Optional[threading.Thread] = None

        # WebSocket 客户端（实时数据源）
        self._ws: Optional[OKXWSClient] = None
        self._ws_enabled = False
        self._last_ws_index_update = 0.0  # 最新一次从 WS 拿到指数价的时间戳
        self._last_ws_balance_update = 0.0  # 最新一次从 WS 拿到余额的时间戳
        self._last_ws_check_ts = 0.0     # 最后一次检查 WS 连接的时间戳

        logger.info("StrategyEngine v2 created")

    # ------------------------------------------------------------------
    # 状态持久化
    # ------------------------------------------------------------------

    STATE_BACKUP_DAYS = 7
    _state_backup_dir = RUNTIME_PATHS.state_backup_dir

    def _save_state(self):
        """保存锚点、初始值、交易记录、运行时配置到文件，重启时恢复

        安全策略：
        1. 先写临时文件再 rename（原子写入，防止写一半崩溃）
        2. 保留 7 天滚动备份
        """
        try:
            data = {
                "anchor_price": self.anchor_price,
                "initial_usdc": self.initial_usdc,
                "initial_btc": self.initial_btc,
                "initial_total_usdc": self.initial_total_usdc,
                "trades": self.trades[-200:],  # 保留最近 200 笔用于前端展示
                "realized_pnl": self.realized_pnl,       # 累计已实现盈亏（不截断）
                "buy_inventory": self.buy_inventory,     # 未平仓买入库存（不截断，PNL 计算依赖）
                "total_trades": self.total_trades,
                "was_trading": self._trading_enabled,  # 重启后自动恢复交易
                "config": self.cfg,  # 运行时配置（含 API 修改的下限/上限等）
                "updated_at": datetime.now(BJT).isoformat(),
            }
            # 原子写入
            import tempfile
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            tmp = tempfile.NamedTemporaryFile(
                mode="w", dir=os.path.dirname(STATE_FILE),
                delete=False, suffix=".tmp",
            )
            try:
                json.dump(data, tmp, default=str)
                tmp.flush()
                os.fsync(tmp.fileno())
                tmp.close()
                os.replace(tmp.name, STATE_FILE)
            except Exception:
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass
                raise

            # 7 天滚动备份
            self._rotate_backup(data)

        except Exception as e:
            logger.warning("Failed to save state: %s", e)

    def _rotate_backup(self, data):
        """保留 7 天的 state 滚动备份（按日期）"""
        try:
            import shutil
            backup_dir = self._state_backup_dir
            os.makedirs(backup_dir, exist_ok=True)
            today = datetime.now(BJT).strftime("%Y%m%d")
            backup_path = os.path.join(backup_dir, f"state.{today}.json")
            if not os.path.exists(backup_path):
                with open(backup_path, "w") as f:
                    json.dump(data, f, default=str)
            # 清理超过 7 天的备份
            cutoff = (datetime.now(BJT) - timedelta(days=7)).strftime("%Y%m%d")
            for fname in os.listdir(backup_dir):
                if fname.startswith("state.") and fname.endswith(".json"):
                    date_part = fname.split(".")[1]
                    if date_part < cutoff:
                        try:
                            os.remove(os.path.join(backup_dir, fname))
                        except Exception:
                            pass
        except Exception as e:
            logger.warning("state backup error: %s", e)

    @staticmethod
    def _load_state():
        """从文件恢复锚点和初始值"""
        try:
            if not os.path.exists(STATE_FILE):
                return None
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    def _round_amount(self, amount_btc):
        if self.contract_size <= 0:
            return round(amount_btc, 6)
        if amount_btc <= 0:
            return 0
        step = Decimal(str(self.contract_size))
        units = max(
            1,
            int((Decimal(str(amount_btc)) / step).quantize(
                Decimal("1"), rounding=ROUND_HALF_UP
            )),
        )
        return float(units * step)

    def _round_price(self, price: float) -> float:
        """按交易所 tick_size 取最近价格档位。"""
        if self.tick_size <= 0:
            return round(price, 1)
        step = Decimal(str(self.tick_size))
        units = (Decimal(str(price)) / step).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
        return float(units * step)

    def _fetch_instrument_info(self):
        try:
            base_currency = self.cfg["instrument_name"].replace("_", "-").split("-")[0]
            instruments = self.api.get_instruments(currency=base_currency, kind="spot")
            for inst in instruments:
                if inst["instrument_name"] == self.cfg["instrument_name"]:
                    self.contract_size = float(inst.get("contract_size", 0.0001))
                    self.min_trade_amount = float(inst.get("min_trade_amount", 0.0001))
                    self.tick_size = float(inst.get("tick_size", 1.0))
                    logger.info("Instrument: contract=%.4f min_trade=%.4f",
                                self.contract_size, self.min_trade_amount)
                    return True
        except Exception as e:
            logger.error("Fetch instrument error: %s", e)
        return False

    # ------------------------------------------------------------------
    # 公共控制
    # ------------------------------------------------------------------

    def initialize(self):
        """初始化（连接 + 拉数据 + 设锚）— 不启动交易循环"""
        if self._running:
            return False
        # 只取消当前策略标的的订单（不碰合约、期权、其他币种）
        try:
            self.api.cancel_all_by_instrument(self.cfg["instrument_name"])
        except Exception:
            pass
        self._running = True
        self._trading_enabled = False
        self._our_buy_id = None
        self._our_sell_id = None
        self._processed_fill_amounts.clear()
        self._processed_fill_costs.clear()
        self._order_event_queue = queue.Queue()
        self._order_event_worker = threading.Thread(
            target=self._order_event_loop, daemon=True,
        )
        self._order_event_worker.start()
        # 启动 WebSocket 客户端（后台线程）
        try:
            self._ws = OKXWSClient(
                self.api.api_key, self.api.api_secret, self.api.passphrase,
                testnet=self.testnet,
                instrument_name=self.cfg["instrument_name"],
                callback=self._on_ws_message,
            )
            self._ws.start()
            self._ws_enabled = True
            logger.info("WS client started")
        except Exception as e:
            logger.warning("WS client start failed (will use REST only): %s", e)
            self._ws_enabled = False
        self._thread = threading.Thread(target=self._data_loop, daemon=True)
        self._thread.start()
        return True

    def start(self):
        """启动交易（需先 initialize）"""
        if not self._running:
            return False
        if self._trading_enabled:
            return False
        self._trading_enabled = True
        # 等待 _init_strategy 完成再存盘（status 进入 ready/running），防止空壳覆盖已恢复数据
        for _ in range(20):  # 最多等 10 秒
            if self.status in ("ready", "running"):
                break
            time.sleep(0.5)
        self._save_state()
        self._log_info("=== 交易已启动 ===")
        # 状态由 _data_loop 在下一轮自动切换为 running
        self._notify_state()
        return True

    def stop(self):
        """停止一切：WS 断开 + 取消挂单 + 保存交易记录"""
        # 停 WS
        if self._ws:
            try:
                self._ws.stop()
            except Exception:
                pass
            self._ws = None
            self._ws_enabled = False
        self._running = False
        self._order_event_queue.put(None)
        if self._order_event_worker and self._order_event_worker is not threading.current_thread():
            self._order_event_worker.join(timeout=1.0)
        self._order_event_worker = None
        # 先取消我们的挂单
        self._cancel_our_orders()
        self._save_state()
        self._trading_enabled = False
        self._set_status("stopped")
        self._notify_state()
        return True

    def _cancel_our_orders(self):
        """取消我们挂出的 maker 单"""
        for oid in [self._our_buy_id, self._our_sell_id]:
            if oid:
                try:
                    self.api.cancel_order(oid)
                    self._log_info("Cancelled order %s on stop", oid)
                except Exception as e:
                    self._log_info("Cancel %s failed: %s", oid, e)
        self._our_buy_id = None
        self._our_sell_id = None

    def get_state(self):
        with self._lock:
            # FIFO 计算交易盈亏
            tp = self._calc_trading_pnl()
            return {
                "status": self.status,
                "initial_usdc": self.initial_usdc,
                "initial_btc": self.initial_btc,
                "initial_total_usdc": self.initial_total_usdc,
                "usdc_balance": self.usdc_balance,
                "btc_balance": self.btc_balance,
                "usdc_available": self.usdc_available,
                "btc_available": self.btc_available,
                "btc_value_usdc": self.btc_value_usdc,
                "total_value_usdc": self.total_value_usdc,
                "btc_index_price": self.btc_index_price,
                "anchor_price": self.anchor_price,
                "daily_rv": self.daily_rv,
                "upper_threshold": self.upper_threshold,
                "lower_threshold": self.lower_threshold,
                "rv_updated_today": self.rv_updated_today,
                "last_rv_update": self.last_rv_update,
                "usdc_insufficient": self.usdc_insufficient,
                "btc_insufficient": self.btc_insufficient,
                "api_connected": self.api_connected,
                "trading_enabled": self._trading_enabled,
                "trades": list(self.trades[-50:]),
                "errors": list(self.errors[-20:]),
                "open_orders": self.open_orders,
                "last_update": self.last_update,
                "start_time": self.start_time,
                "total_pnl": self.total_value_usdc - self.initial_total_usdc,
                "trading_pnl": tp,
                "total_trades": self.total_trades,
                "btc_cost_basis": self.btc_cost_basis,
                "config": {
                    "trade_size_usdc": self.cfg["trade_size_usdc"],
                    "rv_min": self.cfg["rv_min"],
                    "rv_max": self.cfg["rv_max"],
                    "rv_update_interval_minutes": self.cfg.get("rv_update_interval_minutes", 15),
                    "poll_interval": self.cfg["poll_interval"],
                    "cooldown_seconds": self.cfg.get("cooldown_seconds", 180),
                    "stale_threshold": self.cfg.get("stale_threshold", 0.5),
                    "min_poll_balance_usdc": self.cfg["min_poll_balance_usdc"],
                    "instrument_name": self.cfg["instrument_name"],
                    "testnet": self.testnet,
                },
            }

    def _calc_trading_pnl(self):
        """返回累计已实现交易盈亏（由每笔成交增量维护，不依赖 trades 历史长度）"""
        return round(self.realized_pnl, 2)

    def _recompute_inventory_from_trades(self):
        """从 trades 历史重建 realized_pnl 与 buy_inventory（兼容旧 state 无该字段）"""
        self.realized_pnl = 0.0
        self.buy_inventory = []
        for t in self.trades:
            if t["side"] == "buy":
                self.buy_inventory.append([t["amount_btc"], t["price"]])
            else:
                remaining = t["amount_btc"]
                while remaining > 1e-6 and self.buy_inventory:
                    lot = self.buy_inventory[0]
                    take = min(lot[0], remaining)
                    self.realized_pnl += take * (t["price"] - lot[1])
                    lot[0] -= take
                    remaining -= take
                    if lot[0] <= 1e-6:
                        self.buy_inventory.pop(0)

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------

    def _data_loop(self):
        """数据同步 + 可选交易循环
        第一阶段：初始化连接、余额、锚点、RV → 进入"就绪"状态
        第二阶段：用户点"启动"后 → 开启交易信号检查
        """
        logger.info("=== Data loop started ===")
        self.start_time = datetime.now(BJT).isoformat()

        try:
            self._set_status("initializing")
            if not self._init_strategy():
                self._set_status("error")
                self._add_error("Strategy initialization failed")
                self._running = False
                self._order_event_queue.put(None)
                return

            self._set_status("ready")
            self._log_info("就绪 — 数据同步中，等待启动交易")
            self._notify_state()

            while self._running:
                try:
                    # 动态更新状态：交易开关决定 status
                    target_status = "running" if self._trading_enabled else "ready"
                    if self.status != target_status:
                        logger.info("STATUS_CHANGE: %s -> %s", self.status, target_status)
                        self._set_status(target_status)

                    self._update_index_price()
                    self._fetch_balances()
                    self._check_rv_update()
                    self._check_funds()
                    self._fetch_open_orders()

                    # 只有用户点了"启动"才管理挂单
                    if self._trading_enabled:
                        self._manage_maker_orders()

                    self.last_update = datetime.now(BJT).isoformat()
                    self._notify_state()
                except Exception as e:
                    logger.error("Loop error: %s", e, exc_info=True)
                    self._add_error(f"Loop: {e}")
                time.sleep(self.cfg["poll_interval"])

        except Exception as e:
            logger.error("Fatal: %s", e, exc_info=True)
            self._add_error(f"Fatal: {e}")
            self._set_status("error")

        logger.info("=== Data loop ended ===")

    def _order_event_loop(self):
        """Process order events outside the asyncio WebSocket callback thread."""
        while True:
            data = self._order_event_queue.get()
            if data is None:
                return
            try:
                self._handle_order_event(data)
            except Exception as exc:
                logger.warning("Order event processing error: %s", exc)

    def _notify_state(self):
        """通知前端状态更新（WebSocket 回调）"""
        if self._state_callback:
            try:
                self._state_callback(self.get_state())
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 初始化（智能检测已有持仓）
    # ------------------------------------------------------------------

    def _init_strategy(self):
        """初始化：连接 → 查余额 → 判断是否有 BTC → 设锚"""
        logger.info("Initializing...")

        # 1. 连接
        conn = self.api.check_connection()
        self.api_connected = conn.get("connected", False)
        if not self.api_connected:
            logger.error("Cannot connect: %s", conn.get("auth_error", "unknown"))
            return False
        self._fetch_instrument_info()

        # 2. 查余额（仅用于校验，不做初始记录——初始值应该从 state.json 恢复）
        bal = self._fetch_balances()
        if bal is None:
            return False

        # 3. 获取指数价
        price = self.api.get_index_price(self.cfg["index_name"])
        if not price or price <= 0:
            logger.error("Cannot get index price")
            return False
        self.btc_index_price = price

        # 4. 状态恢复：交易记录与初始值无条件恢复；锚点按可用性恢复
        saved = self._load_state()
        if saved:
            # —— 交易记录：无条件恢复（与锚点价格无关）——
            old_trades = saved.get("trades", [])
            if old_trades:
                self.trades = old_trades
                self.total_trades = saved.get("total_trades", len(old_trades))
                logger.info("Restored %d trades from saved state", len(old_trades))
            # 恢复已实现盈亏与买入库存（兼容旧 state 无该字段）
            if "realized_pnl" in saved and "buy_inventory" in saved:
                self.realized_pnl = float(saved.get("realized_pnl", 0.0))
                self.buy_inventory = [list(x) for x in saved.get("buy_inventory", [])]
            else:
                self._recompute_inventory_from_trades()
            # 初始值：恢复（用于 PNL 计算）。.get() 遇到值=0 不回退到默认值，
            # 所以显式判断：saved 值 > 0 才恢复，否则用当前余额快照。
            saved_initial = saved.get("initial_total_usdc", 0)
            if saved_initial > 0:
                self.initial_total_usdc = saved_initial
                self.initial_usdc = saved.get("initial_usdc", bal["usdc_balance"])
                self.initial_btc = saved.get("initial_btc", bal["btc_balance"])
            else:
                self.initial_usdc = bal["usdc_balance"]
                self.initial_btc = bal["btc_balance"]
                self.initial_total_usdc = self.initial_usdc + self.initial_btc * price
                logger.info("Initial total_usdc reset to current balance (saved was 0 or missing)")
            # 重启后自动恢复交易状态
            if saved.get("was_trading"):
                self._trading_enabled = True
                logger.info("Trading auto-resumed from saved state")
            # —— 锚点：仅在保存锚与当前价偏差 <10% 时恢复，否则用当前指数价 ——
            if saved.get("anchor_price", 0) > 0 and abs(saved["anchor_price"] - price) / price < 0.10:
                self.anchor_price = saved["anchor_price"]
                logger.info("Anchor restored from saved state: %.2f (current price: %.2f)",
                            self.anchor_price, price)
            else:
                self.anchor_price = price
                logger.info("Anchor set to current index price: %.2f (saved anchor not reusable)",
                            price)
        else:
            # 首次部署或 state 损坏：用当前 balance snapshot 作为初始值
            if self.initial_total_usdc == 0:
                self.initial_usdc = bal["usdc_balance"]
                self.initial_btc = bal["btc_balance"]
                self.initial_total_usdc = self.initial_usdc + self.initial_btc * price
                logger.info("Initial snapshot: USDC=%.2f BTC=%.6f total=$%.2f",
                            self.initial_usdc, self.initial_btc, self.initial_total_usdc)
            self.anchor_price = price
            logger.info("Anchor set to index price: %.2f", self.anchor_price)

        # 5. 计算 RV + 阈值
        self._update_rv()
        self._recalc_thresholds()
        self._fetch_balances()

        logger.info("Strategy initialized: anchor=%.2f rv=%.2f%% upper=%.2f lower=%.2f",
                    self.anchor_price, self.daily_rv * 100,
                    self.upper_threshold, self.lower_threshold)
        return True

    # ------------------------------------------------------------------
    # 轮询更新
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # RV 计算
    # ------------------------------------------------------------------

    def _check_rv_update(self):
        """按 rv_update_interval_minutes 间隔更新 RV"""
        if not self.last_rv_update:
            self._update_rv()
            return
        # 解析上次更新时间
        try:
            last = datetime.fromisoformat(self.last_rv_update)
        except Exception:
            self._update_rv()
            return
        elapsed = (datetime.now(BJT) - last).total_seconds()
        interval_sec = self.cfg.get("rv_update_interval_minutes", 60) * 60
        if elapsed >= interval_sec:
            self._update_rv()

    def _update_rv(self):
        rv = self._calculate_daily_rv()
        if rv is not None:
            old = self.daily_rv
            self.daily_rv = rv
            self.last_rv_update = datetime.now(BJT).isoformat()
            self.rv_updated_today = True
            self._recalc_thresholds()
            logger.info("RV: %.2f%% → %.2f%%", old * 100, rv * 100)

    def _calculate_daily_rv(self):
        """用当前现货标的 5 分钟 K 线，取 12 根(1小时窗口)的 RMS × √24 作为日化 RV"""
        end = int(time.time() * 1000)
        start = end - 3 * 3600 * 1000  # 拉3小时确保有12根
        data = self.api.get_index_chart_data(self.cfg["index_name"], start, end, "5")
        if not data or not data.get("close") or not data.get("open"):
            return self._fallback_rv()

        opens = data["open"]
        closes = data["close"]
        confirmations = data.get("confirm")
        bars = []
        for i in range(min(len(opens), len(closes))):
            if confirmations and (i >= len(confirmations) or not confirmations[i]):
                continue
            if opens[i] and opens[i] > 0 and closes[i] and closes[i] > 0:
                bars.append((opens[i], closes[i]))

        if len(bars) < 12:
            return self._fallback_rv()

        bars = bars[-12:]

        sq_sum = 0.0
        n = 0
        for open_price, close_price in bars:
            r = (close_price - open_price) / open_price
            sq_sum += r * r
            n += 1

        if n < 12:
            return self._fallback_rv()

        rv = math.sqrt(sq_sum / n)
        rv_daily = rv * math.sqrt(24)  # 小时 RMS → 日化 RV
        return max(self.cfg["rv_min"], min(self.cfg["rv_max"], rv_daily))

    def _fallback_rv(self):
        return self.cfg["rv_min"]

    # ------------------------------------------------------------------
    # WebSocket 回调（由 WS 线程调用）
    # ------------------------------------------------------------------

    def _on_ws_message(self, msg: dict):
        """WS 推送实时更新余额/指数价缓存"""
        try:
            channel = msg.get("channel", "")
            data = msg.get("data", {})
            if channel == "user.portfolio.btc":
                equity = data.get("equity", data.get("balance", 0))
                available = data.get("available", data.get("balance", equity))
                if equity is not None and float(equity) >= 0:
                    old = self.btc_balance
                    self.btc_balance = float(equity)
                    self.btc_available = max(0.0, float(available))
                    self._last_ws_balance_update = time.time()
                    if abs(self.btc_balance - old) > 1e-6:
                        logger.info("WS[btc]: %.6f -> %.6f", old, self.btc_balance)
                    if self.btc_index_price > 0:
                        self._recalc_values()
            elif channel == "user.portfolio.usdc":
                equity = data.get("equity", data.get("balance", 0))
                available = data.get("available", data.get("balance", equity))
                if equity is not None and float(equity) >= 0:
                    old = self.usdc_balance
                    self.usdc_balance = float(equity)
                    self.usdc_available = max(0.0, float(available))
                    self._last_ws_balance_update = time.time()
                    if abs(self.usdc_balance - old) > 1e-6:
                        logger.info("WS[usdc]: %.2f -> %.2f", old, self.usdc_balance)
                    if self.btc_index_price > 0:
                        self.btc_value_usdc = self.btc_balance * self.btc_index_price
                        self.total_value_usdc = self.usdc_balance + self.btc_value_usdc
            elif "index" in channel:
                idx = data.get("index_price") or data.get("idx")
                if idx is not None and float(idx) > 0:
                    old = self.btc_index_price
                    self.btc_index_price = float(idx)
                    if abs(self.btc_index_price - old) > 0.1:
                        logger.info("WS[index]: %.2f -> %.2f", old, self.btc_index_price)
                    self._last_ws_index_update = time.time()
                    self._recalc_values()
                    self.api_connected = True
            elif channel == "user.order":
                if self._order_event_worker and self._order_event_worker.is_alive():
                    self._order_event_queue.put(data)
                else:
                    self._handle_order_event(data)
            # WS 有推送说明连接正常
            self.api_connected = True
            if channel not in ("heartbeat",):
                self._last_ws_check_ts = time.time()
        except Exception as e:
            logger.warning("WS callback error: %s", e)

    def _handle_order_event(self, data: dict):
        """Process an OKX orders-channel update without double-counting fills."""
        order_id = str(data.get("order_id", ""))
        instrument = data.get("instrument_name", "")
        side = data.get("side", "")
        label = data.get("label", "")
        if not order_id:
            return
        if side not in ("buy", "sell"):
            return

        instrument = instrument.replace("_", "-").upper()
        expected_instrument = self.cfg["instrument_name"].replace("_", "-").upper()
        if instrument and instrument != expected_instrument:
            return

        tracked = order_id in (self._our_buy_id, self._our_sell_id)
        label = str(label).lower().replace("-", "_")
        maker_labels = {
            "buy": {"maker_buy", "makerbuy"},
            "sell": {"maker_sell", "makersell"},
        }
        labeled = label in maker_labels[side]
        if not tracked and not labeled:
            return
        if labeled and side == "buy" and not self._our_buy_id:
            self._our_buy_id = order_id
        elif labeled and side == "sell" and not self._our_sell_id:
            self._our_sell_id = order_id

        cumulative_amount = float(data.get("filled_amount", 0) or 0)
        if cumulative_amount <= 0:
            cumulative_amount = float(data.get("fill_amount", 0) or 0)
        if cumulative_amount > 0:
            fill_price = float(
                data.get("fill_price", 0)
                or data.get("average_price", 0)
                or 0
            )
            self._process_order_fill(side, order_id, cumulative_amount, fill_price)

        state = str(data.get("state", "")).lower()
        if state in ("filled", "canceled", "cancelled", "mmp_canceled"):
            if self._our_buy_id == order_id:
                self._our_buy_id = None
            if self._our_sell_id == order_id:
                self._our_sell_id = None

    def _process_order_fill(
        self,
        side_key: str,
        order_id: str,
        cumulative_amount: float,
        fill_price: float,
        cumulative_cost: Optional[float] = None,
    ) -> bool:
        """Record only the new cumulative fill amount for an order."""
        if cumulative_amount <= 0 or fill_price <= 0:
            return False
        with self._fill_lock:
            previous_amount = self._processed_fill_amounts.get(order_id, 0.0)
            delta_amount = cumulative_amount - previous_amount
            if delta_amount <= 1e-12:
                return False

            previous_cost = self._processed_fill_costs.get(order_id, 0.0)
            if cumulative_cost is None:
                delta_cost = delta_amount * fill_price
                next_cost = previous_cost + delta_cost
            else:
                next_cost = cumulative_cost
                delta_cost = next_cost - previous_cost
                if delta_cost <= 0:
                    delta_cost = delta_amount * fill_price
                    next_cost = previous_cost + delta_cost
            delta_price = delta_cost / delta_amount
            self._processed_fill_amounts[order_id] = cumulative_amount
            self._processed_fill_costs[order_id] = next_cost

            self._log_info("%s maker order %s fill +%.6f BTC @ %.2f",
                           side_key, order_id, delta_amount, delta_price)
            self._cooldown_until = time.time() + self.cfg.get("cooldown_seconds", 180)
            self.anchor_price = delta_price
            self._update_rv()
            self._recalc_thresholds()
            self._fetch_balances()

            with self._lock:
                if side_key == "buy":
                    self.buy_inventory.append([round(delta_amount, 6), delta_price])
                else:
                    remaining = delta_amount
                    while remaining > 1e-6 and self.buy_inventory:
                        lot = self.buy_inventory[0]
                        take = min(lot[0], remaining)
                        self.realized_pnl += take * (delta_price - lot[1])
                        lot[0] -= take
                        remaining -= take
                        if lot[0] <= 1e-6:
                            self.buy_inventory.pop(0)
                    if remaining > 1e-6:
                        self._log_info(
                            "Sell %.6f exceeds buy inventory, partial PNL skipped",
                            remaining,
                        )
                self.trades.append({
                    "id": f"{'B' if side_key == 'buy' else 'S'}{int(time.time())}",
                    "time": datetime.now(BJT).isoformat(),
                    "side": side_key,
                    "amount_btc": round(delta_amount, 6),
                    "price": delta_price,
                    "total_usdc": round(delta_amount * delta_price, 2),
                    "order_id": order_id,
                    "status": "filled",
                    "label": "maker",
                })
                self.total_trades += 1
                if len(self.trades) > 500:
                    self.trades = self.trades[-500:]
            self._save_state()

            other_id = self._our_buy_id if side_key == "sell" else self._our_sell_id
            if other_id and other_id != order_id:
                r = self.api.cancel_order(other_id)
                if not r["success"]:
                    self._route_api_error(
                        "cancel", r, "buy" if side_key == "sell" else "sell"
                    )
                if side_key == "sell":
                    self._our_buy_id = None
                else:
                    self._our_sell_id = None

            idx = self.btc_index_price
            if idx > 0:
                deviation = abs(idx / self.anchor_price - 1)
                if deviation > self.daily_rv:
                    old_anchor = self.anchor_price
                    self.anchor_price = idx
                    self._update_rv()
                    self._recalc_thresholds()
                    self._log_info(
                        "方案A: Anchor追 %.2f -> %.2f (deviation %.4f%%), RV=%.2f%%",
                        old_anchor, idx, deviation * 100, self.daily_rv * 100,
                    )
                    self._cooldown_until = time.time() + self.cfg.get("cooldown_seconds", 180)
            return True

    def _recalc_values(self):
        """根据当前余额和指数价重算 USDC 价值"""
        self.btc_value_usdc = self.btc_balance * self.btc_index_price
        self.total_value_usdc = self.usdc_balance + self.btc_value_usdc

    # ------------------------------------------------------------------
    # 价格与余额（优先 WS 缓存，WS 不可用时 fallback REST）
    # ------------------------------------------------------------------

    _INDEX_REST_INTERVAL = 15  # 指数价 REST 备用拉取间隔（秒）
    _BALANCE_REST_INTERVAL = 30  # 余额 REST 备用拉取间隔（秒，WS 静默断流时回退）
    _last_index_rest_ts = 0.0

    def _update_index_price(self):
        """获取指数价：优先 WS 实时数据，WS 过期时 fallback REST"""
        now = time.time()
        # WS 有数据且在 30 秒内更新过，直接用
        if self._ws_enabled and self.btc_index_price > 0 and (now - self._last_ws_index_update) < 30:
            return
        # REST fallback（限制频率）
        if now - self._last_index_rest_ts < self._INDEX_REST_INTERVAL:
            return
        self._last_index_rest_ts = now
        try:
            price = self.api.get_index_price(self.cfg["index_name"])
        except Exception:
            price = None
        if price and price > 0:
            self.btc_index_price = price
            self._recalc_values()
            self.api_connected = True
        elif not self._ws_enabled or not self._ws or not self._ws.connected:
            self.api_connected = False

    def _fetch_balances(self):
        """获取余额：WS 在线时不调 REST（WS 实时推送已在 _on_ws_message 更新）；
        WS 不可用时 fallback REST。
        """
        if self._ws_enabled and self._ws and self._ws.connected and self._ws.authenticated:
            # WS 在线，但余额推送可能静默中断：超过阈值则回 REST 刷新
            if time.time() - self._last_ws_balance_update < self._BALANCE_REST_INTERVAL:
                self.api_connected = True
                return {
                    "usdc_balance": self.usdc_balance,
                    "btc_balance": self.btc_balance,
                    "usdc_available": self.usdc_available,
                    "btc_available": self.btc_available,
                }
            # WS 余额疑似过期，fall through 到 REST 兜底
        # WS 不可用，REST fallback
        try:
            base_currency, quote_currency = self.cfg["instrument_name"].replace("_", "-").split("-")[:2]
            quote = self.api.get_account_summary(currency=quote_currency)
            if quote:
                self.usdc_balance = float(
                    quote.get("equity", quote.get("total_balance", quote.get("balance", 0))) or 0
                )
                self.usdc_available = float(
                    quote.get("available", quote.get("balance", self.usdc_balance)) or 0
                )
            base = self.api.get_account_summary(currency=base_currency)
            if base:
                self.btc_balance = float(
                    base.get("equity", base.get("total_balance", base.get("balance", 0))) or 0
                )
                self.btc_available = float(
                    base.get("available", base.get("balance", self.btc_balance)) or 0
                )
            return {
                "usdc_balance": self.usdc_balance,
                "btc_balance": self.btc_balance,
                "usdc_available": self.usdc_available,
                "btc_available": self.btc_available,
            }
        except Exception as e:
            logger.error("Fetch balances (REST fallback): %s", e)
            return None

    def _fetch_open_orders(self):
        """获取当前所有挂单（含部分成交）"""
        try:
            orders = self.api.get_open_orders(self.cfg["instrument_name"])
            parsed = []
            instrument = self.cfg["instrument_name"]
            for o in orders:
                # 过滤：只保留我们策略标的的挂单，防止其他币种混入
                if o.get("instrument_name", "") != instrument:
                    logger.debug("Ignored non-%s order: %s %s @ %s",
                                 instrument, o.get("instrument_name"),
                                 o.get("direction"), o.get("price"))
                    continue
                filled = float(o.get("filled_amount", 0) or 0)
                amount = float(o.get("amount", 0) or 0)
                parsed.append({
                    "order_id": o.get("order_id", ""),
                    "side": o.get("direction", ""),
                    "price": float(o.get("price", 0) or 0),
                    "average_price": float(o.get("average_price", 0) or 0),
                    "amount": amount,
                    "filled": filled,
                    "remaining": amount - filled,
                    "state": o.get("order_state", ""),
                    "label": o.get("label", ""),
                    "time": o.get("creation_timestamp", ""),
                })
            self.open_orders = parsed
        except Exception as e:
            logger.error("Fetch open orders: %s", e)

    def _reconcile_open_order_fills(self):
        """Reconcile cumulative partial fills when an order WS event was missed."""
        tracked_orders = {
            self._our_buy_id: "buy",
            self._our_sell_id: "sell",
        }
        for order in self.open_orders:
            order_id = order.get("order_id")
            side = tracked_orders.get(order_id)
            if not order_id or not side:
                continue
            filled_amount = float(order.get("filled", 0) or 0)
            fill_price = float(
                order.get("average_price", 0)
                or order.get("price", 0)
                or 0
            )
            if filled_amount > 0 and fill_price > 0:
                average_price = float(order.get("average_price", 0) or 0)
                cumulative_cost = (
                    filled_amount * average_price if average_price > 0 else None
                )
                self._process_order_fill(
                    side, order_id, filled_amount, fill_price, cumulative_cost,
                )

    def _reconcile_missing_order(
        self, side_key: str, order_id: str, default_price: float,
    ) -> Optional[str]:
        """Reconcile a terminal order, including fills before cancellation."""
        order_result = self.api.get_order_state(order_id)
        if not order_result["success"]:
            return None

        parsed = self.api.parse_order_result(order_result.get("result") or {})
        state = parsed["state"]
        if state == "open":
            return state

        fill_price = parsed["average_price"] or default_price
        filled_amount = parsed["filled_amount"]
        if state == "filled" and filled_amount <= 0 and fill_price > 0:
            filled_amount = self.cfg["trade_size_usdc"] / fill_price
        if filled_amount > 0 and fill_price > 0:
            average_price = parsed["average_price"]
            cumulative_cost = (
                filled_amount * average_price if average_price > 0 else None
            )
            self._process_order_fill(
                side_key, order_id, filled_amount, fill_price, cumulative_cost,
            )
        return state

    # ------------------------------------------------------------------
    # 资金检查（买卖方向独立）
    # ------------------------------------------------------------------

    def _check_funds(self):
        """资金保护：USDC 或 BTC 价值低于 $200 时暂停对应方向"""
        threshold = self.cfg["min_poll_balance_usdc"]  # $200
        price = self.btc_index_price

        # USDC 检查
        if self.usdc_available < threshold:
            if not self.usdc_insufficient:
                self.usdc_insufficient = True
                self._log_info("USDC insufficient (%.2f < %.2f), buy paused",
                               self.usdc_available, threshold)
        else:
            if self.usdc_insufficient:
                self.usdc_insufficient = False
                self._log_info("USDC restored (%.2f), buy resumed", self.usdc_available)

        # BTC 检查（按市价折算 USDC）
        btc_value = self.btc_available * price if price > 0 else 0
        if btc_value < threshold:
            if not self.btc_insufficient:
                self.btc_insufficient = True
                self._log_info("BTC insufficient ($%.2f < %.2f), sell paused",
                               btc_value, threshold)
        else:
            if self.btc_insufficient:
                self.btc_insufficient = False
                self._log_info("BTC restored ($%.2f), sell resumed", btc_value)

    # ------------------------------------------------------------------
    # 主动挂单管理 — 提前在阈值位置挂 maker 单，避免行情波动来不及成交
    # ------------------------------------------------------------------

    def _recalc_thresholds(self):
        rv = self.daily_rv  # 用完整日化 RV 设通道宽度
        self.upper_threshold = self._round_price(self.anchor_price * (1 + rv))
        self.lower_threshold = self._round_price(self.anchor_price * (1 - rv))

    def _manage_maker_orders(self):
        """每轮循环维护一对 maker 限价单：
        买入单 @ 下阈值，卖出单 @ 上阈值。
        成交后自动更新锚点并重挂新单。
        """
        if not self._trading_enabled:
            return
        anchor = self.anchor_price
        if anchor <= 0 or self.daily_rv <= 0:
            return

        buy_price = self._round_price(self.lower_threshold)
        sell_price = self._round_price(self.upper_threshold)
        trade_size = self.cfg["trade_size_usdc"]

        # 获取当前所有挂单的 ID 集合，以及按价格索引
        current_ids = {o.get("order_id") for o in self.open_orders}
        orders_by_price = {}
        for o in self.open_orders:
            side = o.get("side")
            price = o.get("price")
            if side and price is not None:
                orders_by_price.setdefault(side, {})[round(price)] = o.get("order_id")

        # --- 防重复兜底：交易所已有同价位的挂单，但我们没追踪 → 认领回来 ---
        for side, our_attr, target_price in [
            ("buy", "_our_buy_id", buy_price),
            ("sell", "_our_sell_id", sell_price),
        ]:
            our_id = getattr(self, our_attr)
            if not our_id:
                existing = orders_by_price.get(side, {}).get(target_price)
                if existing and existing in current_ids:
                    setattr(self, our_attr, existing)
                    self._log_info("Reclaimed %s order %s at price %d", side, existing, target_price)

        # WS 丢失时，按订单累计成交量补齐仍在挂单列表中的部分成交。
        self._reconcile_open_order_fills()

        # --- 检测成交：订单消失后查交易所订单状态判断是否真成交 ---
        # Bugfix v1.9: 不再依赖余额变化（期权估值会污染 BTC balance），
        # 改为直接查询 get_order_state 的 order_state 字段。
        for side_key, our_id_attr in [("sell", "_our_sell_id"), ("buy", "_our_buy_id")]:
            our_id = getattr(self, our_id_attr)
            if our_id and our_id not in current_ids:
                # 查交易所订单状态确认是否成交
                try:
                    default_price = sell_price if side_key == "sell" else buy_price
                    state = self._reconcile_missing_order(
                        side_key, our_id, default_price,
                    )
                    if state == "open":
                        # 交易所说还在，但 get_open_orders 没返回——可能是 API 延迟，跳过本轮
                        self._log_info(
                            "%s order %s missing from open list but state=open, skipping",
                            side_key, our_id,
                        )
                        continue
                except Exception as e:
                    self._log_info("%s order %s get_order_state failed: %s", side_key, our_id, e)
                    # API 失败时回退：不处理，留到下一轮再说
                    continue

                if state != "filled":
                    self._log_info(
                        "%s order %s was cancelled/removed (%s)",
                        side_key, our_id, state or "unknown",
                    )
                setattr(self, our_id_attr, None)

        # --- 取消价位不对的挂单 ---
        for o in self.open_orders:
            target = buy_price if o["side"] == "buy" else sell_price
            if o["order_id"] == self._our_buy_id or o["order_id"] == self._our_sell_id:
                if abs(o["price"] - target) > self.cfg.get("stale_threshold", 0.5):
                    r = self.api.cancel_order(o["order_id"])
                    if not r["success"]:
                        self._route_api_error("cancel", r, o.get("side"))
                    if o["order_id"] == self._our_buy_id:
                        self._our_buy_id = None
                    else:
                        self._our_sell_id = None
                    self._log_info("Cancelled stale %s order at %.2f (target %.2f)",
                                   o["side"], o["price"], target)

        # --- 孤儿单清理：交易所滞留、引擎未追踪、且已偏离目标价的本策略挂单 ---
        stale_th = self.cfg.get("stale_threshold", 0.5)
        for o in self.open_orders:
            oid = o["order_id"]
            if oid == self._our_buy_id or oid == self._our_sell_id:
                continue
            target_px = buy_price if o["side"] == "buy" else sell_price
            if abs(o["price"] - target_px) > stale_th:
                r = self.api.cancel_order(oid)
                if not r["success"]:
                    self._route_api_error("cancel", r, o.get("side"))
                self._log_info("Cancelled orphan %s order %s at %.2f (target %.2f)",
                               o["side"], oid, o["price"], target_px)

        # --- 冷静期：成交后 3 分钟内不挂新单（但成交检测照常进行）---
        if time.time() < self._cooldown_until:
            # 每 5 轮（约 2.5 分钟）才打印一次冷静期提示，防刷屏
            if not hasattr(self, '_cooldown_log_counter'):
                self._cooldown_log_counter = 0
            self._cooldown_log_counter += 1
            if self._cooldown_log_counter % 5 == 1:
                self._log_info("Cooldown active, skipping new orders (%ds left)",
                               int(self._cooldown_until - time.time()))
            return

        # --- 计算下单量 ---
        def calc_amount(price):
            if price <= 0:
                return 0
            amt = self._round_amount(trade_size / price)
            return amt if amt >= self.min_trade_amount else 0

        # --- 挂买入单（防重复：检查交易所是否已有同价位挂单）---
        buy_amount = calc_amount(buy_price)
        buy_exists = any(
            o["side"] == "buy" and abs(o["price"] - buy_price) < self.cfg.get("stale_threshold", 0.5)
            for o in self.open_orders
        )
        if not self._our_buy_id and not buy_exists and buy_amount > 0 and not self.usdc_insufficient:
            if buy_amount * buy_price <= self.usdc_available:
                self._log_info("Placing buy maker @ %.2f for %.6f BTC", buy_price, buy_amount)
                result = self.api.buy(
                    self.cfg["instrument_name"], amount=buy_amount,
                    order_type="limit", price=buy_price,
                    label="maker_buy", post_only=True,
                )
                if result["success"]:
                    parsed = self.api.parse_order_result(result["result"] or {})
                    self._our_buy_id = parsed["order_id"]
                    self._log_info("Buy maker placed: ID %s", self._our_buy_id)
                else:
                    self._route_api_error("buy", result, "buy")
            else:
                self._log_info("Buy skipped: USDC insufficient (need %.2f have %.2f)",
                               buy_amount * buy_price, self.usdc_available)
        elif self._our_buy_id:
            # 防刷屏：不成交时每 10 轮才打一次常规消息
            if not hasattr(self, '_buy_skip_counter'):
                self._buy_skip_counter = 0
            self._buy_skip_counter += 1
            if self._buy_skip_counter % 10 == 1:
                self._log_info("Buy already active: %s", self._our_buy_id)
        else:
            if not hasattr(self, '_buy_skip_counter'):
                self._buy_skip_counter = 0
            self._buy_skip_counter += 1
            if self._buy_skip_counter % 10 == 1:
                self._log_info("Buy skipped: amount=%.6f usdc_insuff=%s", buy_amount, self.usdc_insufficient)

        # --- 挂卖出单（防重复：检查交易所是否已有同价位挂单）---
        sell_amount = calc_amount(sell_price)
        sell_exists = any(
            o["side"] == "sell" and abs(o["price"] - sell_price) < self.cfg.get("stale_threshold", 0.5)
            for o in self.open_orders
        )
        if not self._our_sell_id and not sell_exists and sell_amount > 0 and not self.btc_insufficient:
            if sell_amount <= self.btc_available:
                self._log_info("Placing sell maker @ %.2f for %.6f BTC", sell_price, sell_amount)
                result = self.api.sell(
                    self.cfg["instrument_name"], amount=sell_amount,
                    order_type="limit", price=sell_price,
                    label="maker_sell", post_only=True,
                )
                if result["success"]:
                    parsed = self.api.parse_order_result(result["result"] or {})
                    self._our_sell_id = parsed["order_id"]
                    self._log_info("Sell maker placed: ID %s", self._our_sell_id)
                else:
                    self._route_api_error("sell", result, "sell")
            else:
                self._log_info("Sell skipped: BTC insufficient (need %.6f have %.6f)",
                               sell_amount, self.btc_available)
        elif self._our_sell_id:
            if not hasattr(self, '_sell_skip_counter'):
                self._sell_skip_counter = 0
            self._sell_skip_counter += 1
            if self._sell_skip_counter % 10 == 1:
                self._log_info("Sell already active: %s", self._our_sell_id)
        else:
            if not hasattr(self, '_sell_skip_counter'):
                self._sell_skip_counter = 0
            self._sell_skip_counter += 1
            if self._sell_skip_counter % 10 == 1:
                self._log_info("Sell skipped: amount=%.6f btc_insuff=%s", sell_amount, self.btc_insufficient)

    # ------------------------------------------------------------------
    # 交易执行
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    def _set_status(self, s):
        with self._lock:
            self.status = s

    def _add_error(self, msg):
        with self._lock:
            self.errors.append({"time": datetime.now(BJT).isoformat(), "msg": msg})
        if len(self.errors) > 100:
            self.errors = self.errors[-100:]

    def _route_api_error(self, op: str, result: dict, side: Optional[str] = None):
        """参考 ccxt 错误分级，把 API 失败按类别分流处理。

            - insufficient_funds: 挂起对应方向的余额不足标志（余额轮询会重置）
            - auth:               记入仪表盘 errors（致命，需人工排查）
            - rate_limit / exchange_not_available / exchange_error: 已在请求层退避重试，仅日志
            - order_not_found:    视为已处理（订单已不存在）
            - 其他:               记入仪表盘 errors
            """
        if not result or result.get("success"):
            return
        category = result.get("error_category", "unknown")
        code = result.get("error_code")
        err = result.get("error")
        msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
        self._log_info("%s FAILED [%s] code=%s: %s", op, category, code, msg)

        if category == "insufficient_funds":
            if side == "buy":
                self.usdc_insufficient = True
            elif side == "sell":
                self.btc_insufficient = True
        elif category == "auth":
            self._add_error(f"认证失败({code}): {msg}")
        elif category in ("rate_limit", "exchange_not_available", "exchange_error"):
            pass  # 已在请求层退避重试
        elif category == "order_not_found":
            pass  # 视为已处理
        else:
            self._add_error(f"{op} error[{category}]: {msg}")
    def _log_info(self, fmt, *args):
        logger.info(fmt, *args)
