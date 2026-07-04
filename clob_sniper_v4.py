#!/usr/bin/env python3.11
"""
clob_sniper_v4.py — 三防线双向 RE + OBI WATCH 引擎
========================================================
防线1: τ ∈ [5,25] 黄金时间窗
防线2: |RE| ≥ 0.98  BSM概率偏离
防线3: OBI ≥ 0.75(买YES) / OBI ≤ 0.25(买NO) 币安盘口不平衡

当前模式: 🔍 WATCH ONLY — 只监控不下单
上线只需取消注释 SecureClient.place_market_order() 行
"""
import os, sys, json, time, asyncio, websockets, logging, traceback
from datetime import datetime, timezone

# ============================================================
# 日志
# ============================================================
LOG_FILE = "/root/clob_sniper_v4_watch.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger("SniperV4")

# ============================================================
# 全局共享状态与配置
# ============================================================
SHARED_STATE_PATH = "/dev/shm/poly_state.json"
STALENESS_THRESHOLD_SEC = 1.5

# 黄金过滤器阈值
TAU_MIN = 5
TAU_MAX = 25
RE_THRESHOLD = 0.98

# 币安盘口 OBI 严格过滤阈值
OBI_BUY_YES_LIMIT = 0.75
OBI_BUY_NO_LIMIT = 0.25

# 飞前内存锁
executed_flights = {}

# 币安 OBI 全局内存字典（WebSocket 协程每 100ms 更新）
binance_obi_state = {
    "BTC": 0.50,
    "ETH": 0.50,
    "SOL": 0.50
}

# ============================================================
# 1. 币安高频 WebSocket 监听协程
# ============================================================
class BinanceOBIMonitor:
    def __init__(self):
        self.ws_url = "wss://fstream.binance.com/stream?streams=btcusdt@depth5@100ms/ethusdt@depth5@100ms/solusdt@depth5@100ms"
        self.symbol_map = {
            "btcusdt": "BTC",
            "ethusdt": "ETH",
            "solusdt": "SOL"
        }

    async def start_listening(self):
        global binance_obi_state
        while True:
            try:
                logger.info("正在建立与币安高频 WebSocket 的连接...")
                async with websockets.connect(self.ws_url) as ws:
                    logger.info("币安高频 WebSocket 连接成功。")
                    while True:
                        message = await ws.recv()
                        data = json.loads(message)

                        stream = data.get("stream", "")
                        symbol_key = stream.split("@")[0]
                        coin = self.symbol_map.get(symbol_key)

                        if not coin:
                            continue

                        depth_data = data.get("data", {})
                        bids = depth_data.get("b", [])  # 买方 5 档
                        asks = depth_data.get("a", [])  # 卖方 5 档

                        total_bid_qty = sum(float(b[1]) for b in bids)
                        total_ask_qty = sum(float(a[1]) for a in asks)

                        denominator = total_bid_qty + total_ask_qty
                        if denominator > 0:
                            obi = total_bid_qty / denominator
                            binance_obi_state[coin] = round(obi, 3)

            except Exception as e:
                logger.error(f"币安 WebSocket 中断，3秒后重连: {e}")
                await asyncio.sleep(3.0)

# ============================================================
# 2. SecureClient 初始化（不包含在 Git 仓库中）
# ============================================================
# SecureClient 初始化代码在 .env + 本地版本中。
# 如需部署上线，读取本地 clob_sniper_v4_local.py（已 .gitignore 不追踪）

# ============================================================
# 3. 单次扫描逻辑 (scan_once) — 三防线过滤
# ============================================================
def scan_once() -> int:
    """扫描 poly_state + OBI，触发 WATCH 信号"""
    global executed_flights, binance_obi_state

    if not os.path.exists(SHARED_STATE_PATH):
        return 0

    try:
        with open(SHARED_STATE_PATH, "r") as f:
            state_dict = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return 0

    now = time.time()
    triggered = 0

    for coin in ["BTC", "ETH", "SOL"]:
        data = state_dict.get(coin)
        if not data:
            continue

        re = data.get("re", 0.0)
        time_left = data.get("time_left", 300)
        poly_yes = data.get("poly_yes", 0.5)
        updated_at = data.get("updated_at", 0.0)

        # 防线1: 1.5秒时效
        if now - updated_at > STALENESS_THRESHOLD_SEC:
            continue

        # 防线2: 5-25秒黄金时间窗
        if not (TAU_MIN <= time_left <= TAU_MAX):
            continue

        # 自动计算窗口绝对结束时间戳，用于去重落锁
        window_end_epoch = int(updated_at + time_left)
        flight_key = f"{coin}_{window_end_epoch}"

        # 飞前锁检测
        if flight_key in executed_flights:
            continue

        # 防线3: 读取该币种最新币安 OBI
        obi = binance_obi_state.get(coin, 0.50)

        # === 路径一：RE ≥ +0.98 + OBI ≥ 0.75 → BUY YES ===
        if re >= RE_THRESHOLD and obi >= OBI_BUY_YES_LIMIT:
            executed_flights[flight_key] = True
            logger.warning(
                f"👀 [WATCH: BUY YES] {coin} | "
                f"RE={re:+.2f} | OBI={obi:.3f} | "
                f"τ={time_left:.0f}s | wnd={window_end_epoch} | "
                f"YES=${poly_yes:.3f}"
            )
            triggered += 1

        # === 路径二：RE ≤ -0.98 + OBI ≤ 0.25 → BUY NO ===
        elif re <= -RE_THRESHOLD and obi <= OBI_BUY_NO_LIMIT:
            executed_flights[flight_key] = True
            logger.warning(
                f"👀 [WATCH: BUY NO] {coin} | "
                f"RE={re:+.2f} | OBI={obi:.3f} | "
                f"τ={time_left:.0f}s | wnd={window_end_epoch} | "
                f"NO=${1.0-poly_yes:.3f}"
            )
            triggered += 1

        # 日志：RE 触发但 OBI 阻挡（仅 WATCH 模式下调试用）
        elif re >= RE_THRESHOLD:
            logger.info(
                f"  ⛔ [OBI 阻挡] {coin} RE={re:+.2f} OBI={obi:.3f} "
                f"(需≥{OBI_BUY_YES_LIMIT}) τ={time_left:.0f}s"
            )
        elif re <= -RE_THRESHOLD:
            logger.info(
                f"  ⛔ [OBI 阻挡] {coin} RE={re:+.2f} OBI={obi:.3f} "
                f"(需≤{OBI_BUY_NO_LIMIT}) τ={time_left:.0f}s"
            )

    return triggered

# ============================================================
# 4. 主异步循环
# ============================================================
async def main():
    logger.info("=" * 60)
    logger.info("  CLOB Sniper V4 — 三防线 WATCH 引擎")
    logger.info(f"  时间窗: {TAU_MIN}s ≤ τ ≤ {TAU_MAX}s")
    logger.info(f"  RE阈值: |RE| ≥ {RE_THRESHOLD}")
    logger.info(f"  OBI阈值: ≥{OBI_BUY_YES_LIMIT}(YES) / ≤{OBI_BUY_NO_LIMIT}(NO)")
    logger.info(f"  模式: 🔍 WATCH ONLY (不下单)")
    logger.info(f"  飞前锁: 已激活")
    logger.info("=" * 60)

    # 启动 OBI 监听协程
    monitor = BinanceOBIMonitor()
    obi_task = asyncio.create_task(monitor.start_listening())

    # 主扫描循环
    scan_interval = 0.8
    while True:
        try:
            n = scan_once()
            if n > 0:
                logger.info(f"  本轮触发 {n} 个信号")
            await asyncio.sleep(scan_interval)
        except KeyboardInterrupt:
            logger.info("用户中断")
            break
        except Exception as e:
            logger.warning(f"扫描异常: {e}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
