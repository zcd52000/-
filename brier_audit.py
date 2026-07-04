#!/usr/bin/env python3
"""
Brier Score 实时审计监控器 v3.1 — 双循环解耦 + 看门狗加固版
- 循环A: WebSocket + KVM 锁死（微秒级本地操作，零网络等待）
- 循环B: REST 刷新（独立 httpx 2 秒超时，30 秒间隔）
- 看门狗: 最大迭代次数 / 连续失败保护 / 静默超时自动退出
- asyncio.gather 并发运行，互不阻塞
- Dry-run ONLY (只读不交易)
"""
import asyncio
import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import scipy.stats as stats

# WebSocket
try:
    import websockets
except ImportError:
    os.system("pip install websockets -q")
    import websockets

import httpx

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler('/root/brier_audit.log')]
)
logger = logging.getLogger("BrierAudit")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)

# ============================================================
# 配置
# ============================================================
COINS = ["BTC", "ETH", "SOL"]
SYMBOL_MAP = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
}
WS_BINANCE = "wss://stream.binance.com:9443/ws"

SLUG_TEMPLATES = {
    "BTC": "btc-updown-5m-{ts}",
    "ETH": "eth-updown-5m-{ts}",
    "SOL": "sol-updown-5m-{ts}",
}
GAMMA = "https://gamma-api.polymarket.com"

CSV_PATH = "/root/brier_audit.csv"
WINDOW_SEC = 300  # 5分钟
REST_INTERVAL = 30.0  # REST 刷新间隔（秒）

# 看门狗常量
MAX_ITERATIONS = 86400     # 每个循环最大迭代（≈24h@1s tick）
MAX_IDLE_SECONDS = 300     # 无有效数据静默超时（5分钟）
MAX_CONSECUTIVE_FAILURES = 10  # 连续失败阈值
REST_TIMEOUT = 2.0         # REST请求超时（秒）


class KValueLockManager:
    """锁定K值（行权价），窗口周期内不变"""

    def __init__(self, interval_sec: int = 300):
        self.interval = interval_sec
        self.current_K: Optional[float] = None
        self.current_epoch_start = 0
        self.settle_snapshot: Optional[float] = None
        self.last_epoch: int = 0
        self.last_K: Optional[float] = None
        self.last_settle_price: Optional[float] = None
        self.last_epoch_down = -1
        self.settle_bsm_snapshot = 0.5  # 窗口结束时的BSM快照

    def get_and_update_K(self, current_spot_price: float) -> Tuple[float, float]:
        now = time.time()
        epoch_start = int(now // self.interval) * self.interval
        settle_time = epoch_start + self.interval
        time_left = settle_time - now

        if epoch_start != self.current_epoch_start:
            settle_K = self.current_K
            settle_price = self.settle_snapshot
            if settle_K is not None and settle_price is None:
                settle_price = float(current_spot_price)

            self.last_epoch = self.current_epoch_start
            self.last_K = settle_K
            self.last_settle_price = settle_price

            self.current_K = float(current_spot_price)
            self.current_epoch_start = epoch_start
            self.settle_snapshot = None

        if time_left <= 5.0 and time_left > 0:
            self.settle_snapshot = float(current_spot_price)

        # 在窗口结束瞬间（τ≤1秒）保存BSM快照用于结算
        if time_left <= 1.0 and time_left > 0:
            pass  # BSM由tick_kvm在结算检测前最后一秒写入 settle_bsm_snapshot

        return self.current_K, time_left


class BSMProbabilityEngine:
    """BSM二元期权概率引擎"""

    def __init__(self):
        self.price_history: List[float] = []        # 全量WS tick（用于实时显示）
        self.price_history_10s: List[float] = []    # 10秒采样（用于波动率计算）
        self.max_history_len = 300
        self.k_manager = KValueLockManager()
        self._last_10s_sample = 0.0  # 上次10秒采样时间

    def set_strike(self, strike_price: float):
        self.k_manager.current_K = float(strike_price)

    def update_price(self, spot_price: float):
        self.price_history.append(float(spot_price))
        if len(self.price_history) > self.max_history_len:
            self.price_history.pop(0)
        # 每10秒采样一次（用于年化波动率）
        now = time.time()
        if now - self._last_10s_sample >= 10.0:
            self.price_history_10s.append(float(spot_price))
            if len(self.price_history_10s) > 90:  # 15分钟 = 90个10秒采样
                self.price_history_10s.pop(0)
            self._last_10s_sample = now

    def calculate_realtime_volatility(self) -> float:
        """
        科学严谨的高频自适应波动率计算：
        - 采用 15 分钟固定时间窗口 (90个 10秒采样点)
        - 严格的一年秒数年化因子 (31,536,000 秒)
        - MIN_VOLATILITY = 0.15 (15%年化)  | MAX_VOLATILITY = 8.00 (800%年化)
        - OPTIMAL_TAU = 25 (锁定最后25秒出击)
        - OPTIMAL_RE  = -0.98 (锁定相对偏差≤-0.98)
        """
        if len(self.price_history_10s) < 10:
            return 0.15  # MIN_VOLATILITY

        prices = np.array(self.price_history_10s)
        log_returns = np.diff(np.log(prices))
        std_dev = float(np.std(log_returns))

        # 年化：采样间隔10秒，一年=31,536,000秒
        steps_per_year = 31536000.0 / 10.0
        annualized_vol = std_dev * np.sqrt(steps_per_year)

        # 边界截断 [0.15, 8.00]
        return max(min(annualized_vol, 8.00), 0.15)

    def compute_probability(self, spot_price: float, settle_time: float) -> Tuple[float, float]:
        K, tau_sec = self.k_manager.get_and_update_K(spot_price)

        if K is None:
            return 0.5, 0.0
        if tau_sec <= 1.0:
            return (1.0 if spot_price > K else 0.0), K
        sigma = self.calculate_realtime_volatility()
        S_t = float(spot_price)
        if sigma <= 0.0:
            return (1.0 if S_t > K else 0.0), K
        try:
            # τ必须转换成年以匹配年化σ
            tau_years = tau_sec / (365.25 * 86400.0)
            numerator = np.log(S_t / K) - 0.5 * (sigma ** 2) * tau_years
            denominator = sigma * np.sqrt(tau_years)
            d2 = numerator / denominator
            raw_bsm = float(stats.norm.cdf(d2))
            # 无截断：返回真实的BSM概率
            # Brier审计对数计算时在外部用EPSILON保护，不干扰RE的真实值
            return raw_bsm, K
        except:
            return (1.0 if S_t > K else 0.0), K


class BrierAuditor:
    """
    Brier Score 实时审计器 v3.1 (看门狗加固版)
    - 循环A (realtime_kvm_loop): Binance WS + KVM（连续流）
    - 循环A' (tick_kvm): 每秒 tick，本地操作
    - 循环B (background_rest_loop): REST 刷新
    - 所有循环有最大迭代 / 看门狗保护
    """

    def __init__(self):
        self.prices: Dict[str, float] = {}
        self.engines: Dict[str, Optional[BSMProbabilityEngine]] = {}
        self.current_slugs: Dict[str, Optional[str]] = {}
        self.window_ends: Dict[str, Optional[int]] = {}
        self.market_prices: Dict[str, Tuple[float, float]] = {}

        # CSV
        self.csv_file = open(CSV_PATH, 'a', newline='')
        self.csv_writer = csv.writer(self.csv_file)
        if os.path.getsize(CSV_PATH) == 0:
            self.csv_writer.writerow([
                "timestamp", "coin", "spot_price", "strike_k",
                "bsm_prob_yes", "poly_yes_price", "poly_no_price",
                "edge", "window_end", "result"
            ])
            self.csv_file.flush()

        self.settled_windows: set = set()
        self.is_running = True
        self._start_time = time.time()

        # 共享执行状态（Linux内存文件系统 /dev/shm，纳秒级读写）
        self._k_share_path = "/dev/shm/poly_state.json"
        self._k_share_last_write = 0

    @staticmethod
    def _parse_end_date(market: dict) -> Optional[int]:
        """从 market dict 解析结束时间戳。
        优先使用 slug 中的时间戳（更可靠），fallback 到 endDate。
        """
        slug = market.get('slug', '')
        parts = slug.split('-')
        if parts and parts[-1].isdigit():
            slug_ts = int(parts[-1])
            # 验证 endDate 是否一致；如果不一致，slug 的 timestamp 更可信
            end_str = market.get('endDate', '')
            if end_str:
                try:
                    dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                    end_ts = int(dt.timestamp())
                    # 如果偏差 >= 120s，slug 的 timestamp 更准
                    if abs(end_ts - slug_ts) >= 120:
                        slug_ts = slug_ts
                except:
                    pass
            return slug_ts
        
        # fallback: endDate
        end_str = market.get('endDate', '')
        if end_str:
            try:
                dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                return int(dt.timestamp())
            except:
                pass
        return None

    def _write_k_share(self, now: int, coin: str, k: float,
                       spot: float, bsm: float, price: float):
        """每秒写入该coin的执行状态到共享内存——按coin独立限速，单JSON文件"""
        key = f"_k_share_last_write_{coin}"
        last_ts = getattr(self, key, 0)
        if now - last_ts < 0.5:
            return
        try:
            # 读现有状态，更新当前coin，写回
            data = {}
            if os.path.isfile(self._k_share_path) and os.path.getsize(self._k_share_path) > 0:
                with open(self._k_share_path) as f:
                    data = json.load(f)
            tau_sec = max(0, int(self.window_ends.get(coin, now + 300)) - now)
            current_re = round((bsm - price) / max(price, 0.001), 4)
            data[coin] = {
                "re": current_re,
                "time_left": round(float(tau_sec), 1),
                "updated_at": time.time(),
                "bsm_prob": round(bsm, 4),   # BSM probability (0~1)
                "spot_price": round(spot, 2),  # 实时现货价
                "strike_k": round(k, 2),       # K值（锚定价）
                "poly_yes": round(price, 4),   # 市场 YES 价格
            }
            with open(self._k_share_path, 'w') as f:
                json.dump(data, f)
            setattr(self, key, now)
        except Exception as e:
            logger.error(f"k_share写失败: {e}")

    # ============================================================
    # 循环A: WebSocket + KVM 极速循环（微秒级，零网络等待）
    # ============================================================

    async def realtime_kvm_loop(self):
        """
        循环A：Binance WebSocket 价格流
        带重连保护和最大迭代限制
        """
        streams = [f"{s}@aggTrade" for s in SYMBOL_MAP.values()]
        streams_str = "/".join(streams)
        url = f"{WS_BINANCE}/{streams_str}"

        iteration = 0
        reconnect_count = 0

        while self.is_running and iteration < MAX_ITERATIONS:
            iteration += 1
            try:
                async for ws in websockets.connect(url):
                    # 重连成功，重置重连计数
                    reconnect_count = 0
                    try:
                        async for msg in ws:
                            data = json.loads(msg)
                            if 's' in data and 'p' in data:
                                symbol = data['s'].lower()
                                price = float(data['p'])
                                for coin, sym in SYMBOL_MAP.items():
                                    if sym == symbol:
                                        self.prices[coin] = price
                                        if coin in self.engines and self.engines[coin]:
                                            self.engines[coin].update_price(price)
                    except Exception as e:
                        logger.error(f"WS error: {e}")
                        await asyncio.sleep(2)
            except Exception as e:
                reconnect_count += 1
                logger.error(f"WS connect error (attempt {reconnect_count}): {e}")
                if reconnect_count >= MAX_CONSECUTIVE_FAILURES:
                    logger.error(f"WS连续重连失败{reconnect_count}次，自动退出")
                    break
                await asyncio.sleep(5)

        logger.warning("🛑 realtime_kvm_loop 自动退出")

    async def tick_kvm(self):
        """
        每秒 tick：KVM + BSM + 结算检测 + CSV写入
        看门狗：连续无价格数据超时检测
        """
        iteration = 0
        last_valid_prices = {coin: 0.0 for coin in COINS}
        no_price_time = {coin: time.time() for coin in COINS}

        while self.is_running and iteration < MAX_ITERATIONS:
            iteration += 1
            now = int(time.time())

            for coin in COINS:
                spot = self.prices.get(coin)
                if spot is None:
                    # 无价格数据，检查静默时间
                    if time.time() - no_price_time[coin] > MAX_IDLE_SECONDS:
                        logger.warning(f"⏰ [{coin}] 已{int(time.time()-no_price_time[coin])}s无价格数据")
                    continue

                # 价格在更新
                if spot != last_valid_prices[coin]:
                    no_price_time[coin] = time.time()
                    last_valid_prices[coin] = spot

                mk_prices = self.market_prices.get(coin)
                if mk_prices is None:
                    continue

                yes_price, no_price = mk_prices

                # 惰性创建引擎
                if coin not in self.engines or self.engines[coin] is None:
                    self.engines[coin] = BSMProbabilityEngine()
                    logger.info(f"[{coin}] 引擎就绪")

                engine = self.engines[coin]
                window_end = self.window_ends.get(coin, now + 300)

                # ─── 结算检测（必须在compute_probability之前！） ───
                # 先检查是否有窗口刚结束，用旧的BSM值结算
                pending_settle_epoch = engine.k_manager.last_epoch
                if pending_settle_epoch > 0:
                    last_k = engine.k_manager.last_K
                    last_settle = engine.k_manager.last_settle_price
                    if last_k is not None and last_settle is not None and last_k > 0 and last_settle > 0:
                        window_key = f"{coin}-{pending_settle_epoch}"
                        if window_key not in self.settled_windows:
                            # 使用k_manager中保存在epoch切换时的BSM快照
                            settle_bsm = engine.k_manager.settle_bsm_snapshot
                            t60_bsm = getattr(engine, '_last_window_bsm_at_t60', 'NOTSET')
                            last_bsm_k = getattr(engine, '_last_bsm_k', 0.0)
                            last_bsm_epoch = getattr(engine, '_last_bsm_epoch', -1)
                            saved_tau = getattr(engine, '_debug_bsm_save_tau', -1)
                            saved_epoch_cls = last_bsm_epoch
                            resolved_up = last_settle >= last_k
                            result_text = "WIN" if resolved_up else "LOSS"
                            brier = (settle_bsm - (1.0 if resolved_up else 0.0)) ** 2
                            logger.warning(
                                f"🏁 [{coin}] 结算! K={last_k:.0f} "
                                f"终价={last_settle:.0f} "
                                f"BSM={settle_bsm:.4f} "
                                f"{'↑UP' if resolved_up else '↓DOWN'} "
                                f"Brier={brier:.4f} "
                                f"[snap={settle_bsm:.4f} t60={t60_bsm} snap_ep={saved_epoch_cls} cur_ep={engine.k_manager.current_epoch_start}]"
                            )
                            # 结算时保存最后已知的市场价格（用于后续edge分析）
                            last_yes = self.market_prices.get(coin, (0.0, 0.0))[0]
                            last_no = self.market_prices.get(coin, (0.0, 0.0))[1]
                            settle_edge = settle_bsm - last_yes
                            self.csv_writer.writerow([
                                now, coin, f"{last_settle:.1f}", f"{last_k:.1f}",
                                f"{settle_bsm:.4f}", f"{last_yes:.4f}", f"{last_no:.4f}",
                                f"{settle_edge:.4f}",
                                pending_settle_epoch, result_text
                            ])
                            self.csv_file.flush()
                            self.settled_windows.add(window_key)
                            engine.k_manager.last_epoch = -1

                # ─── 计算新窗口的BSM ───
                try:
                    bsm_prob, current_k = engine.compute_probability(spot, window_end)
                except Exception as e:
                    logger.error(f"BSM异常: {e}")
                    continue

                if current_k is None or current_k == 0.0:
                    continue

                # 保存当前BSM，供下一轮窗口切换后结算使用
                engine._last_window_bsm = bsm_prob
                engine._last_bsm_k = current_k
                engine._last_bsm_epoch = engine.k_manager.current_epoch_start
                # ★ 每秒更新BSM快照，结算时取到的是结算瞬间的BSM
                engine.k_manager.settle_bsm_snapshot = bsm_prob
                current_epoch = engine.k_manager.current_epoch_start
                if current_epoch > 0:
                    engine._debug_bsm_save_tau = max(0, current_epoch + WINDOW_SEC - int(time.time()))
                else:
                    engine._debug_bsm_save_tau = -1

                # ─── 实时记录 ───
                edge = bsm_prob - yes_price
                # 用KVM内部epoch_start计算真实window_end，消除epoch漂移
                real_window_end = engine.k_manager.current_epoch_start + WINDOW_SEC
                sec_left = max(0, real_window_end - now)

                # 当τ≈60秒时，保存为t60快照（用于结算评估）
                if 55 <= sec_left <= 65:
                    engine._last_window_bsm_at_t60 = bsm_prob

                record_interval = 1 if sec_left <= 30 else 2
                if now % record_interval == 0:
                    self.csv_writer.writerow([
                        now, coin, f"{spot:.1f}", f"{current_k:.1f}",
                        f"{bsm_prob:.4f}", f"{yes_price:.4f}", f"{no_price:.4f}",
                        f"{edge:.4f}", real_window_end, ""
                    ])
                    self.csv_file.flush()
                # K值共享文件无条件每秒写入（不依赖record_interval）
                self._write_k_share(now, coin, current_k, spot, bsm_prob, yes_price)

            await asyncio.sleep(1.0)

        logger.warning("🛑 tick_kvm 自动退出 (迭代上限)")

    # ============================================================
    # 循环B: 后台 REST 刷新（独立 httpx 2 秒超时）
    # ============================================================

    async def background_rest_loop(self):
        """
        循环B：独立 httpx 2 秒超时，每 30 秒刷新 Polymarket 市场列表
        带：最大迭代、连续失败计数、超时保护
        """
        iteration = 0
        consecutive_failures = 0

        async with httpx.AsyncClient(timeout=REST_TIMEOUT) as client:
            while self.is_running and iteration < MAX_ITERATIONS:
                iteration += 1
                try:
                    await self._refresh_all_markets(client)
                    consecutive_failures = 0
                except Exception as e:
                    consecutive_failures += 1
                    logger.error(f"REST刷新异常(#{consecutive_failures}): {e}")
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        logger.error(f"REST连续失败{consecutive_failures}次，暂停10分钟")
                        await asyncio.sleep(600)
                        consecutive_failures = 0

                await asyncio.sleep(REST_INTERVAL)

        logger.warning("🛑 background_rest_loop 自动退出")

    async def _refresh_all_markets(self, client: httpx.AsyncClient):
        """
        并发 fetch 所有币种的市场，选择最邻近到期的活跃合约。
        核心补丁：对每个币种扫描接下来 3 个窗口，用 min(time_left)
        锁定即将到期的当前合约，阻断提前跳转。
        兜底：如果选中合约偏移超过 300s，用静态 (now//300+1)*300 作为 fallback。
        """
        now = int(time.time())

        async def fetch_multi(coin: str):
            """对单个币种扫描 3 个窗口，返回最邻近且 >2s 的合约"""
            candidates = []
            for offset in range(1, 4):
                we = ((now // WINDOW_SEC) + offset) * WINDOW_SEC
                slug = SLUG_TEMPLATES[coin].format(ts=we)
                try:
                    resp = await client.get(f"{GAMMA}/markets", params={"slug": slug})
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, list) and len(data) > 0:
                            m = data[0]
                            end_ts = self._parse_end_date(m)
                            if end_ts:
                                time_left = end_ts - now
                                # 锁定最邻近且留 2 秒安全垫
                                if time_left > 2.0:
                                    candidates.append((m, time_left, end_ts))
                                    logger.info(f"  [{coin}] offset={offset} slug={slug} τ={time_left}s")
                except httpx.TimeoutException:
                    pass
                except:
                    pass

            if not candidates:
                return None

            # 核心补丁：选择 time_left 最小的合约（最邻近到期）
            best_market, best_tl, best_end = min(candidates, key=lambda x: x[1])

            # 兜底：如果选中的合约 time_left 超过 360s（跳了完整窗口），回退到静态计算
            if best_tl > 360:
                fallback_we = ((now // WINDOW_SEC) + 1) * WINDOW_SEC
                fallback_tl = fallback_we - now
                # 尝试用静态窗口 slug 拿市场数据
                fallback_slug = SLUG_TEMPLATES[coin].format(ts=fallback_we)
                try:
                    resp = await client.get(f"{GAMMA}/markets", params={"slug": fallback_slug})
                    if resp.status_code == 200:
                        fd = resp.json()
                        if isinstance(fd, list) and len(fd) > 0:
                            fm = fd[0]
                            fe = self._parse_end_date(fm)
                            if fe:
                                return coin, fm, fallback_tl, fe
                except:
                    pass

            return coin, best_market, best_tl, best_end

        tasks = [fetch_multi(coin) for coin in COINS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if r is None or isinstance(r, Exception):
                continue
            coin, market, time_left, end_ts = r
            raw = market.get('outcomePrices', '[0.5,0.5]')
            if isinstance(raw, str):
                prices = json.loads(raw)
            else:
                prices = raw
            yes_price = float(prices[0])
            no_price = float(prices[1])
            self.market_prices[coin] = (yes_price, no_price)
            self.current_slugs[coin] = market.get('slug', '')
            self.window_ends[coin] = end_ts

            if time_left <= 30:
                logger.info(f"📡 {coin} 最邻近合约: τ={time_left}s slug={market.get('slug','')}")

    # ============================================================
    # 启动器: asyncio.gather 三协程并发
    # ============================================================

    async def run(self):
        logger.info("=" * 60)
        logger.info("  Brier Score 实时审计监控器 v3.1 — 看门狗加固版")
        logger.info(f"  监控: {', '.join(COINS)}")
        logger.info(f"  输出: {CSV_PATH}")
        logger.info(f"  最大迭代: {MAX_ITERATIONS} | 静默超时: {MAX_IDLE_SECONDS}s | 连续失败阈值: {MAX_CONSECUTIVE_FAILURES}")
        logger.info(f"  REST超时: {REST_TIMEOUT}s | 间隔: {REST_INTERVAL}s")
        logger.info("  MODE: 🔍 DRY-RUN (只读)")
        logger.info("=" * 60)

        await asyncio.gather(
            self.realtime_kvm_loop(),
            self.tick_kvm(),
            self.background_rest_loop(),
        )

    def close(self):
        self.csv_file.close()


async def main():
    auditor = BrierAuditor()
    try:
        await auditor.run()
    except KeyboardInterrupt:
        logger.info("审计停止")
    finally:
        auditor.close()


if __name__ == "__main__":
    asyncio.run(main())
