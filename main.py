"""
资金费率套利机器人 v5

三种模式:
  python main.py --monitor  --capital 10000    # 纯看：扫描打印，不记录
  python main.py --simulate --capital 10000    # 模拟：走完整流程，虚拟建仓，记录到 DB
  python main.py --capital 10000               # 实盘：真金白银

模拟模式特点:
  ✅ 每 5 分钟扫描真实费率
  ✅ 按策略规则决定开仓/平仓
  ✅ 用实时价格+模拟滑点计算成交价
  ✅ 虚拟仓位写入 SQLite
  ✅ 每 8 小时按真实费率结算收入
  ✅ 一周后用 results.py 查看完整损益报告
  ❌ 不发送任何真实订单
"""
import argparse
import asyncio
import sys
import yaml
import signal
from datetime import datetime, timezone
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from capital import resolve, print_plan, CapitalPlan
from screener import DynamicScreener, classify, _to_ccxt
from monitor import FundingRateMonitor
from position import PositionManager
from notifier import TelegramNotifier

logger.add(
    "logs/bot_{time:YYYY-MM-DD}.log",
    rotation="1 day", retention="30 days", level="INFO",
    format="{time:HH:mm:ss} | {level:<7} | {message}",
)


def parse_args():
    p = argparse.ArgumentParser(description="资金费率套利机器人")
    p.add_argument("--capital", type=float, default=None, help="初始资金 (USDT)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--monitor", action="store_true", help="纯监控（只看不记录）")
    g.add_argument("--simulate", action="store_true", help="模拟交易（虚拟建仓，记录到DB）")
    p.add_argument("--t3", action="store_true", help="启用 T3")
    p.add_argument("--no-t3", action="store_true", help="关闭 T3")
    p.add_argument("--yes", action="store_true", help="实盘模式跳过二次确认（用于 PM2/systemd）")
    p.add_argument("--config", type=str, default="config.yaml")
    return p.parse_args()


class FundingArbitrageBot:

    def __init__(self, config: dict, plan: CapitalPlan, mode: str = "live"):
        """mode: 'monitor' | 'simulate' | 'live'"""
        self.config = config
        self.plan = plan
        self.mode = mode

        self.screener = DynamicScreener(config, plan)
        # 共用 screener 的 ccxt 客户端：省 ~80MB markets metadata，rate limiter 全局生效
        self.monitor = FundingRateMonitor(config, exchange=self.screener.exchange)
        self.fees_cfg = config["fees"]
        self.risk = config["strategy"]["risk"]
        self.running = True

        # 模拟和实盘都写 DB，但用不同的数据库文件
        if mode == "simulate":
            self.positions = PositionManager(db_path="simulate.db")
            from sim_executor import SimulatedExecutor
            self.executor = SimulatedExecutor(config)
        elif mode == "live":
            self.positions = PositionManager(db_path="arbitrage.db")
            from executor import BinanceExecutor
            self.executor = BinanceExecutor(config)
        else:
            self.positions = None
            self.executor = None

        # Telegram: 实盘开，模拟可选，监控关
        tg_enabled = config.get("telegram", {}).get("enabled", False)
        if mode == "live" and tg_enabled:
            self.notifier = TelegramNotifier(config)
        elif mode == "simulate" and tg_enabled:
            self.notifier = TelegramNotifier(config)
        else:
            self.notifier = None

        # executor 共享 notifier（用于回滚/尾单失败的 TG 告警队列消费）
        if self.executor:
            self.executor._notifier_ref = self.notifier  # 仅做标记，实际推送走 _flush_executor_alerts

        # 账实对账器（仅实盘，simulate 无需）
        self.reconciler = None
        if mode == "live":
            from reconciler import Reconciler
            self.reconciler = Reconciler(self.executor, self.positions, self.notifier)

    # ==================================================================
    # 守卫方法
    # ==================================================================
    async def _flush_executor_alerts(self):
        """消费 executor._critical_errors 并推送 TG（rollback / 尾单 / 单腿平仓失败）"""
        if not self.executor or not self.notifier:
            if self.executor:
                self.executor._critical_errors.clear()
            return
        errors = list(self.executor._critical_errors)
        self.executor._critical_errors.clear()
        for err in errors:
            try:
                await self.notifier.on_error(err)
            except Exception:
                pass

    async def _mark_partial_close_risk(self, symbol: str) -> None:
        """
        部分平仓后立即标记账实风险，阻止继续开仓直到对账恢复。
        """
        msg = f"平仓未完成（部分成交）: {symbol}，已暂停开仓，等待对账恢复"
        logger.error(msg)
        if self.reconciler:
            self.reconciler.is_clean = False
        if self.notifier:
            try:
                await self.notifier.on_error(msg)
            except Exception:
                pass

    @staticmethod
    def _in_settlement_window() -> bool:
        """检查是否处于 8h 结算封锁窗口 (HH:59:30 – HH:00:30 UTC)"""
        now = datetime.now(timezone.utc)
        m, s = now.minute, now.second
        return (m == 59 and s >= 30) or (m == 0 and s <= 30)

    @staticmethod
    def _funding_settlement_key(now: datetime | None = None) -> str:
        """
        生成当前 8h 结算周期 key（UTC），用于资金费入账幂等去重。
        例如: 2026-05-02T08:00:00Z
        """
        now = now or datetime.now(timezone.utc)
        slot_hour = (now.hour // 8) * 8
        slot = now.replace(hour=slot_hour, minute=0, second=0, microsecond=0)
        return slot.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _passes_break_even(self, coin: dict, alloc: float) -> bool:
        """预期回本检查：min_holding_hours 内累计资金费 ≥ 双程手续费 × 1.5"""
        rate = abs(coin["rate"])
        min_hold = self.config["strategy"]["exit"].get("min_holding_hours", 24)
        expected_funding = alloc * rate * (min_hold / 8)
        two_way_fee = self._calc_fees(alloc) * 2
        return expected_funding >= two_way_fee * 1.5

    # ==================================================================
    # 动态换仓
    # ==================================================================
    async def _find_rotation_target(self, candidate: dict, open_positions: list[dict]):
        """
        返回可被 candidate 替换的最差持仓（同 tier 内 score 最低且持仓 ≥ min_age_hours）。
        没有合适目标返回 None。
        """
        cfg = self.config["strategy"].get("rotation", {})
        if not cfg.get("enabled", False):
            return None
        if self._in_settlement_window():
            return None

        multiplier = cfg.get("score_multiplier", 1.5)
        min_age = cfg.get("min_age_hours", 24)
        tier = candidate["tier"]

        same_tier = [p for p in open_positions if classify(_to_ccxt(p["symbol"])) == tier]
        if not same_tier:
            return None

        now = datetime.now(timezone.utc)
        worst = None
        worst_score = float("inf")

        for pos in same_tier:
            opened = datetime.fromisoformat(pos["opened_at"])
            age_h = (now - opened).total_seconds() / 3600
            if age_h < min_age:
                continue

            ccxt_sym = _to_ccxt(pos["symbol"])
            rate_data = await self.monitor.fetch_funding_rate(ccxt_sym)
            if not rate_data:
                continue
            cur_rate = rate_data["rate"]
            # 方向不一致或费率已转向 → 留给 task_check_and_close 处理
            if pos["direction"] == "positive" and cur_rate <= 0:
                continue
            if pos["direction"] == "reverse" and cur_rate >= 0:
                continue

            detail = await self.screener._evaluate(ccxt_sym)
            if not detail:
                continue

            cur_score = self.screener._score({
                "tier": tier,
                "abs_rate": abs(cur_rate),
                "annualized": abs(cur_rate) * 3 * 365,
                **detail,
            })
            if cur_score < worst_score:
                worst_score = cur_score
                worst = (pos, cur_score)

        if worst is None:
            return None

        if candidate["score"] >= worst_score * multiplier:
            return worst[0]
        return None

    async def _close_position(self, pos: dict, reason: str) -> bool:
        """
        平仓单个持仓（提取自 task_check_and_close 的平仓子流程，供换仓复用）。
        返回 True 表示平仓成功。
        """
        symbol = pos["symbol"]
        ccxt_sym = _to_ccxt(symbol)

        basis_data = await self.monitor.fetch_basis(ccxt_sym)
        close_basis = basis_data["basis_pct"] if basis_data else None
        if basis_data:
            current_price = basis_data["perp_price"]
        else:
            ticker = await self.monitor.get_ticker(ccxt_sym)
            current_price = float(ticker["last"]) if ticker and ticker.get("last") else None

        result = self.executor.close_arbitrage(
            symbol, pos["quantity"], pos["direction"],
            current_price=current_price,
            usdt_amount=pos["usdt_amount"],
        )
        if not result.get("success"):
            return False
        if result.get("partial"):
            await self._mark_partial_close_risk(symbol)
            return False

        close_spot = result.get("spot_avg_price", 0)
        close_futures = result.get("futures_avg_price", 0)
        if close_spot and close_futures:
            if pos["direction"] == "positive":
                close_pnl = (
                    (close_spot - pos["spot_price"])
                    + (pos["futures_price"] - close_futures)
                ) * pos["quantity"]
            else:
                close_pnl = (
                    (pos["spot_price"] - close_spot)
                    + (close_futures - pos["futures_price"])
                ) * pos["quantity"]
        else:
            close_pnl = 0

        fees = self._calc_fees(pos["usdt_amount"])
        rebate = (pos["fees_paid"] + fees) * self.fees_cfg["rebate_rate"]
        self.positions.record_close(pos["id"], close_pnl, fees, rebate, close_basis)

        close_spot_side = "SELL" if pos["direction"] == "positive" else "BUY"
        close_futures_side = "BUY" if pos["direction"] == "positive" else "SELL"
        spot_fee = pos["usdt_amount"] * self.fees_cfg["spot_taker"]
        futures_fee = pos["usdt_amount"] * self.fees_cfg["futures_taker"]
        self.positions.record_trade(
            pos["id"], "close", close_spot_side, "spot",
            symbol, pos["quantity"], close_spot, spot_fee, result.get("spot"),
        )
        self.positions.record_trade(
            pos["id"], "close", close_futures_side, "futures",
            symbol, pos["quantity"], close_futures, futures_fee, result.get("futures"),
        )

        tag = "[模拟]" if self.mode == "simulate" else "[实盘]"
        logger.info(f"{tag} 平仓 {symbol}: {reason} | 价格盈亏 ${close_pnl:+.4f}")

        if self.notifier:
            total_fees = pos["fees_paid"] + fees
            net_pnl = pos["funding_earned"] + close_pnl - total_fees + rebate
            try:
                await self.notifier.on_close(
                    symbol, pos["funding_earned"],
                    total_fees, rebate, net_pnl, reason,
                )
            except Exception:
                pass
        return True

    # ==================================================================
    # 监控模式
    # ==================================================================
    def _append_raw_csv(self, rows: list[dict]) -> None:
        """把全量原始快照追加到日 CSV, 用于阶段 1 分析费率分布。"""
        import csv, os
        if not rows:
            return
        os.makedirs("logs", exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        path = f"logs/monitor-candidates-{date_str}.csv"
        fieldnames = [
            "timestamp", "symbol", "tier", "funding_rate", "annualized",
            "mark_price", "index_price", "predicted_rate", "next_funding_time",
        ]
        is_new = not os.path.exists(path)
        with open(path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if is_new:
                writer.writeheader()
            writer.writerows(rows)

    async def task_monitor_scan(self):
        try:
            # 阶段 1 数据采集: 在 screen 之前先落一份全量快照（含 T3 且不过滤）
            try:
                raw = await self.screener.raw_snapshot()
                if raw:
                    self._append_raw_csv(raw)
                    logger.info(f"原始快照已写入 CSV ({len(raw)} 行)")
            except Exception as e:
                logger.warning(f"原始快照写入失败: {e}")

            qualified = await self.screener.screen()
            if not qualified:
                logger.info("未发现合格标的")
                return

            mock_pos = []
            used = 0

            logger.info(f"\n{'='*70}")
            logger.info(f" 模拟分配 (可用 ${self.plan.tradable:,.0f})")
            logger.info(f"{'='*70}")

            for coin in qualified:
                ok, alloc, _ = self.screener.check_allocation(coin, mock_pos)
                if not ok:
                    continue
                alloc = min(alloc, self.plan.tradable - used)
                if alloc < 200:
                    continue
                daily = alloc * abs(coin["rate"]) * 3
                logger.info(
                    f" [{coin['tier_name'][:5]}] {coin['binance_symbol']:>10} | "
                    f"费率 {coin['rate']:+.4%} | 年化 {coin['annualized']:>5.1%} | "
                    f"仓 ${alloc:>6,.0f} | 日收 ${daily:.2f}"
                )
                mock_pos.append({"symbol": coin["binance_symbol"], "usdt_amount": alloc})
                used += alloc

            logger.info(f"{'='*70}")

        except Exception as e:
            logger.exception(f"监控异常: {e}")
            # 监控模式无 notifier，无需处理

    # ==================================================================
    # 模拟 / 实盘：扫描→开仓
    # ==================================================================
    async def task_scan_and_open(self):
        try:
            # 结算窗口封锁
            if self._in_settlement_window():
                logger.debug("处于结算窗口，跳过本次开仓扫描")
                return

            qualified = await self.screener.screen()
            if not qualified:
                return

            open_pos = self.positions.get_open_positions()
            used = sum(p["usdt_amount"] for p in open_pos)
            available = self.plan.tradable - used

            # 至少要够开"最小单仓的一半"，防止 leftover 太碎；自动跟随 tradable 伸缩
            min_available = min(t.max_position for t in self.plan.tiers.values()) * 0.5
            if available < min_available:
                logger.info(f"可用不足 ${available:.0f} (需 >= ${min_available:.0f})")
                return

            # 日亏损上限检查
            daily_pnl = self.positions.get_daily_pnl()
            if daily_pnl < -self.plan.daily_loss_limit:
                logger.warning(
                    f"触发日亏损上限: 今日盈亏 ${daily_pnl:.2f} < "
                    f"-${self.plan.daily_loss_limit:.2f}，今日停止开新仓"
                )
                if self.notifier:
                    try:
                        await self.notifier.on_error(
                            f"触发日亏损上限 ${daily_pnl:.2f}，今日不再开新仓"
                        )
                    except Exception:
                        pass
                return

            # 账实一致性检查
            if self.reconciler and not self.reconciler.is_clean:
                logger.warning("账实不符（对账未通过），跳过开仓")
                return

            # BNB 余额检查 — 不足时硬停本次开仓
            if self.fees_cfg.get("use_bnb_discount", False):
                bnb_balance = self.executor.check_bnb_balance()
                bnb_min = self.fees_cfg.get("bnb_min_balance", 0.05)
                if bnb_balance < bnb_min:
                    logger.error(
                        f"BNB 余额不足 ({bnb_balance:.4f} < {bnb_min} BNB)，"
                        f"本次停止开仓（手续费无折扣将超过收益）"
                    )
                    if self.notifier:
                        try:
                            await self.notifier.on_error(
                                f"BNB 余额不足 {bnb_balance:.4f} BNB（建议 >{bnb_min}），已停止本次开仓"
                            )
                        except Exception:
                            pass
                    return

            order_priority = self.config["strategy"].get("order_priority", "concurrent")
            max_basis = self.config["strategy"]["entry"].get("max_basis_pct", 0.001)

            for coin in qualified:
                ok, alloc, reason = self.screener.check_allocation(coin, open_pos)

                # 槽位已满时尝试动态换仓
                if not ok and "已满" in reason:
                    target = await self._find_rotation_target(coin, open_pos)
                    if target:
                        rot_reason = f"主动换仓 → {coin['binance_symbol']}"
                        logger.info(
                            f"触发换仓: 平 {target['symbol']} 换入 {coin['binance_symbol']} "
                            f"(新 score {coin['score']:.1f})"
                        )
                        if not await self._close_position(target, rot_reason):
                            logger.warning(f"换仓平仓失败，跳过 {coin['binance_symbol']}")
                            continue
                        # 刷新仓位与可用资金
                        open_pos = self.positions.get_open_positions()
                        used = sum(p["usdt_amount"] for p in open_pos)
                        available = self.plan.tradable - used
                        ok, alloc, reason = self.screener.check_allocation(coin, open_pos)

                if not ok:
                    continue
                alloc = min(alloc, available)
                if alloc < 200:
                    continue

                # 预期回本检查：当前费率在最短持仓期内能否覆盖双程手续费
                if not self._passes_break_even(coin, alloc):
                    logger.info(
                        f"跳过 {coin['binance_symbol']}: 费率 {coin['rate']:.4%} "
                        f"不足以覆盖手续费（min_holding_hours 内预期收益 < 双程费 ×1.5）"
                    )
                    continue

                # 基差检查：基差过大时暂缓开仓，等待收敛
                ccxt_sym = _to_ccxt(coin["binance_symbol"])
                basis_data = await self.monitor.fetch_basis(ccxt_sym)
                if basis_data and abs(basis_data["basis_pct"]) > max_basis:
                    logger.info(
                        f"跳过 {coin['binance_symbol']}: 基差过大 "
                        f"{basis_data['basis_pct']:+.4%} > {max_basis:.4%}"
                    )
                    continue
                open_basis = basis_data["basis_pct"] if basis_data else None

                self.executor.set_leverage(coin["binance_symbol"], self.risk["max_leverage"])

                result = self.executor.open_arbitrage(
                    symbol=coin["binance_symbol"],
                    usdt_amount=alloc,
                    current_price=coin["mid_price"],
                    direction=coin["direction"],
                    order_priority=order_priority,
                )

                if result["success"]:
                    # 用实际成交金额（非预算 alloc），防止分批有批次失败时记账偏高
                    actual_usdt = result["quantity"] * result["spot_avg_price"]
                    fees = self._calc_fees(actual_usdt)
                    pos_id = self.positions.record_open(
                        symbol=coin["binance_symbol"],
                        direction=coin["direction"],
                        quantity=result["quantity"],
                        spot_price=result["spot_avg_price"],
                        futures_price=result["futures_avg_price"],
                        usdt_amount=actual_usdt,
                        slippage=result["slippage"],
                        fees_paid=fees,
                        open_basis=open_basis,
                    )

                    # 记录两条腿的成交明细
                    spot_side = "BUY" if coin["direction"] == "positive" else "SELL"
                    futures_side = "SELL" if coin["direction"] == "positive" else "BUY"
                    spot_fee = actual_usdt * self.fees_cfg["spot_taker"]
                    futures_fee = actual_usdt * self.fees_cfg["futures_taker"]
                    self.positions.record_trade(
                        pos_id, "open", spot_side, "spot",
                        coin["binance_symbol"], result["quantity"],
                        result["spot_avg_price"], spot_fee, result.get("spot"),
                    )
                    self.positions.record_trade(
                        pos_id, "open", futures_side, "futures",
                        coin["binance_symbol"], result["quantity"],
                        result["futures_avg_price"], futures_fee, result.get("futures"),
                    )

                    if self.notifier:
                        await self.notifier.on_open(
                            coin["binance_symbol"], coin["direction"],
                            actual_usdt, result["slippage"], coin["rate"],
                        )
                    open_pos = self.positions.get_open_positions()
                    available -= actual_usdt

                    tag = "[模拟]" if self.mode == "simulate" else "[实盘]"
                    logger.info(
                        f"{tag} 开仓 [{coin['tier_name']}] "
                        f"{coin['binance_symbol']} ${actual_usdt:.0f}"
                    )

        except Exception as e:
            logger.exception(f"扫描异常: {e}")
            if self.notifier:
                try:
                    await self.notifier.on_error(f"扫描开仓异常: {e}", exc=e)
                except Exception:
                    pass
        finally:
            await self._flush_executor_alerts()

    # ==================================================================
    # 模拟 / 实盘：持仓检查→平仓
    # ==================================================================
    async def task_check_and_close(self):
        try:
            # 结算窗口封锁
            if self._in_settlement_window():
                logger.debug("处于结算窗口，跳过本次持仓检查")
                return

            for pos in self.positions.get_open_positions():
                symbol = pos["symbol"]
                ccxt_sym = _to_ccxt(symbol)
                tier = classify(ccxt_sym)
                ticker = None  # 本次迭代的 ticker 缓存，避免重复请求

                should_close, reason = await self.monitor.should_exit(
                    ccxt_sym, pos["direction"], opened_at=pos["opened_at"]
                )

                if not should_close:
                    age = (datetime.now(timezone.utc) -
                           datetime.fromisoformat(pos["opened_at"])).days
                    if age >= self.config["strategy"]["exit"]["max_holding_days"]:
                        should_close, reason = True, f"持仓 {age} 天"

                if not should_close and tier == 3:
                    ticker = await self.monitor.get_ticker(ccxt_sym)
                    if ticker and float(ticker.get("quoteVolume", 0) or 0) < 10e6:
                        should_close, reason = True, "T3 流动性不足"

                if not should_close and tier == 2:
                    ticker = await self.monitor.get_ticker(ccxt_sym)
                    if ticker and float(ticker.get("quoteVolume", 0) or 0) < 50e6:
                        should_close, reason = True, "T2 流动性下降"

                if should_close:
                    # 获取基差（同时提供当前价格，减少一次 API 调用）
                    basis_data = await self.monitor.fetch_basis(ccxt_sym)
                    close_basis = basis_data["basis_pct"] if basis_data else None
                    if basis_data:
                        current_price = basis_data["perp_price"]
                    elif ticker and ticker.get("last"):
                        current_price = float(ticker["last"])
                    else:
                        if ticker is None:
                            ticker = await self.monitor.get_ticker(ccxt_sym)
                        current_price = float(ticker["last"]) if ticker and ticker.get("last") else None

                    result = self.executor.close_arbitrage(
                        symbol, pos["quantity"], pos["direction"],
                        current_price=current_price,
                        usdt_amount=pos["usdt_amount"],
                    )
                    if result["success"]:
                        if result.get("partial"):
                            await self._mark_partial_close_risk(symbol)
                            continue

                        # 计算价格盈亏（对冲组合中的基差变化）
                        close_spot = result.get("spot_avg_price", 0)
                        close_futures = result.get("futures_avg_price", 0)
                        if close_spot and close_futures:
                            if pos["direction"] == "positive":
                                # 现货多 + 合约空：现货涨赚，合约跌赚
                                close_pnl = (
                                    (close_spot - pos["spot_price"])
                                    + (pos["futures_price"] - close_futures)
                                ) * pos["quantity"]
                            else:
                                # 现货空 + 合约多：现货跌赚，合约涨赚
                                close_pnl = (
                                    (pos["spot_price"] - close_spot)
                                    + (close_futures - pos["futures_price"])
                                ) * pos["quantity"]
                        else:
                            close_pnl = 0

                        fees = self._calc_fees(pos["usdt_amount"])
                        rebate = (pos["fees_paid"] + fees) * self.fees_cfg["rebate_rate"]
                        self.positions.record_close(pos["id"], close_pnl, fees, rebate, close_basis)

                        # 记录平仓两条腿的成交明细
                        close_spot_side = "SELL" if pos["direction"] == "positive" else "BUY"
                        close_futures_side = "BUY" if pos["direction"] == "positive" else "SELL"
                        spot_fee = pos["usdt_amount"] * self.fees_cfg["spot_taker"]
                        futures_fee = pos["usdt_amount"] * self.fees_cfg["futures_taker"]
                        self.positions.record_trade(
                            pos["id"], "close", close_spot_side, "spot",
                            symbol, pos["quantity"], close_spot, spot_fee,
                            result.get("spot"),
                        )
                        self.positions.record_trade(
                            pos["id"], "close", close_futures_side, "futures",
                            symbol, pos["quantity"], close_futures, futures_fee,
                            result.get("futures"),
                        )

                        tag = "[模拟]" if self.mode == "simulate" else "[实盘]"
                        logger.info(
                            f"{tag} 平仓 {symbol}: {reason} | "
                            f"价格盈亏 ${close_pnl:+.4f}"
                        )

                        if self.notifier:
                            total_fees = pos["fees_paid"] + fees
                            net_pnl = pos["funding_earned"] + close_pnl - total_fees + rebate
                            try:
                                await self.notifier.on_close(
                                    symbol, pos["funding_earned"],
                                    total_fees, rebate, net_pnl, reason,
                                )
                            except Exception:
                                pass

        except Exception as e:
            logger.exception(f"持仓检查异常: {e}")
            if self.notifier:
                try:
                    await self.notifier.on_error(f"持仓检查异常: {e}", exc=e)
                except Exception:
                    pass
        finally:
            await self._flush_executor_alerts()

    # ==================================================================
    # 费率结算（模拟和实盘共用）
    # ==================================================================
    async def task_record_funding(self):
        try:
            settlement_key = self._funding_settlement_key()
            for pos in self.positions.get_open_positions():
                data = await self.monitor.fetch_funding_rate(_to_ccxt(pos["symbol"]))
                if not data:
                    continue
                rate = data["rate"]
                payment = pos["usdt_amount"] * (rate if pos["direction"] == "positive" else -rate)
                recorded = self.positions.record_funding(
                    pos["id"], rate, payment, settlement_key=settlement_key
                )
                if not recorded:
                    logger.warning(
                        f"跳过重复资金费记录: {pos['symbol']} | key={settlement_key}"
                    )
                    continue

                tag = "[模拟]" if self.mode == "simulate" else "[实盘]"
                logger.info(
                    f"{tag} 费率到账 {pos['symbol']} | "
                    f"{rate:+.4%} | ${payment:+.4f}"
                )

                if self.notifier:
                    try:
                        await self.notifier.on_funding(
                            pos["symbol"], rate, payment,
                            pos["funding_earned"] + payment,
                        )
                    except Exception:
                        pass

        except Exception as e:
            logger.exception(f"费率记录异常: {e}")
            if self.notifier:
                try:
                    await self.notifier.on_error(f"费率记录异常: {e}", exc=e)
                except Exception:
                    pass

    # ==================================================================
    async def task_daily_report(self):
        try:
            summary = self.positions.get_summary()
            tag = "模拟" if self.mode == "simulate" else "实盘"
            logger.info(
                f"[{tag}日报] "
                f"仓位:{summary.get('open_trades',0)} | "
                f"费率收入:${summary.get('total_funding',0):+.2f} | "
                f"手续费:${summary.get('total_fees',0):.2f} | "
                f"返佣:${summary.get('total_rebate',0):.2f} | "
                f"净盈亏:${summary.get('total_net_pnl',0):+.2f}"
            )
            if self.notifier:
                await self.notifier.on_daily_report(summary)
        except Exception as e:
            logger.exception(f"日报异常: {e}")
            if self.notifier:
                try:
                    await self.notifier.on_error(f"日报异常: {e}", exc=e)
                except Exception:
                    pass

    def _calc_fees(self, amount):
        """计算手续费，支持 BNB 抵扣折扣"""
        if self.fees_cfg.get("use_bnb_discount", False):
            spot_rate = self.fees_cfg.get("spot_taker_bnb", self.fees_cfg["spot_taker"])
            futures_rate = self.fees_cfg.get("futures_taker_bnb", self.fees_cfg["futures_taker"])
        else:
            spot_rate = self.fees_cfg["spot_taker"]
            futures_rate = self.fees_cfg["futures_taker"]
        return amount * (spot_rate + futures_rate)

    # ==================================================================
    async def _task_reconcile(self):
        try:
            await self.reconciler.check()
        except Exception as e:
            logger.exception(f"对账任务异常: {e}")

    async def run(self):
        sched = AsyncIOScheduler()
        m = self.config["schedule"]["check_interval_minutes"]

        job_defaults = {"max_instances": 1, "coalesce": True}

        if self.mode == "monitor":
            sched.add_job(self.task_monitor_scan, "interval", minutes=m, id="monitor", **job_defaults)
        else:
            sched.add_job(self.task_scan_and_open, "interval", minutes=m, id="scan", **job_defaults)
            sched.add_job(self.task_check_and_close, "interval", minutes=m * 2, id="check", **job_defaults)
            sched.add_job(self.task_record_funding, "cron", hour="0,8,16", minute=5, id="funding", **job_defaults)
            sched.add_job(self.task_daily_report, "cron", hour=0, minute=30, id="report", **job_defaults)
            if self.reconciler:
                sched.add_job(self._task_reconcile, "interval", hours=1, id="reconcile", **job_defaults)

        sched.start()

        mode_labels = {
            "monitor": "📡 纯监控",
            "simulate": "🧪 模拟交易（不下真单，记录到 simulate.db）",
            "live": "💰 实盘交易",
        }
        tier_str = "T1+T2+T3" if self.plan.t3_enabled else "T1+T2"

        print_plan(self.plan)
        logger.info(f"  运行模式:   {mode_labels[self.mode]}")
        logger.info(f"  等级:       {tier_str}")
        logger.info(f"  扫描间隔:   {m} 分钟")
        if self.mode == "simulate":
            logger.info(f"  数据文件:   simulate.db")
            logger.info(f"  查看结果:   python results.py")
        logger.info("=" * 55)

        if self.notifier:
            await self.notifier.on_start(self.plan.initial, [f"{self.mode} | {tier_str}"])

        # 启动后立即执行一次
        if self.mode == "monitor":
            await self.task_monitor_scan()
        else:
            if self.reconciler:
                await self._task_reconcile()  # 先对账，再开仓
            await self.task_scan_and_open()

        try:
            while self.running:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            sched.shutdown()
            await self.screener.close()
            await self.monitor.close()
            if self.positions:
                self.positions.close_db()
            if self.notifier:
                await self.notifier.on_stop()
            logger.info("已安全退出")


def main():
    import os
    os.makedirs("logs", exist_ok=True)

    args = parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)

    if args.t3:
        config["tiers"]["t3_enabled"] = True
    elif args.no_t3:
        config["tiers"]["t3_enabled"] = False

    plan = resolve(config, capital_override=args.capital)

    if args.monitor:
        mode = "monitor"
    elif args.simulate:
        mode = "simulate"
    else:
        mode = "live"

    if mode == "live":
        print_plan(plan)
        if args.yes:
            logger.info("实盘模式（--yes 已确认，跳过二次确认）")
        elif sys.stdin.isatty():
            confirm = input("\n  ⚠️  实盘模式，确认? [Y/n] ").strip().lower()
            if confirm and confirm != "y":
                print("  已取消")
                return
        else:
            # systemd / nohup 等无 TTY 环境：deploy.sh 的 YES-LIVE 已经把过关
            logger.info("实盘模式（非交互环境，跳过二次确认）")

    bot = FundingArbitrageBot(config, plan, mode=mode)
    loop = asyncio.new_event_loop()

    def shutdown(sig, frame):
        bot.running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    loop.run_until_complete(bot.run())


if __name__ == "__main__":
    main()
