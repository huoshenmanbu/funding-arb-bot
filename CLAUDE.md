# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A funding rate arbitrage bot for Binance. It earns yield by simultaneously holding a spot position and an opposite perpetual futures position, collecting the funding rate difference every 8 hours. Written in Chinese (comments, logs, UI).

## Commands

```bash
# Preflight checks
python preflight.py --monitor   # Monitor-mode checks only
python preflight.py             # Full checks (including trade permissions)

# Run modes
python main.py --monitor --capital 10000    # Read-only: scan and print, no DB
python main.py --simulate --capital 10000   # Paper trading: virtual positions in simulate.db
python main.py --capital 10000              # Live trading: real orders, arbitrage.db

# Flags
--t3 / --no-t3       # Enable/disable T3 (small-cap) tier
--config path.yaml    # Custom config file

# View results
python results.py                     # Simulation report (simulate.db)
python results.py --db arbitrage.db   # Live report
python results.py --export            # Export CSV

# Capital allocation preview
python capital.py

# Deploy to Linux server (systemd)
bash deploy.sh
```

## Architecture

**Three operating modes** controlled by `main.py` CLI flags:
- **monitor**: Read-only scanning via ccxt, no database, no orders
- **simulate**: Full pipeline with `SimulatedExecutor` writing to `simulate.db`
- **live**: Real orders via Binance SDK writing to `arbitrage.db`

**Dual SDK design** (critical for rebate preservation):
- `ccxt` is used **only for reading** market data (funding rates, order books, tickers) in `monitor.py` and `screener.py`. ccxt injects a brokerId that would affect rebate attribution.
- `binance-connector` / `binance-futures-connector` (official Binance SDKs) are used **only for order execution** in `executor.py`. This preserves the user's 30% fee rebate.

**Data flow per scan cycle:**
1. `DynamicScreener.screen()` — loads markets via ccxt, fetches all funding rates in batch, filters by tier thresholds (volume, OI, spread, depth), scores and ranks candidates
2. `FundingArbitrageBot.task_scan_and_open()` — checks allocation limits per tier, calls executor
3. `BinanceExecutor.open_arbitrage()` — concurrent spot + futures market orders via ThreadPoolExecutor, with atomic rollback if one leg fails
4. `PositionManager` — records to SQLite

**Tier system** (coin classification in `screener.py`):
- T1 (core): BTC, ETH — highest allocation, lowest rate threshold
- T2 (major): SOL, BNB, XRP, etc. (20 coins) — medium allocation
- T3 (opportunity): everything else — smallest allocation, requires `--t3` flag, has listing-age and total-cap constraints

**Capital allocation** (`capital.py`): All amounts are derived from `initial_capital` and percentage config in `config.yaml`. The `CapitalPlan` dataclass flows through the entire system. Percentages: 80% tradable, 15% on-chain reserve (Aave), 5% emergency.

**Scheduling** (`APScheduler`):
- Scan + open: every N minutes (default 5)
- Position check + close: every 2N minutes
- Funding settlement recording: cron at 00:05, 08:05, 16:05 UTC (matching Binance 8h cycles)
- Daily report: cron at 00:30 UTC

**Exit conditions** (checked in `task_check_and_close`):
- Funding rate reverses direction
- Rate drops below `min_profitable_rate`
- Position held longer than `max_holding_days`
- T2/T3 liquidity drops below threshold

## Key Design Decisions

- `executor.py` uses `concurrent.futures.ThreadPoolExecutor` to fire spot and futures legs simultaneously, with rollback logic if one side fails. Both `open_arbitrage` and `close_arbitrage` follow this pattern.
- `SimulatedExecutor` has an identical interface to `BinanceExecutor` — swap is done by import in `main.py.__init__` based on mode.
- Symbol format conversion: Binance native format is `BTCUSDT`, ccxt format is `BTC/USDT:USDT`. The helper `_to_ccxt()` in `screener.py` converts between them.
- SQLite databases are mode-separated: `simulate.db` vs `arbitrage.db`.
- Config uses proportion-driven design: only `initial_capital` needs to be set, everything else derives from percentages in `config.yaml`.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **funding-arb-bot** (819 symbols, 1339 relationships, 33 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/funding-arb-bot/context` | Codebase overview, check index freshness |
| `gitnexus://repo/funding-arb-bot/clusters` | All functional areas |
| `gitnexus://repo/funding-arb-bot/processes` | All execution flows |
| `gitnexus://repo/funding-arb-bot/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
