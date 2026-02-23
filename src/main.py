import argparse
import logging
import sys
import datetime
import pandas as pd
import yfinance as yf
import json
import os
import pytz
from pathlib import Path

from config import load_config, load_denylist
from providers.market_data import YFinanceProvider
from providers.pr_sources import RSSPRSource
from scoring.volume_spike import VolumeSpikeDetector
from scoring.filters import FilterEngine
from notify.slack import SlackNotifier

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EASTERN = pytz.timezone('US/Eastern')

def get_run_label() -> str:
    now_et = datetime.datetime.now(EASTERN)
    hour = now_et.hour
    if hour < 12:
        return "Morning"
    elif hour < 15:
        return "Midday"
    else:
        return "Closing"

def main():
    parser = argparse.ArgumentParser(description="PR Volume Spike Bot")
    parser.add_argument("--dry-run", action="store_true", help="Do not send Slack alerts")
    parser.add_argument("--run-label", type=str, default=None, help="Override run label (Morning/Midday/Closing)")
    args = parser.parse_args()

    run_label = args.run_label or get_run_label()
    logger.info(f"=== PR Volume Spike Bot — {run_label} Run ===")

    # Load Config
    try:
        cfg = load_config()
        denylist = load_denylist(cfg['exclusions']['tickers_denylist_path'])
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Init Components
    market_provider = YFinanceProvider()

    # ------------------------------------------------------------------ #
    # MARKET STATUS CHECK                                                  #
    # ------------------------------------------------------------------ #
    logger.info("Checking market status...")

    search_start_time = datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=48)
    trading_dates = set()

    try:
        spy = yf.download("SPY", period="10d", progress=False)
        if spy.empty:
            logger.warning("Could not fetch SPY data. Assuming market OPEN.")
        else:
            trading_dates = set(ts.date() for ts in spy.index)

            last_date = spy.index[-1].date()
            today_date = datetime.datetime.now(EASTERN).date()

            if last_date < today_date:
                is_weekday = today_date.weekday() < 5
                if run_label == "Morning" and is_weekday:
                    logger.info(f"Morning run: no data for today yet (last: {last_date}). Proceeding.")
                else:
                    logger.info(f"Market appears closed. Last data: {last_date}, Today: {today_date}. Exiting.")
                    if not args.dry_run:
                        sys.exit(0)
            else:
                logger.info(f"Market confirmed open. Last data: {last_date}")

            if len(spy) >= 2:
                last_trading_day = spy.index[-1].date()
                today_dt = datetime.datetime.now(EASTERN).date()
                if last_trading_day < today_dt:
                    # Market hasn't opened today yet (morning / weekend / holiday)
                    # Search from the most recent trading day's close
                    prev_day_date = last_trading_day
                else:
                    # Market is open today — search from the prior session's close
                    prev_day_date = spy.index[-2].date()
                prev_close_et = EASTERN.localize(
                    datetime.datetime.combine(prev_day_date, datetime.time(16, 0))
                )
                search_start_time = prev_close_et.astimezone(pytz.utc)
                logger.info(f"PR search window starts at: {search_start_time} (prev close: {prev_day_date})")
            else:
                logger.warning("Not enough SPY data for previous close. Using default 48h window.")

    except Exception as e:
        logger.error(f"Market status check failed: {e}. Proceeding with default window.", exc_info=True)

    # ------------------------------------------------------------------ #
    # BUILD COMPANY NAME → TICKER MAP (feeds PR-first resolution)         #
    # ------------------------------------------------------------------ #
    name_to_ticker = market_provider.build_name_to_ticker_map()

    # ------------------------------------------------------------------ #
    # INIT REMAINING COMPONENTS                                           #
    # ------------------------------------------------------------------ #
    pr_source = RSSPRSource()
    pr_source.set_name_to_ticker_map(name_to_ticker)

    detector = VolumeSpikeDetector(cfg)
    filter_engine = FilterEngine(cfg, denylist)
    notifier = SlackNotifier(
        os.environ.get(cfg['slack']['webhook_env_var']) if not args.dry_run else None
    )

    # ------------------------------------------------------------------ #
    # PR-FIRST PIPELINE                                                   #
    # Step 1: Ask RSS feeds "what companies issued PRs since last close?" #
    # Step 2: For each resolved ticker, check for a volume spike.         #
    # Step 3: Apply filters (market cap, sector, denylist).               #
    # ------------------------------------------------------------------ #
    logger.info("Fetching PR candidate tickers from wire services...")
    candidate_ticker_map = pr_source.get_all_candidate_tickers(
        window_start=search_start_time,
        config=cfg,
        trading_dates=trading_dates,
    )

    pr_tickers = list(candidate_ticker_map.keys())
    logger.info(f"Candidate tickers from PRs: {len(pr_tickers)}")

    stats = {
        "pr_candidates": len(pr_tickers),
        "scanned": 0,
        "spikes": 0,
        "cap_filtered": 0,
        "pharma_filtered": 0,
        "alerts": 0,
    }
    alerts_generated = []

    # Download price history for all PR candidates in one batch
    # Chunk to avoid yfinance limits
    chunk_size = 100
    for i in range(0, len(pr_tickers), chunk_size):
        chunk = pr_tickers[i:i + chunk_size]
        logger.info(f"Processing chunk {i}–{i+len(chunk)} of {len(pr_tickers)} PR candidates...")

        try:
            data = yf.download(chunk, period="3mo", group_by='ticker', threads=True, progress=False)
        except Exception as e:
            logger.error(f"Download failed for chunk: {e}")
            continue

        is_multi = isinstance(data.columns, pd.MultiIndex)

        for ticker in chunk:
            stats['scanned'] += 1

            if is_multi:
                try:
                    df = data[ticker].dropna()
                except KeyError:
                    continue
            else:
                df = data.dropna()
                if df.empty:
                    continue

            if df.empty or 'Volume' not in df.columns:
                continue

            # 1. VOLUME SPIKE CHECK
            res = detector.check_spike(df)
            if not res:
                continue

            stats['spikes'] += 1
            logger.info(f"Spike found: {ticker} ({res['multiple']}x, {res['pct_change']:+.1f}%)")

            # 2. DETAILS + FILTERS (single yfinance call for cap, sector, industry)
            details = market_provider.get_ticker_details(ticker)
            mcap    = details.get('market_cap')
            name    = details.get('name', ticker)
            sector  = details.get('sector')
            industry = details.get('industry')

            if not filter_engine.check_market_cap(mcap):
                stats['cap_filtered'] += 1
                logger.info(f"Filtered {ticker} by market cap: {mcap}")
                continue

            is_excl, reason = filter_engine.is_pharma_excluded(ticker, sector, industry, name)
            if is_excl:
                stats['pharma_filtered'] += 1
                logger.info(f"Filtered {ticker} by sector/pharma: {reason}")
                continue

            # 3. GRAB THE MATCHED PRs (already resolved — just format them)
            prs = pr_source.get_prs(ticker, search_start_time, cfg, trading_dates)
            if not prs:
                # Shouldn't happen (ticker came from candidate map) but guard anyway
                logger.info(f"No PR found for confirmed spike {ticker} (race condition?)")
                continue

            top_pr = prs[0]

            # Double-check PR headline doesn't contain pharma terms
            is_excl_pr, reason_pr = filter_engine.is_pharma_excluded(
                ticker, sector, industry, top_pr.headline
            )
            if is_excl_pr:
                stats['pharma_filtered'] += 1
                logger.info(f"Filtered {ticker} by PR headline: {reason_pr}")
                continue

            stats['alerts'] += 1
            alerts_generated.append({
                "ticker":       ticker,
                "company_name": name,
                "spike":        res,
                "pr":           top_pr,
                "market_cap":   mcap,
                "sector":       sector,
            })
            logger.info(f"ALERT: {ticker} — {top_pr.headline[:80]}")

    # ------------------------------------------------------------------ #
    # REPORT                                                               #
    # ------------------------------------------------------------------ #
    logger.info(f"{run_label} Run Complete. Stats: {stats}")

    if alerts_generated and not args.dry_run:
        notifier.post_final_report(alerts_generated, run_label=run_label)
    elif args.dry_run and alerts_generated:
        logger.info(f"Dry run ({run_label}): skipping Slack. Alerts:")
        for a in alerts_generated:
            logger.info(f"  {a['ticker']}: {a['pr'].headline}")
    else:
        logger.info(f"{run_label} run found 0 alerts.")
        # Send a daily heartbeat on the Closing run so the channel confirms the bot is live
        if run_label == "Closing" and not args.dry_run:
            notifier.post_status(stats, run_label=run_label)

    label_slug = run_label.lower()
    report_path = Path(f"daily_report_{label_slug}.json")
    with open(report_path, "w") as f:
        serializable = []
        for a in alerts_generated:
            item = a.copy()
            item['pr'] = a['pr']._asdict()
            item['pr']['published_at'] = str(item['pr']['published_at'])
            serializable.append(item)

        json.dump({"run_label": run_label, "stats": stats, "alerts": serializable}, f, indent=2)

if __name__ == "__main__":
    main()
