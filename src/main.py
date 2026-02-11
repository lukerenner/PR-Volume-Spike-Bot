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
from providers.sectors import SectorProvider
from scoring.volume_spike import VolumeSpikeDetector
from scoring.filters import FilterEngine
from notify.slack import SlackNotifier

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EASTERN = pytz.timezone('US/Eastern')

def get_run_label() -> str:
    """Auto-detect which run this is based on current Eastern time."""
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

    # MARKET STATUS CHECK
    logger.info("Checking market status...")

    # Defaults
    search_start_time = datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=48)
    trading_dates = set()

    try:
        spy = yf.download("SPY", period="10d", progress=False)
        if spy.empty:
            logger.warning("Could not fetch SPY data. Assuming market OPEN.")
        else:
            trading_dates = set(ts.date() for ts in spy.index)

            last_date_ts = spy.index[-1]
            last_date = last_date_ts.date()
            today_now = datetime.datetime.now(EASTERN)
            today_date = today_now.date()

            # For Morning/Midday runs, market is still open. We check if today
            # is a trading day by confirming SPY has data for today OR that
            # the last trading day was recent enough (yesterday for weekdays).
            if last_date < today_date:
                # Market may not have opened yet (very early run) or it's a holiday.
                # If this is a Morning run and today is a weekday, give it the benefit
                # of the doubt — yfinance may not have intraday data yet.
                is_weekday = today_date.weekday() < 5
                if run_label == "Morning" and is_weekday:
                    logger.info(f"Morning run: no data for today yet (last: {last_date}). "
                                f"Proceeding — market may have just opened.")
                else:
                    logger.info(f"Market appears closed. Last data: {last_date}, Today: {today_date}. Exiting.")
                    if not args.dry_run:
                        sys.exit(0)
            else:
                logger.info(f"Market confirmed open. Last data: {last_date}")

            # Calculate "Previous Close" for PR search window
            if len(spy) >= 2:
                prev_day_ts = spy.index[-2]
                prev_day_date = prev_day_ts.date()
                et_tz = EASTERN
                prev_close_et = et_tz.localize(datetime.datetime.combine(prev_day_date, datetime.time(16, 0)))
                search_start_time = prev_close_et.astimezone(pytz.utc)
                logger.info(f"PR search window starts at: {search_start_time} (UTC)")
            else:
                 logger.warning("Not enough SPY data to determine previous close. Using default 48h.")

    except Exception as e:
        logger.error(f"Market status check failed: {e}. Proceeding with default window.", exc_info=True)

    pr_source = RSSPRSource()
    sector_provider = SectorProvider()
    detector = VolumeSpikeDetector(cfg)
    filter_engine = FilterEngine(cfg, denylist)
    notifier = SlackNotifier(os.environ.get(cfg['slack']['webhook_env_var']) if not args.dry_run else None)

    # Fetch Universe
    universe_mode = cfg['universe']['mode']
    logger.info(f"Fetching universe for mode: {universe_mode}")
    tickers = market_provider.get_universe(universe_mode, cfg['universe'].get('watchlist'))
    logger.info(f"Universe size: {len(tickers)}")


    stats = {
        "scanned": 0,
        "spikes": 0,
        "cap_filtered": 0,
        "pharma_filtered": 0,
        "alerts": 0
    }
    alerts_generated = []

    # Chunking
    chunk_size = 100 
    
    # Process using search_start_time from above
    window_start = search_start_time

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        logger.info(f"Processing {i}/{len(tickers)}...")

        try:
            data = yf.download(chunk, period="3mo", group_by='ticker', threads=True, progress=False)
        except Exception as e:
            logger.error(f"Download failed: {e}")
            continue

        is_multi = len(chunk) > 1 and isinstance(data.columns, pd.MultiIndex)
        
        for ticker in chunk:
            stats['scanned'] += 1
            
            if is_multi:
                try:
                    df = data[ticker].dropna()
                except KeyError:
                    continue
            else:
                 df = data.dropna()
                 if df.empty: continue

            if df.empty:
                continue

            # Robust column check
            if 'Volume' not in df.columns:
                continue

            # 1. SPIKE CHECK
            res = detector.check_spike(df)
            if not res:
                continue
                
            stats['spikes'] += 1
            logger.info(f"Spike found: {ticker} ({res['multiple']}x)")
            
            # 2. MARKET CAP CHECK 
            details = market_provider.get_ticker_details(ticker)
            mcap = details.get('market_cap')
            name = details.get('name')
            
            if not filter_engine.check_market_cap(mcap):
                stats['cap_filtered'] += 1
                logger.info(f"Filtered {ticker} by Market Cap: {mcap}")
                continue
                
            # 3. PHARMA/SECTOR CHECK
            sector, industry = sector_provider.get_sector_industry(ticker)
            is_excl, reason = filter_engine.is_pharma_excluded(ticker, sector, industry, name) # Name check too?
            if is_excl:
                stats['pharma_filtered'] += 1
                logger.info(f"Filtered {ticker} by Pharma: {reason}")
                continue
                
            # 4. PR CHECK (Strict)
            prs = pr_source.get_prs(ticker, window_start, cfg, trading_dates)
            
            if prs:
                top_pr = prs[0]
                # Double check PR text exclusion
                is_excl_pr, reason_pr = filter_engine.is_pharma_excluded(ticker, sector, industry, top_pr.headline)
                if is_excl_pr:
                    stats['pharma_filtered'] += 1
                    logger.info(f"Filtered {ticker} by PR Text: {reason_pr}")
                    continue

                # Add to report
                stats['alerts'] += 1
                
                alerts_generated.append({
                    "ticker": ticker, 
                    "company_name": name,
                    "spike": res, 
                    "pr": top_pr,
                    "market_cap": mcap
                })
                logger.info(f"Found Alert candidate: {ticker}")
            else:
                logger.info(f"No matching PR for {ticker}")

    # END OF LOOP
    logger.info(f"{run_label} Run Complete. Stats: {stats}")

    # Send Consolidated Slack
    if alerts_generated and not args.dry_run:
        notifier.post_final_report(alerts_generated, run_label=run_label)
    elif args.dry_run and alerts_generated:
        logger.info(f"Dry run ({run_label}): Skipping Slack post. Candidates found:")
        for a in alerts_generated:
            logger.info(f" - {a['ticker']}: {a['pr'].headline}")
    elif not alerts_generated:
        logger.info(f"{run_label} run found 0 alerts.")

    # Save reports (include run label in filename to avoid overwriting between runs)
    label_slug = run_label.lower()
    report_path = Path(f"daily_report_{label_slug}.json")
    with open(report_path, "w") as f:
        serializable_alerts = []
        for a in alerts_generated:
            item = a.copy()
            item['pr'] = a['pr']._asdict()
            item['pr']['published_at'] = str(item['pr']['published_at'])
            serializable_alerts.append(item)

        json.dump({
            "run_label": run_label,
            "stats": stats,
            "alerts": serializable_alerts
        }, f, indent=2)

if __name__ == "__main__":
    main()
