import argparse
import logging
import sys
import datetime
import pandas as pd
import yfinance as yf
import json
import os
from pathlib import Path

from config import load_config, load_denylist
from providers.market_data import YFinanceProvider
from providers.pr_sources import RSSPRSource, GoogleNewsFallback
from providers.sectors import SectorProvider
from scoring.volume_spike import VolumeSpikeDetector
from scoring.filters import PharmaFilter
from notify.slack import SlackNotifier

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="PR Volume Spike Bot")
    parser.add_argument("--dry-run", action="store_true", help="Do not send Slack alerts")
    args = parser.parse_args()

    # Load Config
    try:
        cfg = load_config()
        denylist = load_denylist(cfg['exclusions']['tickers_denylist_path'])
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Init Components
    market_provider = YFinanceProvider()
    pr_source = RSSPRSource()
    pr_fallback = GoogleNewsFallback()
    sector_provider = SectorProvider()
    detector = VolumeSpikeDetector(cfg)
    pharma_filter = PharmaFilter(cfg, denylist)
    notifier = SlackNotifier(os.environ.get(cfg['slack']['webhook_env_var']) if not args.dry_run else None)

    # Fetch Universe
    universe_mode = cfg['universe']['mode']
    logger.info(f"Fetching universe for mode: {universe_mode}")
    tickers = market_provider.get_universe(universe_mode, cfg['universe'].get('watchlist'))
    logger.info(f"Universe size: {len(tickers)}")

    # Stats
    stats = {
        "scanned": 0,
        "spikes": 0,
        "excluded": 0,
        "alerts": 0
    }
    
    alerts_generated = []

    # Optimization: Use yf.download for bulk if tickers > 10
    # This avoids serial individual requests
    params_period = "3mo"
    
    # Process in chunks of 50 to avoid massive downloads failure
    chunk_size = 50
    # Setup window for PR search
    pr_window_hours = cfg.get('pr_window_hours', 24)
    # Use timezone-aware UTC for calculation, or just subtract hours from now
    # We will pass a localized start time if possible, or naive
    window_start = datetime.datetime.now() - datetime.timedelta(hours=pr_window_hours)

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        logger.info(f"Processing chunk {i} to {i+len(chunk)}...")
        
        try:
            # yf.download is verbose, suppress it?
            # group_by='ticker' makes Ticker the top level column
            data = yf.download(chunk, period=params_period, group_by='ticker', threads=True, progress=False)
        except Exception as e:
            logger.error(f"Failed to download chunk {i}: {e}")
            continue

        # If only 1 ticker, data levels are different (no top level ticker)
        is_multi = len(chunk) > 1 and isinstance(data.columns, pd.MultiIndex)
        
        for ticker in chunk:
            stats['scanned'] += 1
            
            # Extract DF for this ticker
            if is_multi:
                try:
                    df = data[ticker].dropna()
                except KeyError:
                    logger.warning(f"No data for {ticker}")
                    continue
            else:
                # Single ticker case or flat DF
                # If only 1 ticker in list, yf returns single-level DF
                df = data.dropna()
                # If we are failing here logic might be off for 1-item chunk list passed to loop
                # Robustness: Check columns
                if 'Close' not in df.columns:
                     logger.warning(f"Invalid columns for {ticker}")
                     continue

            if df.empty:
                continue

            # Check Spike
            res = detector.check_spike(df)
            if res:
                stats['spikes'] += 1
                logger.info(f"Spike detected for {ticker}: {res['multiple']}x")

                # Check Exclusion (Pharma)
                # Fetch Sector info
                sector, industry = sector_provider.get_sector_industry(ticker)
                
                # We don't have PR text yet, so pass empty
                is_excluded, reason = pharma_filter.is_excluded(ticker, sector, industry)
                if is_excluded:
                    logger.info(f"Excluded {ticker}: {reason}")
                    stats['excluded'] += 1
                    continue

                # Fetch PRs
                prs = pr_source.get_prs(ticker, window_start)
                if not prs:
                    # Try fallback
                    prs = pr_fallback.get_prs(ticker, window_start)
                
                if prs:
                    # Pick best PR (most recent)
                    top_pr = prs[0] # List is assumed sorted or we sort
                    # Double check exclusion with PR headline
                    is_excluded_pr, reason_pr = pharma_filter.is_excluded(ticker, sector, industry, top_pr.headline)
                    if is_excluded_pr:
                         logger.info(f"Excluded {ticker} by PR text: {reason_pr}")
                         stats['excluded'] += 1
                         continue

                    # Success - Alert!
                    company = ticker # We don't have easy company name unless we stored it from sector info
                    # yfinance info has shortName, but we only called it in sector provider.
                    # Optimization opportunity: modify sector provider to return name too.
                    # For now pipeline is cleaner to just alert.
                    
                    link_quote = f"https://finance.yahoo.com/quote/{ticker}"
                    link_chart = f"https://finance.yahoo.com/chart/{ticker}"
                    
                    logger.info(f"Posting alert for {ticker}")
                    if not args.dry_run:
                        notifier.post_alert(ticker, company, res, top_pr, {'quote': link_quote, 'chart': link_chart})
                    
                    stats['alerts'] += 1
                    
                    alerts_generated.append({
                        "ticker": ticker,
                        "spike": res,
                        "pr": {
                            "headline": top_pr.headline,
                            "url": top_pr.url,
                            "source": top_pr.source,
                            "date": str(top_pr.published_at)
                        }
                    })
                else:
                    logger.info(f"No recent PR found for {ticker}")

    # Summary
    logger.info(f"Run Complete. Stats: {stats}")
    if not args.dry_run:
        notifier.post_summary(stats)

    # Save artifacts
    report_path = Path("daily_report.json")
    with open(report_path, "w") as f:
        json.dump({"stats": stats, "alerts": alerts_generated}, f, indent=2)
        
    md_path = Path("daily_report.md")
    with open(md_path, "w") as f:
        f.write("# Daily Spike Report\n\n")
        f.write(f"**Date:** {datetime.date.today()}\n")
        f.write(f"**Stats:** {stats}\n\n")
        if alerts_generated:
            f.write("## Alerts\n")
            for a in alerts_generated:
                f.write(f"- **{a['ticker']}**: {a['spike']['multiple']}x Vol, {a['spike']['pct_change']}% Price\n")
                f.write(f"  - [{a['pr']['headline']}]({a['pr']['url']})\n")

if __name__ == "__main__":
    main()
