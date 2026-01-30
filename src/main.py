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
    chunk_size = 100 # Can handle slightly larger chunks for price data
    
    # Process
    window_start = datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=cfg.get('pr_config', {}).get('window_hours', 24))

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        logger.info(f"Processing {i}/{len(tickers)}...")

        try:
            # Download bulk price data
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
                 # Verify it's actually for this ticker (safe for 1-item chunk)
                 if df.empty: continue

            if df.empty:
                continue

            # Robust column check
            # Handle MultiIndex column case where levels might be swapped or named differently
            if 'Volume' not in df.columns:
                # Try lowercase or common issues?
                # For now just skip and log
                # logger.warning(f"No Volume column for {ticker}, columns: {df.columns}")
                continue

            # 1. SPIKE CHECK
            res = detector.check_spike(df)
            if not res:
                # No spike, skip
                continue
                
            stats['spikes'] += 1
            logger.info(f"Spike found: {ticker} ({res['multiple']}x)")
            
            # 2. MARKET CAP CHECK (Expensive API Call, do only on spills)
            # Fetch info for this single ticker
            # Optimize: Maybe we could use yahooquery for batch info if vol is high?
            # For now, simplistic
            mcap = market_provider.get_market_cap(ticker)
            if not filter_engine.check_market_cap(mcap):
                stats['cap_filtered'] += 1
                logger.info(f"Filtered {ticker} by Market Cap: {mcap}")
                continue
                
            # 3. PHARMA/SECTOR CHECK
            sector, industry = sector_provider.get_sector_industry(ticker)
            is_excl, reason = filter_engine.is_pharma_excluded(ticker, sector, industry)
            if is_excl:
                stats['pharma_filtered'] += 1
                logger.info(f"Filtered {ticker} by Pharma: {reason}")
                continue
                
            # 4. PR CHECK (Strict)
            prs = pr_source.get_prs(ticker, window_start, cfg)
            
            if prs:
                top_pr = prs[0]
                # Double check PR text exclusion
                is_excl_pr, reason_pr = filter_engine.is_pharma_excluded(ticker, sector, industry, top_pr.headline)
                if is_excl_pr:
                    stats['pharma_filtered'] += 1
                    continue

                # ALERT
                company = f"{ticker} (Cap: ${mcap:,.0f})" if mcap else ticker
                links = {
                    'quote': f"https://finance.yahoo.com/quote/{ticker}",
                    'chart': f"https://finance.yahoo.com/chart/{ticker}"
                }
                
                if not args.dry_run:
                    notifier.post_alert(ticker, company, res, top_pr, links)
                
                stats['alerts'] += 1
                alerts_generated.append({"ticker": ticker, "spike": res, "pr": top_pr._asdict()})
                logger.info(f"ALERT SENT: {ticker}")
            else:
                logger.info(f"No matching PR for {ticker}")

    # Summary
    logger.info(f"Run Complete. Stats: {stats}")
    # Write reports...
    report_path = Path("daily_report.json")
    with open(report_path, "w") as f:
        # Convert datetime in pr to str
        for a in alerts_generated:
            a['pr']['published_at'] = str(a['pr']['published_at'])
        json.dump({"stats": stats, "alerts": alerts_generated}, f, indent=2)

if __name__ == "__main__":
    main()
