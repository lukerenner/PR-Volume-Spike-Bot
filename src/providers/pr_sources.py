import feedparser
import datetime
import pytz
from abc import ABC, abstractmethod
from typing import List, NamedTuple, Set
import logging
import time

logger = logging.getLogger(__name__)
EASTERN = pytz.timezone('US/Eastern')

class PRItem(NamedTuple):
    ticker: str
    headline: str
    url: str
    source: str
    published_at: datetime.datetime

class PRSource(ABC):
    # Updated signature to accept valid_trading_dates
    @abstractmethod
    def get_prs(self, ticker: str, window_start: datetime.datetime, config: dict, trading_dates: Set[datetime.date] = None) -> List[PRItem]:
        pass

class RSSPRSource(PRSource):
    def get_prs(self, ticker: str, window_start: datetime.datetime, config: dict, trading_dates: Set[datetime.date] = None) -> List[PRItem]:
        safe_ticker = ticker.replace('.', '-')
        rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={safe_ticker}"
        
        pr_cfg = config.get('pr_config', {})
        allowed_sources = [s.lower() for s in pr_cfg.get('allowed_sources', [])]
        required_keywords = [k.lower() for k in pr_cfg.get('required_keywords', [])]
        
        # Exclusion Window
        excl_start_str = pr_cfg.get('exclude_time_start_et', "09:30")
        excl_end_str = pr_cfg.get('exclude_time_end_et', "16:00")
        try:
            h1, m1 = map(int, excl_start_str.split(':'))
            h2, m2 = map(int, excl_end_str.split(':'))
            t_start = datetime.time(h1, m1)
            t_end = datetime.time(h2, m2)
        except:
            t_start, t_end = datetime.time(9, 30), datetime.time(16, 0)

        try:
            feed = feedparser.parse(rss_url)
            results = []
            
            for entry in feed.entries:
                if not hasattr(entry, 'published_parsed'):
                    continue
                
                full_text = (entry.title + " " + entry.get('summary', "")).lower()
                
                # 1. Source Check
                if allowed_sources and not any(s in full_text for s in allowed_sources):
                    continue

                # 2. Keyword Check
                if required_keywords and not any(k in full_text for k in required_keywords):
                    continue
                
                # 3. Time Check
                dt_utc = datetime.datetime.fromtimestamp(time.mktime(entry.published_parsed), pytz.utc)
                dt_et = dt_utc.astimezone(EASTERN)
                pr_date = dt_et.date()
                
                # Dynamic Window Check
                if dt_utc <= window_start.replace(tzinfo=pytz.utc):
                    continue

                # MARKET HOURS EXCLUSION LOGIC
                # Only excluding if:
                # A) It's a Weekday
                # B) AND it is a known Trading Date (if we have that info)
                #    If we don't have trading_dates info (None), fallback to excluding M-F.
                #    If we DO have trading_dates logic:
                #       If pr_date NOT in trading_dates -> It was a Holiday/Weekend -> Allow it.
                #       If pr_date IN trading_dates -> Apply 9:30-16:00 exclusion.

                should_check_time = True
                if trading_dates is not None:
                     # Since trading_dates comes from SPY history, it contains only days the market was OPEN.
                     # If PR is on a Monday that isn't in trading_dates, it's a holiday.
                     if pr_date not in trading_dates:
                         should_check_time = False
                else:
                     # Fallback 
                     if dt_et.weekday() >= 5: # Sat/Sun
                         should_check_time = False

                if should_check_time:
                    t = dt_et.time()
                    if t_start <= t <= t_end:
                        continue

                found_source = "Yahoo"
                for s in allowed_sources: 
                    if s in full_text: 
                        found_source = s.title() 
                        break

                results.append(PRItem(
                    ticker=ticker,
                    headline=entry.title,
                    url=entry.link,
                    source=found_source,
                    published_at=dt_utc
                ))
            return results 
        except Exception as e:
            logger.error(f"RSS error {ticker}: {e}")
            return []
