import feedparser
import datetime
import pytz
from abc import ABC, abstractmethod
from typing import List, NamedTuple
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
    # We ignore config 'window_hours' if window_start is passed specifically
    @abstractmethod
    def get_prs(self, ticker: str, window_start: datetime.datetime, config: dict) -> List[PRItem]:
        pass

class RSSPRSource(PRSource):
    def get_prs(self, ticker: str, window_start: datetime.datetime, config: dict) -> List[PRItem]:
        safe_ticker = ticker.replace('.', '-')
        rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={safe_ticker}"
        
        pr_cfg = config.get('pr_config', {})
        allowed_sources = [s.lower() for s in pr_cfg.get('allowed_sources', [])]
        required_keywords = [k.lower() for k in pr_cfg.get('required_keywords', [])]
        
        # Exclusion Window (defaults 9:30 - 16:00)
        excl_start_str = pr_cfg.get('exclude_time_start_et', "09:30")
        excl_end_str = pr_cfg.get('exclude_time_end_et', "16:00")
        
        try:
            h1, m1 = map(int, excl_start_str.split(':'))
            h2, m2 = map(int, excl_end_str.split(':'))
            t_start = datetime.time(h1, m1)
            t_end = datetime.time(h2, m2)
        except:
            t_start = datetime.time(9, 30)
            t_end = datetime.time(16, 0)

        try:
            feed = feedparser.parse(rss_url)
            results = []
            
            for entry in feed.entries:
                if not hasattr(entry, 'published_parsed'):
                    continue
                
                full_text = (entry.title + " " + entry.get('summary', "")).lower()
                
                # 1. Source Check
                if allowed_sources:
                    if not any(s in full_text for s in allowed_sources):
                        continue

                # 2. Keyword Check
                if required_keywords:
                    if not any(k in full_text for k in required_keywords):
                        continue
                
                # 3. Time Check
                dt_utc = datetime.datetime.fromtimestamp(time.mktime(entry.published_parsed), pytz.utc)
                dt_et = dt_utc.astimezone(EASTERN)
                
                # Dynamic Window Check first (Must be AFTER prev close)
                # window_start is passed from main as UTC
                if dt_utc <= window_start.replace(tzinfo=pytz.utc):
                    continue

                # Market Hours Exclusion (9:30-16:00 ET)
                # ONLY apply on Weekdays (Mon=0, Sun=6). Sat(5)/Sun(6) are always valid.
                if dt_et.weekday() < 5: # 0-4 is Mon-Fri
                    t = dt_et.time()
                    if t_start <= t <= t_end:
                        # Exclude Intraday
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
