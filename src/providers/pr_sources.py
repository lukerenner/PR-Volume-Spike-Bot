import feedparser
import datetime
import pytz
from abc import ABC, abstractmethod
from typing import List, NamedTuple
import logging
import requests
from urllib.parse import quote_plus
import time

logger = logging.getLogger(__name__)

# Eastern Time Zone
EASTERN = pytz.timezone('US/Eastern')

class PRItem(NamedTuple):
    ticker: str
    headline: str
    url: str
    source: str
    published_at: datetime.datetime

class PRSource(ABC):
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
        max_time_str = pr_cfg.get('max_time_et', "09:00")
        
        # Parse max time
        try:
            mh, mm = map(int, max_time_str.split(':'))
            max_time = datetime.time(mh, mm)
        except:
            max_time = datetime.time(9, 0)

        try:
            feed = feedparser.parse(rss_url)
            results = []
            
            for entry in feed.entries:
                if not hasattr(entry, 'published_parsed'):
                    continue
                
                # 1. Source Check
                # Yahoo RSS usually puts source in "title"? No, often it's not explicitly structured.
                # Actually Yahoo Finance RSS doesn't always strictly separate Source.
                # However, it might be in 'summary' or 'author'.
                # Let's check entry.get('source') if available.
                #feedparser puts it in entry.source usually?
                
                # Debugging Yahoo RSS structure: usually no "source" field in standard feed.
                # Often the title is "Headline - Source". Or we can't filter source easily here.
                # If Strict Mode is ON, relying on Yahoo RSS is risky for "Source" without more parsing.
                # We will accept it if we can match keywords (e.g. "PR Newswire" in description?).
                # Strategy: Check entire entry text (title + summary) for allowed source string.
                
                full_text = (entry.title + " " + entry.get('summary', "")).lower()
                
                source_match = False
                found_source_name = "Yahoo"
                if allowed_sources:
                    for s in allowed_sources:
                        if s in full_text:
                            source_match = True
                            found_source_name = s.title() # capitalize for display
                            break
                else:
                    source_match = True # No restrictions
                
                if not source_match:
                    continue
                    
                # 2. Keyword Check
                if required_keywords:
                    kw_match = False
                    for k in required_keywords:
                        if k in full_text:
                            kw_match = True
                            break
                    if not kw_match:
                        continue
                
                # 3. Time Check
                # published_parsed is UTC struct_time
                dt_utc = datetime.datetime.fromtimestamp(time.mktime(entry.published_parsed), pytz.utc)
                dt_et = dt_utc.astimezone(EASTERN)
                
                # "Before 9am EST" logic.
                # This usually implies "published TODAY before 9am" or "YESTERDAY after close".
                # If we run at 18:30 ET, "today < 9am" is 9.5 hours ago.
                # We should separate "Date" check (is it recent?) from "Time of Day" check?
                # User Requirement: "PR publish time must be before 9am EST". 
                # Interpreted: If it was published at 10am, ignore it.
                
                if dt_et.time() >= max_time:
                    continue

                # 4. Window Check (Recency)
                # window_start is likely naive "start of processing - 24h".
                # We need to be careful with comparison. 
                # Let's compare timestamps.
                if dt_utc < window_start.replace(tzinfo=pytz.utc):
                    continue

                results.append(PRItem(
                    ticker=ticker,
                    headline=entry.title,
                    url=entry.link,
                    source=found_source_name,
                    published_at=dt_utc
                ))
            
            return results
            
        except Exception as e:
            logger.error(f"RSS error {ticker}: {e}")
            return []

class GoogleNewsFallback(PRSource):
    def get_prs(self, ticker: str, window_start: datetime.datetime, config: dict) -> List[PRItem]:
        # Fallback likely doesn't support strict Source filtering easily without custom query logic
        # We will attempt to add source to query? 
        return [] # Disable fallback for strict requirements to avoid noise.
