import feedparser
import datetime
from abc import ABC, abstractmethod
from typing import List, NamedTuple
import logging
import requests
from urllib.parse import quote_plus
import time

logger = logging.getLogger(__name__)

class PRItem(NamedTuple):
    ticker: str
    headline: str
    url: str
    source: str
    published_at: datetime.datetime

class PRSource(ABC):
    @abstractmethod
    def get_prs(self, ticker: str, window_start: datetime.datetime) -> List[PRItem]:
        pass

class RSSPRSource(PRSource):
    """
    Checks specific RSS feeds.
    NOTE: Most public RSS feeds are aggregate (all news), not per-ticker.
    Finding a free per-ticker RSS feed is hard. 
    Yahoo Finance has an RSS feed per ticker: https://feeds.finance.yahoo.com/rss/2.0/headline?s=TICKER
    """
    
    def get_prs(self, ticker: str, window_start: datetime.datetime) -> List[PRItem]:
        safe_ticker = ticker.replace('.', '-')
        rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={safe_ticker}"
        
        try:
            feed = feedparser.parse(rss_url)
            results = []
            
            for entry in feed.entries:
                # Parse timestamp
                # feedparser usually handles this in published_parsed (struct_time)
                if not hasattr(entry, 'published_parsed'):
                    continue
                    
                pub_ts = datetime.datetime.fromtimestamp(time.mktime(entry.published_parsed))
                # Ensure timezone awareness if window_start is aware, or convert.
                # Simplification: Assume pure UTC comparison or rely on naive
                
                # Make pub_ts naive for comparison if window_start is naive, or match timezones
                # For MVP, removing tz info usually works for simple "last 36 hours" checks
                pub_ts_check = pub_ts.replace(tzinfo=None)
                window_start_check = window_start.replace(tzinfo=None)
                
                if pub_ts_check >= window_start_check:
                    results.append(PRItem(
                        ticker=ticker,
                        headline=entry.title,
                        url=entry.link,
                        source="Yahoo Finance RSS",
                        published_at=pub_ts
                    ))
            
            return results
            
        except Exception as e:
            logger.error(f"Error fetching RSS for {ticker}: {e}")
            return []

class GoogleNewsFallback(PRSource):
    """
    Fallback using Google News RSS search.
    Query: Ticker + "Press Release"
    """
    def get_prs(self, ticker: str, window_start: datetime.datetime) -> List[PRItem]:
        query = f"{ticker} press release"
        encoded_query = quote_plus(query)
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"
        
        try:
            feed = feedparser.parse(rss_url)
            results = []
            for entry in feed.entries:
                 if not hasattr(entry, 'published_parsed'):
                    continue
                 pub_ts = datetime.datetime.fromtimestamp(time.mktime(entry.published_parsed))
                 
                 pub_ts_check = pub_ts.replace(tzinfo=None)
                 window_start_check = window_start.replace(tzinfo=None)

                 if pub_ts_check >= window_start_check:
                     results.append(PRItem(
                        ticker=ticker,
                        headline=entry.title,
                        url=entry.link,
                        source="Google News RSS",
                        published_at=pub_ts
                    ))
            return results
        except Exception as e:
            logger.error(f"Error fetching Google News for {ticker}: {e}")
            return []
