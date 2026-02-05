import feedparser
import datetime
import pytz
import re
from abc import ABC, abstractmethod
from typing import List, NamedTuple, Set, Dict
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
    @abstractmethod
    def get_prs(self, ticker: str, window_start: datetime.datetime, config: dict, trading_dates: Set[datetime.date] = None) -> List[PRItem]:
        pass

class RSSPRSource(PRSource):
    """
    Multi-source PR aggregator that pulls from major wire services.
    Caches the full feed to avoid repeated requests when checking multiple tickers.
    """

    # Wire service RSS feeds
    RSS_FEEDS = {
        "PR Newswire": "https://www.prnewswire.com/rss/news-releases-list.rss",
        "GlobeNewswire": "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20Releases",
        "Accesswire": "https://www.accesswire.com/rss/news.xml",
    }

    def __init__(self):
        self._cache: Dict[str, List[dict]] = {}
        self._cache_time: datetime.datetime = None
        self._cache_ttl = datetime.timedelta(minutes=10)

    def _fetch_all_prs(self) -> List[dict]:
        """Fetch PRs from all wire services, with caching."""
        now = datetime.datetime.now(pytz.utc)

        # Return cached if fresh
        if self._cache_time and (now - self._cache_time) < self._cache_ttl:
            all_items = []
            for items in self._cache.values():
                all_items.extend(items)
            return all_items

        # Fetch fresh
        self._cache = {}
        for source_name, url in self.RSS_FEEDS.items():
            try:
                feed = feedparser.parse(url)
                items = []
                for entry in feed.entries:
                    if not hasattr(entry, 'published_parsed') or not entry.published_parsed:
                        continue

                    dt_utc = datetime.datetime.fromtimestamp(
                        time.mktime(entry.published_parsed), pytz.utc
                    )

                    # Extract ticker symbols from GlobeNewswire categories
                    tickers_found = set()
                    if hasattr(entry, 'tags'):
                        for tag in entry.tags:
                            term = tag.get('term', '')
                            # Match patterns like "NYSE:ABC" or "NASDAQ:XYZ" or "TSX:ABC"
                            match = re.search(r'(?:NYSE|NASDAQ|AMEX|OTC|TSX|TSX-V)[:\-]([A-Z]{1,5})', term, re.I)
                            if match:
                                tickers_found.add(match.group(1).upper())

                    items.append({
                        'title': entry.title,
                        'link': entry.link,
                        'summary': entry.get('summary', ''),
                        'published_at': dt_utc,
                        'source': source_name,
                        'tickers_in_category': tickers_found,
                    })

                self._cache[source_name] = items
                logger.info(f"Fetched {len(items)} PRs from {source_name}")

            except Exception as e:
                logger.warning(f"Failed to fetch {source_name}: {e}")
                self._cache[source_name] = []

        self._cache_time = now

        all_items = []
        for items in self._cache.values():
            all_items.extend(items)
        logger.info(f"Total PRs in cache: {len(all_items)}")
        return all_items

    def get_prs(self, ticker: str, window_start: datetime.datetime, config: dict, trading_dates: Set[datetime.date] = None) -> List[PRItem]:
        """Find PRs that mention the given ticker."""
        safe_ticker = ticker.replace('-', '.').upper()  # Normalize for matching
        ticker_variants = {
            ticker.upper(),
            safe_ticker,
            ticker.replace('.', '-').upper(),
        }

        pr_cfg = config.get('pr_config', {})
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

        all_prs = self._fetch_all_prs()
        results = []

        for pr in all_prs:
            dt_utc = pr['published_at']

            # Time window check
            if dt_utc <= window_start.replace(tzinfo=pytz.utc):
                continue

            # Check if ticker matches
            ticker_match = False

            # 1. Check category tags (most reliable for GlobeNewswire)
            if ticker_variants & pr.get('tickers_in_category', set()):
                ticker_match = True

            # 2. Check title/summary for ticker mention with word boundaries
            if not ticker_match:
                full_text = (pr['title'] + " " + pr.get('summary', '')).upper()
                for variant in ticker_variants:
                    # Match ticker with word boundaries (not part of another word)
                    pattern = r'\b' + re.escape(variant) + r'\b'
                    if re.search(pattern, full_text):
                        ticker_match = True
                        break

            if not ticker_match:
                continue

            # Keyword check (optional - if configured)
            full_text_lower = (pr['title'] + " " + pr.get('summary', '')).lower()
            if required_keywords and not any(k in full_text_lower for k in required_keywords):
                continue

            # Market hours exclusion
            dt_et = dt_utc.astimezone(EASTERN)
            pr_date = dt_et.date()

            should_check_time = True
            if trading_dates is not None:
                if pr_date not in trading_dates:
                    should_check_time = False
            else:
                if dt_et.weekday() >= 5:  # Sat/Sun
                    should_check_time = False

            if should_check_time:
                t = dt_et.time()
                if t_start <= t <= t_end:
                    continue

            results.append(PRItem(
                ticker=ticker,
                headline=pr['title'],
                url=pr['link'],
                source=pr['source'],
                published_at=dt_utc
            ))

        return results
