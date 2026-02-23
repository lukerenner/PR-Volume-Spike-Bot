import calendar
import feedparser
import datetime
import pytz
import re
from abc import ABC, abstractmethod
from typing import List, NamedTuple, Set, Dict, Optional
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

    Primary workflow (PR-first):
      1. Call get_all_candidate_tickers() to get every ticker mentioned in today's PRs.
      2. The caller checks those tickers for volume spikes.
      3. Call get_prs(ticker, ...) to retrieve the matching PRs for confirmed spike tickers.

    Caches the full feed for 10 minutes to avoid hammering wire services.
    """

    RSS_FEEDS = {
        # GlobeNewswire exchange feeds: 100% ticker-tagged, US-listed companies only
        "GlobeNewswire NASDAQ": "https://www.globenewswire.com/RssFeed/exchange/NASDAQ",
        "GlobeNewswire NYSE":   "https://www.globenewswire.com/RssFeed/exchange/NYSE",
        "GlobeNewswire AMEX":   "https://www.globenewswire.com/RssFeed/exchange/AMEX",
        # PR Newswire general feed — lower resolution but adds diversity
        "PR Newswire":          "https://www.prnewswire.com/rss/news-releases-list.rss",
    }

    def __init__(self):
        self._cache: Dict[str, List[dict]] = {}
        self._cache_time: Optional[datetime.datetime] = None
        self._cache_ttl = datetime.timedelta(minutes=10)
        # Injected by main after SEC EDGAR map is loaded
        self._name_to_ticker: Dict[str, str] = {}

    def set_name_to_ticker_map(self, mapping: Dict[str, str]):
        """Provide the company-name → ticker lookup built from SEC EDGAR."""
        self._name_to_ticker = mapping

    # ------------------------------------------------------------------
    # Internal feed fetch
    # ------------------------------------------------------------------

    def _fetch_all_prs(self) -> List[dict]:
        """Fetch PRs from all wire services, with caching."""
        now = datetime.datetime.now(pytz.utc)

        if self._cache_time and (now - self._cache_time) < self._cache_ttl:
            all_items = []
            for items in self._cache.values():
                all_items.extend(items)
            return all_items

        self._cache = {}
        for source_name, url in self.RSS_FEEDS.items():
            try:
                feed = feedparser.parse(url)
                items = []
                for entry in feed.entries:
                    if not hasattr(entry, 'published_parsed') or not entry.published_parsed:
                        continue

                    # Use calendar.timegm (treats struct_time as UTC) to avoid
                    # time.mktime's local-timezone misinterpretation bug
                    dt_utc = datetime.datetime.fromtimestamp(
                        calendar.timegm(entry.published_parsed), pytz.utc
                    )

                    # Extract tickers explicitly embedded in category tags
                    # (GlobeNewswire and Business Wire use this convention)
                    tickers_in_category: Set[str] = set()
                    if hasattr(entry, 'tags'):
                        for tag in entry.tags:
                            term = tag.get('term', '')
                            match = re.search(
                                r'(?:NYSE|NASDAQ|AMEX|OTC|TSX|TSX-V)[:\-]([A-Z]{1,5})',
                                term, re.I
                            )
                            if match:
                                tickers_in_category.add(match.group(1).upper())

                    # Pull full text: try summary, then description, then content
                    summary = (
                        entry.get('summary')
                        or entry.get('description')
                        or (entry.content[0].get('value', '') if hasattr(entry, 'content') and entry.content else '')
                        or ''
                    )

                    items.append({
                        'title':               entry.title,
                        'link':                entry.link,
                        'summary':             summary,
                        'published_at':        dt_utc,
                        'source':              source_name,
                        'tickers_in_category': tickers_in_category,
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

    # ------------------------------------------------------------------
    # Company-name extraction helpers
    # ------------------------------------------------------------------

    # Common corporate suffixes to strip before name lookup
    _CORP_SUFFIXES = re.compile(
        r'\b(Inc\.?|Corp\.?|Ltd\.?|LLC|L\.L\.C\.?|PLC|plc|Co\.?|'
        r'Holdings?|Group|Technologies|Technology|Sciences?|Therapeutics|'
        r'Pharmaceuticals?|Biosciences?|Solutions?|Systems?|Networks?|'
        r'Enterprises?|International|Worldwide|Global|Capital|Partners?|'
        r'Acquisition|Acquisitions)\b\.?',
        re.I
    )

    def _extract_company_names_from_title(self, title: str) -> List[str]:
        """
        Heuristic: the issuing company name is usually the first noun phrase
        before common PR verbs (Announces, Reports, Completes, etc.).
        Returns a list of candidate name strings to look up.
        """
        # Strip leading source attribution like "COMPANY NAME / Business Wire:"
        title = re.sub(r'^[^/]+/\s*(?:PR Newswire|Business Wire|GlobeNewswire|Accesswire)\s*[:/—–-]\s*', '', title, flags=re.I)

        # Split on common PR verbs to isolate the company name portion
        split_pattern = re.compile(
            r'\s+(?:Announces?|Reports?|Completes?|Launches?|Signs?|Enters?|'
            r'Secures?|Receives?|Expands?|Awards?|Selects?|Closes?|Grants?|'
            r'Appoints?|Acquires?|Partners?|Agrees?|Files?|Achieves?|Confirms?|'
            r'Updates?|Provides?|Releases?|Presents?|Declares?|Regains?|'
            r'Executes?|Completes?)\b',
            re.I
        )
        parts = split_pattern.split(title, maxsplit=1)
        if not parts:
            return []

        raw_name = parts[0].strip()
        # Drop parenthetical ticker hints like "(NASDAQ: XYZ)"
        raw_name = re.sub(r'\s*\(.*?\)', '', raw_name).strip()

        candidates = [raw_name]

        # Also try with corporate suffixes stripped
        stripped = self._CORP_SUFFIXES.sub('', raw_name).strip().strip(',').strip()
        if stripped and stripped != raw_name:
            candidates.append(stripped)

        return [c for c in candidates if len(c) > 2]

    def _resolve_name_to_ticker(self, name: str) -> Optional[str]:
        """Look up a company name in the SEC EDGAR map (case-insensitive)."""
        if not self._name_to_ticker:
            return None
        key = name.strip().lower()
        return self._name_to_ticker.get(key)

    # ------------------------------------------------------------------
    # Ticker extraction — inline tags OR name resolution
    # ------------------------------------------------------------------

    def _get_tickers_for_pr(self, pr: dict) -> Set[str]:
        """
        Return the set of tickers associated with a PR entry.
        Strategy (in priority order):
          1. Explicit exchange:ticker tags in the feed categories.
          2. Ticker symbol mentioned directly in title/summary text.
          3. Company name → ticker via SEC EDGAR name map.
        """
        tickers: Set[str] = set()

        # 1. Category tags (most reliable)
        tickers.update(pr.get('tickers_in_category', set()))
        if tickers:
            return tickers

        title = pr.get('title', '')
        summary = pr.get('summary', '')
        full_text = (title + ' ' + summary).upper()

        # 2. Bare ticker mentioned in text — pattern: (NASDAQ: XYZ) or NYSE:XYZ
        exchange_tagged = re.findall(
            r'(?:NYSE|NASDAQ|AMEX|OTC)[:\s]+([A-Z]{1,5})\b', full_text
        )
        tickers.update(exchange_tagged)
        if tickers:
            return tickers

        # 2b. $TICKER format (common in financial wire copy, e.g. "$PLTR")
        dollar_tickers = re.findall(r'\$([A-Z]{1,5})\b', full_text)
        tickers.update(dollar_tickers)
        if tickers:
            return tickers

        # 3. Name-to-ticker resolution
        for candidate_name in self._extract_company_names_from_title(title):
            ticker = self._resolve_name_to_ticker(candidate_name)
            if ticker:
                tickers.add(ticker)
                break  # One match is enough

        return tickers

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_all_candidate_tickers(
        self,
        window_start: datetime.datetime,
        config: dict,
        trading_dates: Set[datetime.date] = None,
    ) -> Dict[str, List[dict]]:
        """
        PR-first entry point.

        Returns a dict mapping ticker → list of matching PR dicts for all PRs
        published after window_start that pass keyword and time filters.
        The caller iterates over these tickers, checks volume spikes, then
        calls get_prs() to fetch the structured PRItem list for confirmed spikes.
        """
        pr_cfg = config.get('pr_config', {})
        required_keywords = [k.lower() for k in pr_cfg.get('required_keywords', [])]

        excl_start_str = pr_cfg.get('exclude_time_start_et', '')
        excl_end_str = pr_cfg.get('exclude_time_end_et', '')
        t_start = t_end = None
        if excl_start_str and excl_end_str:
            try:
                h1, m1 = map(int, excl_start_str.split(':'))
                h2, m2 = map(int, excl_end_str.split(':'))
                t_start = datetime.time(h1, m1)
                t_end = datetime.time(h2, m2)
            except Exception:
                pass

        all_prs = self._fetch_all_prs()
        ticker_to_prs: Dict[str, List[dict]] = {}

        for pr in all_prs:
            dt_utc = pr['published_at']

            if dt_utc <= window_start.replace(tzinfo=pytz.utc):
                continue

            # Keyword filter
            full_text_lower = (pr['title'] + ' ' + pr.get('summary', '')).lower()
            if required_keywords and not any(k in full_text_lower for k in required_keywords):
                continue

            # Market-hours exclusion
            dt_et = dt_utc.astimezone(EASTERN)
            pr_date = dt_et.date()
            should_check_time = True
            if trading_dates is not None:
                if pr_date not in trading_dates:
                    should_check_time = False
            elif dt_et.weekday() >= 5:
                should_check_time = False

            if should_check_time and t_start and t_end:
                if t_start <= dt_et.time() <= t_end:
                    continue

            # Resolve tickers
            tickers = self._get_tickers_for_pr(pr)
            for ticker in tickers:
                ticker_to_prs.setdefault(ticker, []).append(pr)

        logger.info(f"PR-first: resolved {len(ticker_to_prs)} candidate tickers from RSS feeds")
        return ticker_to_prs

    def get_prs(
        self,
        ticker: str,
        window_start: datetime.datetime,
        config: dict,
        trading_dates: Set[datetime.date] = None,
    ) -> List[PRItem]:
        """
        Returns structured PRItems for a specific ticker (used after volume
        spike confirmation to build the alert payload).
        """
        # Pull from the candidate map if cache is warm
        candidate_map = self.get_all_candidate_tickers(window_start, config, trading_dates)
        raw_prs = candidate_map.get(ticker.upper(), [])

        results = []
        for pr in raw_prs:
            results.append(PRItem(
                ticker=ticker,
                headline=pr['title'],
                url=pr['link'],
                source=pr['source'],
                published_at=pr['published_at'],
            ))
        return results
