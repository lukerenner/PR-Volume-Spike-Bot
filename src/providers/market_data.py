import yfinance as yf
import pandas as pd
from abc import ABC, abstractmethod
from typing import List, Optional, Dict
import logging
import requests

logger = logging.getLogger(__name__)

class MarketDataProvider(ABC):
    @abstractmethod
    def get_universe(self, mode: str, config_watchlist: List[str] = None) -> List[str]:
        pass

    @abstractmethod
    def get_ticker_details(self, ticker: str) -> Dict:
        """Returns dict with 'market_cap', 'name', 'sector', 'industry'."""
        pass

class YFinanceProvider(MarketDataProvider):
    def get_universe(self, mode: str, config_watchlist: List[str] = None) -> List[str]:
        if mode == "WATCHLIST":
            return config_watchlist or []
        elif mode == "SP1500":
            return self._fetch_all_us_tickers() or config_watchlist or []
        elif mode == "ALL_US":
            return self._fetch_all_us_tickers() or config_watchlist or []
        else:
            return []

    def _fetch_all_us_tickers(self) -> List[str]:
        tickers = self._fetch_from_sec_edgar()
        if tickers:
            return tickers

        logger.warning("SEC EDGAR failed, trying Nasdaq Trader FTP fallback...")
        tickers = self._fetch_from_nasdaq_ftp()
        if tickers:
            return tickers

        logger.error("All ticker sources failed. Universe will fall back to watchlist.")
        return []

    def _fetch_from_sec_edgar(self) -> List[str]:
        logger.info("Downloading ticker list from SEC EDGAR...")
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {"User-Agent": "PRVolumeBot/1.0 contact@example.com"}
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            tickers = set()
            for entry in data.values():
                ticker = entry.get("ticker")
                if ticker and isinstance(ticker, str):
                    tickers.add(ticker.replace(".", "-"))
            logger.info(f"SEC EDGAR returned {len(tickers)} tickers")
            return sorted(tickers)
        except Exception as e:
            logger.error(f"SEC EDGAR fetch failed: {e}")
            return []

    def _fetch_from_nasdaq_ftp(self) -> List[str]:
        logger.info("Downloading full symbol list from Nasdaq Trader FTP...")
        tickers = set()
        try:
            url = "http://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
            df = pd.read_csv(url, sep="|")
            if 'Test Issue' in df.columns:
                df = df[df['Test Issue'] != 'Y']
            if 'Symbol' in df.columns:
                tickers.update(df['Symbol'].unique())
        except Exception as e:
            logger.warning(f"Nasdaq listed fetch failed: {e}")

        try:
            url = "http://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
            df = pd.read_csv(url, sep="|")
            if 'Test Issue' in df.columns:
                df = df[df['Test Issue'] != 'Y']
            if 'ACT Symbol' in df.columns:
                tickers.update(df['ACT Symbol'].unique())
            elif 'Symbol' in df.columns:
                tickers.update(df['Symbol'].unique())
        except Exception as e:
            logger.warning(f"Nasdaq other-listed fetch failed: {e}")

        cleaned = [t.replace('.', '-') for t in tickers if isinstance(t, str)]
        if cleaned:
            logger.info(f"Nasdaq FTP returned {len(cleaned)} tickers")
        return sorted(cleaned)

    def build_name_to_ticker_map(self) -> Dict[str, str]:
        """
        Build a lowercase company-name → ticker map from SEC EDGAR.
        Used by RSSPRSource for name-based ticker resolution.
        Returns empty dict on failure (non-fatal).
        """
        logger.info("Building company name → ticker map from SEC EDGAR...")
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {"User-Agent": "PRVolumeBot/1.0 contact@example.com"}
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            name_map: Dict[str, str] = {}
            for entry in data.values():
                ticker = entry.get("ticker")
                name = entry.get("title")  # SEC uses "title" for company name
                if ticker and name and isinstance(name, str):
                    name_map[name.strip().lower()] = ticker.upper()
            logger.info(f"Name→ticker map built: {len(name_map)} entries")
            return name_map
        except Exception as e:
            logger.warning(f"Failed to build name→ticker map: {e}")
            return {}

    def get_ticker_details(self, ticker: str) -> Dict:
        """
        Single yfinance call returning market_cap, name, sector, and industry.
        Previously these were fetched in two separate calls (market_data + sectors).
        """
        try:
            safe_ticker = ticker.replace('.', '-')
            info = yf.Ticker(safe_ticker).info
            return {
                'market_cap': info.get('marketCap'),
                'name':       info.get('shortName') or info.get('longName') or ticker,
                'sector':     info.get('sector'),
                'industry':   info.get('industry'),
            }
        except Exception:
            return {'market_cap': None, 'name': ticker, 'sector': None, 'industry': None}
