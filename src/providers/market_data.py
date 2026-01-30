import yfinance as yf
import pandas as pd
from abc import ABC, abstractmethod
from typing import List, Optional
import logging
import io
import requests

logger = logging.getLogger(__name__)

class MarketDataProvider(ABC):
    @abstractmethod
    def get_universe(self, mode: str, config_watchlist: List[str] = None) -> List[str]:
        pass

    @abstractmethod
    def get_market_cap(self, ticker: str) -> float:
        pass

class YFinanceProvider(MarketDataProvider):
    def get_universe(self, mode: str, config_watchlist: List[str] = None) -> List[str]:
        if mode == "WATCHLIST":
            return config_watchlist or []
        elif mode == "SP1500":
            try:
                tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
                df = tables[0]
                return df['Symbol'].tolist()
            except Exception as e:
                logger.error(f"Failed to fetch S&P 500: {e}")
                return ["AAPL", "MSFT"]
        elif mode == "ALL_US":
            return self._fetch_all_us_tickers() or config_watchlist or []
        else:
            return []

    def _fetch_all_us_tickers(self) -> List[str]:
        logger.info("Downloading full symbol list from Nasdaq Trader (Manual)...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        tickers = set()

        # 1. Nasdaq Listed
        try:
            url = "http://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
            df = pd.read_csv(url, sep="|")
            # Filter
            # Structure: Symbol|Security Name|...|Test Issue|...
            # Remove Test Issue = 'Y'
            if 'Test Issue' in df.columns:
                df = df[df['Test Issue'] != 'Y']
            if 'Symbol' in df.columns:
                tickers.update(df['Symbol'].unique())
        except Exception as e:
            logger.error(f"Failed to download nasdaqlisted: {e}")

        # 2. Other Listed (NYSE/AMEX)
        try:
            url = "http://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
            df = pd.read_csv(url, sep="|")
             # Structure: Act Symbol|Security Name|...|Test Issue|...
             # Note: "Act Symbol" is the column
             # Also "Test Issue"
            if 'Test Issue' in df.columns:
                df = df[df['Test Issue'] != 'Y']
            if 'ACT Symbol' in df.columns:
                 tickers.update(df['ACT Symbol'].unique())
            elif 'Symbol' in df.columns:
                 tickers.update(df['Symbol'].unique())
        except Exception as e:
             logger.error(f"Failed to download otherlisted: {e}")
        
        # Clean up
        cleaned = []
        for t in tickers:
             if not isinstance(t, str): continue
             # Remove ETFs/Warrants if possible? 
             # Simpler: just ensure it's a valid string.
             # Convert dots to dashes? yfinance uses dashes.
             # Note: Nasdaq uses 'BRK.B', yfinance uses 'BRK-B'
             cleaned.append(t.replace('.', '-'))
             
        logger.info(f"Retrieved {len(cleaned)} symbols.")
        return sorted(list(cleaned))

    def get_market_cap(self, ticker: str) -> float:
        """Returns market cap, or None if failed."""
        try:
            safe_ticker = ticker.replace('.', '-')
            info = yf.Ticker(safe_ticker).info
            return info.get('marketCap')
        except Exception:
            return None
