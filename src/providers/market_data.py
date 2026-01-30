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
        """Returns dict with 'market_cap' (float) and 'name' (str)"""
        pass

class YFinanceProvider(MarketDataProvider):
    def get_universe(self, mode: str, config_watchlist: List[str] = None) -> List[str]:
        if mode == "WATCHLIST":
            return config_watchlist or []
        elif mode == "SP1500":
             return ["AAPL", "MSFT"] # Simplified fallback
        elif mode == "ALL_US":
            return self._fetch_all_us_tickers() or config_watchlist or []
        else:
            return []

    def _fetch_all_us_tickers(self) -> List[str]:
        # ... (Previous implementation kept same for brevity, but re-included to ensure file writes correctly)
        # To avoid re-writing 50 lines, I will trust the implementation is similar 
        # but for this Tool Use I must provide full content or use replace.
        # I'll provide full content to be safe.
        logger.info("Downloading full symbol list from Nasdaq Trader (Manual)...")
        tickers = set()
        try:
            url = "http://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
            df = pd.read_csv(url, sep="|")
            if 'Test Issue' in df.columns: df = df[df['Test Issue'] != 'Y']
            if 'Symbol' in df.columns: tickers.update(df['Symbol'].unique())
        except Exception: pass

        try:
            url = "http://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
            df = pd.read_csv(url, sep="|")
            if 'Test Issue' in df.columns: df = df[df['Test Issue'] != 'Y']
            if 'ACT Symbol' in df.columns: tickers.update(df['ACT Symbol'].unique())
            elif 'Symbol' in df.columns: tickers.update(df['Symbol'].unique())
        except Exception: pass
        
        cleaned = []
        for t in tickers:
             if isinstance(t, str): cleaned.append(t.replace('.', '-'))
        return sorted(list(cleaned))

    def get_market_cap(self, ticker: str) -> float:
        # Backward compatibility if needed, but we prefer get_ticker_details
        d = self.get_ticker_details(ticker)
        return d.get('market_cap')

    def get_ticker_details(self, ticker: str) -> Dict:
        try:
            safe_ticker = ticker.replace('.', '-')
            info = yf.Ticker(safe_ticker).info
            return {
                'market_cap': info.get('marketCap'),
                'name': info.get('shortName') or info.get('longName') or ticker
            }
        except Exception:
            return {'market_cap': None, 'name': ticker}
