import yfinance as yf
import pandas as pd
from abc import ABC, abstractmethod
from typing import List, Optional
import logging
import datetime

logger = logging.getLogger(__name__)

class MarketDataProvider(ABC):
    @abstractmethod
    def get_universe(self, mode: str, config_watchlist: List[str] = None) -> List[str]:
        """Returns a list of tickers based on the mode."""
        pass

    @abstractmethod
    def get_history(self, ticker: str, lookback_days: int) -> pd.DataFrame:
        """Returns historical OHLCV data. 
        DataFrame should have index as Datetime, and columns: [Open, High, Low, Close, Volume]
        """
        pass

class YFinanceProvider(MarketDataProvider):
    def get_universe(self, mode: str, config_watchlist: List[str] = None) -> List[str]:
        if mode == "WATCHLIST":
            return config_watchlist or []
        elif mode == "SP1500" or mode == "SP500":
            # MVP: Fallback to S&P 500 from Wikipedia to ensure we have a good free universe
            try:
                tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
                df = tables[0]
                return df['Symbol'].tolist()
            except Exception as e:
                logger.error(f"Failed to fetch S&P 500 list from Wikipedia: {e}")
                # Fallback to a tiny list so we don't crash hard
                return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA"]
        else:
            logger.warning(f"Unknown universe mode {mode}, returning empty list.")
            return []

    def get_history(self, ticker: str, lookback_days: int) -> pd.DataFrame:
        # We need enough buffer for median calculation (e.g. 20 trading days).
        # Fetching 2 months is safe.
        # yfinance allows period="1mo", "3mo", etc.
        try:
            # yfinance tickers for dots often need translation (e.g. BRK.B -> BRK-B)
            safe_ticker = ticker.replace('.', '-')
            ticker_obj = yf.Ticker(safe_ticker)
            
            # Fetch history
            # period="3mo" gives plenty of data for 20-day MA
            df = ticker_obj.history(period="3mo")
            
            if df.empty:
                logger.warning(f"No data found for {ticker}")
                return pd.DataFrame()
            
            # Ensure columns are standard
            df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
            
            # Filter to relevant lookback if needed, or just return everything
            # The caller will slice the last N days.
            return df
            
        except Exception as e:
            logger.error(f"Error fetching history for {ticker}: {e}")
            return pd.DataFrame()
