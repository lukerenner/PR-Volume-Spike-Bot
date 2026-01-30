import yfinance as yf
import logging

logger = logging.getLogger(__name__)

class SectorProvider:
    def get_sector_industry(self, ticker: str):
        """
        Returns (sector, industry) tuple.
        Returns (None, None) if not found.
        """
        try:
            safe_ticker = ticker.replace('.', '-')
            info = yf.Ticker(safe_ticker).info
            return info.get('sector'), info.get('industry')
        except Exception as e:
            logger.error(f"Error fetching sector info for {ticker}: {e}")
            return None, None
