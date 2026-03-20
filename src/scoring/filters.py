from typing import Set, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class FilterEngine:
    def __init__(self, config: dict, denylist: Set[str]):
        self.excluded_sectors = set(config['exclusions']['sectors'])
        self.keywords = set(k.lower() for k in config['exclusions']['industries_keywords'])
        self.financial_keywords = [
            k.lower() for k in config['exclusions'].get('financial_disclosure_keywords', [])
        ]
        self.denylist = denylist
        self.max_market_cap = config.get('max_market_cap')  # None = no limit

    def check_market_cap(self, market_cap: Optional[float]) -> bool:
        """
        Returns True if the ticker is ALLOWED (market cap below max).
        If market cap is unknown (None), allow it through — unknown cap stocks
        are typically small/micro-cap, which is exactly what we want.
        """
        if self.max_market_cap is None:
            return True
        if market_cap is None:
            logger.debug("market_cap unknown — allowing through (assumed micro-cap)")
            return True
        return market_cap < self.max_market_cap

    def is_pharma_excluded(
        self,
        ticker: str,
        sector: Optional[str],
        industry: Optional[str],
        pr_text: str = "",
    ) -> Tuple[bool, str]:
        """
        Returns (True, reason) if the ticker should be excluded, else (False, "").
        Checks denylist, sector, industry keywords, and PR headline keywords.
        """
        if ticker in self.denylist:
            return True, "Denylist"

        if sector and sector in self.excluded_sectors:
            return True, f"Sector: {sector}"

        if industry:
            ind_lower = industry.lower()
            for k in self.keywords:
                if k in ind_lower:
                    return True, f"Industry keyword: {k}"

        if pr_text:
            text_lower = pr_text.lower()
            for k in self.keywords:
                if k in text_lower:
                    return True, f"PR keyword: {k}"

        return False, ""

    def is_financial_disclosure(self, headline: str) -> Tuple[bool, str]:
        """
        Returns (True, matched_keyword) if the headline is a financial disclosure
        (earnings, offerings, splits, dividends, debt, etc.) that is not replicable
        by a marketing team and should be suppressed.
        """
        h = headline.lower()
        for kw in self.financial_keywords:
            if kw in h:
                return True, kw
        return False, ""
