from typing import List, Set, Optional
import logging

logger = logging.getLogger(__name__)

class FilterEngine:
    def __init__(self, config: dict, denylist: Set[str]):
        self.excluded_sectors = set(config['exclusions']['sectors'])
        self.keywords = set(k.lower() for k in config['exclusions']['industries_keywords'])
        self.denylist = denylist
        self.max_market_cap = config.get('max_market_cap') # None implies no limit

    def check_market_cap(self, market_cap: Optional[float]) -> bool:
        """
        Returns True if ALLOWED (i.e. < max_cap).
        If market_cap is None (missing data), allow it or strict?
        Let's allow it but warn, or maybe strictly require it.
        For Microcap hunting, strict is safer.
        """
        if self.max_market_cap is None:
            return True
        if market_cap is None:
            # If we can't verify cap, we might skip or Include.
            # Safe bet: Skip to reduce noise.
            return False 
        
        return market_cap < self.max_market_cap

    def is_pharma_excluded(self, ticker: str, sector: str, industry: str, pr_text: str = "") -> bool:
        if ticker in self.denylist:
            return True, "Denylist"

        if sector and sector in self.excluded_sectors:
            return True, f"Sector: {sector}"
        
        if industry:
            ind_lower = industry.lower()
            for k in self.keywords:
                if k in ind_lower:
                    return True, f"Industry Keyword: {k}"

        if pr_text:
            text_lower = pr_text.lower()
            for k in self.keywords:
                 if k in text_lower:
                    return True, f"PR Keyword: {k}"
        
        return False, ""
