from typing import List, Set
import logging

logger = logging.getLogger(__name__)

class PharmaFilter:
    def __init__(self, config: dict, denylist: Set[str]):
        self.excluded_sectors = set(config['exclusions']['sectors'])
        self.keywords = set(k.lower() for k in config['exclusions']['industries_keywords'])
        self.denylist = denylist

    def is_excluded(self, ticker: str, sector: str, industry: str, pr_text: str = "") -> bool:
        # 1. Check Denylist
        if ticker in self.denylist:
            return True, "Denylist"

        # 2. Check Sector/Industry
        if sector and sector in self.excluded_sectors:
            return True, f"Sector: {sector}"
        
        # 3. Check Industry Keywords
        # industry string from yfinance might be "Biotechnology"
        if industry:
            ind_lower = industry.lower()
            for k in self.keywords:
                if k in ind_lower:
                    return True, f"Industry Keyword: {k} in {industry}"

        # 4. Check PR Content / Headline (Heuristic fallback)
        if pr_text:
            text_lower = pr_text.lower()
            for k in self.keywords:
                 if k in text_lower:
                    # Tighter check: ensure it's not just a passing mention?
                    # For MVP, conservative is better.
                    return True, f"PR Keyword: {k}"
        
        return False, ""
