import pandas as pd
import logging

logger = logging.getLogger(__name__)

class VolumeSpikeDetector:
    def __init__(self, config: dict):
        self.median_mult = config['thresholds']['volume_multiple_median']
        self.mean_mult = config['thresholds']['volume_multiple_mean']
        self.min_vol = config['thresholds']['min_abs_volume']
        self.min_pct_move = config['thresholds'].get('min_abs_pct_move', 0.0)
        self.lookback = config['thresholds']['lookback_days']

    def check_spike(self, df: pd.DataFrame) -> dict:
        """
        Checks if the last row in df is a volume spike.
        Returns dict with details if spike, else None.
        """
        if len(df) < self.lookback + 1:
            return None
        
        # Split into "today" (last row) and "history" (previous N)
        # Assuming df is sorted by date ascending
        today = df.iloc[-1]
        history = df.iloc[-(self.lookback+1):-1] # Last 20 days excluding today
        
        vol_today = today['Volume']
        close_today = today['Close']
        open_today = today['Open']
        
        # Check liquidity floor
        if vol_today < self.min_vol:
            return None
            
        med_vol = history['Volume'].median()
        mean_vol = history['Volume'].mean()
        
        threshold = max(self.median_mult * med_vol, self.mean_mult * mean_vol)
        
        is_spike = vol_today >= threshold
        
        if is_spike:
            # Check price move if configured
            # Calculate % change from prev close (or today open if prev close missing, but history should have it)
            prev_close = history.iloc[-1]['Close']
            pct_change = ((close_today - prev_close) / prev_close) * 100
            
            if abs(pct_change) < self.min_pct_move:
                return None
            
            return {
                'volume_today': int(vol_today),
                'volume_median': int(med_vol),
                'volume_mean': int(mean_vol),
                'multiple': round(vol_today / med_vol, 2) if med_vol > 0 else 0,
                'price_close': round(close_today, 2),
                'pct_change': round(pct_change, 2),
                'date': today.name.strftime('%Y-%m-%d')
            }
            
        return None
