import pandas as pd
import pytest
from src.scoring.volume_spike import VolumeSpikeDetector

@pytest.fixture
def mock_config():
    return {
        'thresholds': {
            'volume_multiple_median': 3.0,
            'volume_multiple_mean': 2.5,
            'lookback_days': 20,
            'min_abs_volume': 1000,
            'min_abs_pct_move': 0.0
        }
    }

def test_volume_spike_detected(mock_config):
    detector = VolumeSpikeDetector(mock_config)
    
    # Create synthetic data
    # History: 20 days of volume=1000
    dates = pd.date_range(end='2026-01-29', periods=21)
    df = pd.DataFrame({
        'Date': dates,
        'Open': 100, 'High': 110, 'Low': 90, 'Close': 105,
        'Volume': [1000] * 20 + [5000] # Last day is 5000 (5x median)
    }).set_index('Date')
    
    res = detector.check_spike(df)
    assert res is not None
    assert res['volume_today'] == 5000
    assert res['multiple'] == 5.0

def test_no_spike_low_volume(mock_config):
    detector = VolumeSpikeDetector(mock_config)
    
    # History: 20 days of volume=1000
    # Today: 2000 (2x median, threshold is 3x)
    dates = pd.date_range(end='2026-01-29', periods=21)
    df = pd.DataFrame({
        'Date': dates,
        'Open': 100, 'High': 110, 'Low': 90, 'Close': 105,
        'Volume': [1000] * 20 + [2000] 
    }).set_index('Date')
    
    res = detector.check_spike(df)
    assert res is None

def test_liquidity_filter(mock_config):
    # Set min vol high
    mock_config['thresholds']['min_abs_volume'] = 10000 
    detector = VolumeSpikeDetector(mock_config)
    
    # Today vol 5000 (spike vs history of 100, but < min_abs)
    dates = pd.date_range(end='2026-01-29', periods=21)
    df = pd.DataFrame({
        'Date': dates,
        'Open': 100, 'High': 110, 'Low': 90, 'Close': 105,
        'Volume': [100] * 20 + [5000]
    }).set_index('Date')
    
    res = detector.check_spike(df)
    assert res is None
