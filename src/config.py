import yaml
import os
from pathlib import Path

def load_config(config_path: str = "config/config.yaml") -> dict:
    """Utilities to load and validate the configuration."""
    
    # Resolve absolute path if needed, assuming run from root or similar
    # For now, simplistic path resolution
    base_path = Path(os.getcwd())
    full_path = base_path / config_path
    
    if not full_path.exists():
        # Try looking one level up if running from src
        full_path = base_path.parent / config_path
        
    if not full_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path} or {full_path}")

    with open(full_path, 'r') as f:
        config = yaml.safe_load(f)
        
    return config

def load_denylist(denylist_path: str) -> set:
    """Loads a set of denylisted tickers from a file."""
    base_path = Path(os.getcwd())
    full_path = base_path / denylist_path
    
    if not full_path.exists():
         # Try looking one level up if running from src
        full_path = base_path.parent / denylist_path

    if not full_path.exists():
        print(f"Warning: Denylist file not found at {denylist_path}. Returning empty set.")
        return set()
        
    with open(full_path, 'r') as f:
        lines = f.readlines()
        
    return {line.strip().upper() for line in lines if line.strip() and not line.startswith('#')}
