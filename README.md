# PR Volume Spike Bot

Daily monitoring tool that detects US equity volume spikes, correlates them with same-day Press Releases, and alerts via Slack.

## Features
- **Volume Spike Detection**: Checks if volume is > 3x median (20d) and > 2.5x mean.
- **PR Association**: Searches RSS feeds (Yahoo Finance, Google News fallback) for PRs in the last 36 hours.
- **Pharma Filtering**: Excludes Healthcare/Biotech stocks to avoid "Phase 3" noise.
- **Slack Alerts**: Sends rich formatting alerts with links.
- **Free**: Uses `yfinance` and RSS feeds. No API keys required for data (unless adding custom providers).

## Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configuration**
   Edit `config/config.yaml` to set your preferences.
   - `slacks.webhook_env_var`: Name of the ENV var containing your Slack Webhook URL.
   - `universe.mode`: `SP1500` (broad) or `WATCHLIST` (specific tickers).
   - `thresholds`: Adjust volume/price sensitivity.

3. **Slack Setup**
   - Create an Incoming Webhook in your Slack Workspace.
   - Export the URL as an environment variable:
     ```bash
     export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
     ```

## Usage

**Run Locally (Dry Run):**
```bash
# Dry run prints to console and saves reports, no Slack alert.
python src/main.py --dry-run
```

**Run Normally:**
```bash
python src/main.py
```

## GitHub Actions
The workflow is defined in `.github/workflows/daily.yml`.
- Runs Monday-Friday at market close (approx).
- You must set `SLACK_WEBHOOK_URL` in your GitHub Repository Secrets.

## Output
- **Slack**: Immediate alerts for matching tickers.
- **Artifacts**: `daily_report.json` and `daily_report.md` are saved (and uploaded on GitHub Actions).

## Exclusion Logic
- **Sector**: If `yfinance` data says "Health Care", it's excluded.
- **Keywords**: Checks for "biotech", "clinical", "trial" in sector/industry and PR headlines.
- **Denylist**: Add manual exclusions to `config/denylist_tickers.txt`.
