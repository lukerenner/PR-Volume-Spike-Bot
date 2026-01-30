import requests
import os
import json
import logging

logger = logging.getLogger(__name__)

class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def post_alert(self, ticker: str, company: str, spike_data: dict, pr_item: any, links: dict):
        if not self.webhook_url:
            logger.warning("No Slack webhook provided. Skipping alert.")
            return

        # Format message
        # spike_data has: volume_today, volume_median, multiple, price_close, pct_change
        
        # Color coding: Green if price up, Red if down
        color = "#36a64f" if spike_data['pct_change'] >= 0 else "#ff0000"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 {ticker} — {company or 'Unknown'}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Volume Multiplier:*\n{spike_data['multiple']}x"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Price Change:*\n{spike_data['pct_change']}%"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Volume:*\n{spike_data['volume_today']:,}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Median Vol (20d):*\n{spike_data['volume_median']:,}"
                    }
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*PR:* <{pr_item.url}|{pr_item.headline}>\n_{pr_item.source} — {pr_item.published_at.strftime('%Y-%m-%d %H:%M')}_"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"<{links['quote']}|Yahoo Quote> | <{links['chart']}|Chart>"
                    }
                ]
            }
        ]

        payload = {
            "blocks": blocks,
            "attachments": [
                {
                    "color": color,
                    "blocks": [] # Color strip only
                }
            ]
        }

        try:
            resp = requests.post(self.webhook_url, json=payload)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to post to Slack: {e}")

    def post_summary(self, stats: dict):
        if not self.webhook_url:
            return
            
        text = f"📊 *Daily Summary*: scanned {stats['scanned']} tickers. Found {stats['spikes']} volume spikes. Sent {stats['alerts']} alerts (excluded {stats['excluded']} pharma)."
        
        try:
            requests.post(self.webhook_url, json={"text": text})
        except Exception:
            pass
