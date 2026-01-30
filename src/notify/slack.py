import requests
import os
import json
import logging

logger = logging.getLogger(__name__)

class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def post_final_report(self, alerts: list):
        if not self.webhook_url:
            logger.warning("No Slack webhook provided. Skipping alert.")
            return

        if not alerts:
            # Optional: Post "No alerts today" or just stay silent?
            # User probably only wants noise if there is news.
            logger.info("No alerts to post.")
            return

        # Format:
        # Here are the volume-spiking PRs of the day:
        # • Company | Headline (Link)
        
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Here are the volume-spiking PRs of the day:*"
                }
            },
            {
                "type": "divider"
            }
        ]

        # Use bullet points
        # If list is too long (Slack limit 50 blocks), we might need to chunk or just put in one text block.
        # A single section text block can hold ~3000 chars. That's safer for 10-20 alerts.
        
        list_text = ""
        for a in alerts:
            # a = {'ticker': 'XYZ', 'company': 'XYZ Inc', 'spike': {...}, 'pr': PRItem object}
            # PRItem needs to be accessed. In main it was converted to dict for report, 
            # but we can pass the raw object or dict. Let's assume dict or object handle.
            
            ticker = a['ticker']
            headline = a['pr'].headline
            url = a['pr'].url
            # company name might be just ticker if we didn't fetch full name
            # User asked for "Company", let's use Ticker for clarity if Name unavailable
            
            # Format: • Ticker | <URL|Headline>
            line = f"• *{ticker}* | <{url}|{headline}>\n"
            list_text += line

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": list_text
            }
        })
        
        payload = {"blocks": blocks}

        try:
            resp = requests.post(self.webhook_url, json=payload)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to post to Slack: {e}")

    def post_alert(self, *args, **kwargs):
        # Deprecated for single message preference
        pass

    def post_summary(self, *args, **kwargs):
        # Deprecated logic, maybe keep for debug log?
        pass
