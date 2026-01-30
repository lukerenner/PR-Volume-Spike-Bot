import requests
import logging

logger = logging.getLogger(__name__)

class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def post_final_report(self, alerts: list):
        if not self.webhook_url or not alerts:
            return

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Here are the volume-spiking PRs of the day:*"
                }
            },
            {"type": "divider"}
        ]

        list_text = ""
        for a in alerts:
            # Format: • Company | Ticker | Volume Spike (3x) | Headline (Link)
            ticker = a['ticker']
            company = a.get('company_name', ticker)
            multiple = a['spike']['multiple']
            headline = a['pr'].headline
            url = a['pr'].url
            
            line = f"• {company} | *{ticker}* | {multiple}x Vol | <{url}|{headline}>\n"
            list_text += line

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": list_text
            }
        })
        
        try:
            requests.post(self.webhook_url, json={"blocks": blocks})
        except Exception as e:
            logger.error(f"Failed to post to Slack: {e}")

    def post_alert(self, *args, **kwargs): pass
    def post_summary(self, *args, **kwargs): pass
