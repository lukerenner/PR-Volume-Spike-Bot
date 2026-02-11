import requests
import logging
import datetime
import pytz

logger = logging.getLogger(__name__)

HEADER_EMOJI = {
    "Morning": ":sunrise:",
    "Midday": ":sun_with_face:",
    "Closing": ":bell:",
}

class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def post_final_report(self, alerts: list, run_label: str = ""):
        if not self.webhook_url or not alerts:
            return

        emoji = HEADER_EMOJI.get(run_label, ":chart_with_upwards_trend:")
        now_et = datetime.datetime.now(pytz.timezone('US/Eastern'))
        time_str = now_et.strftime("%-I:%M %p ET")

        header = f"{emoji} *{run_label} Volume Spikes* — {time_str}  ({len(alerts)} alert{'s' if len(alerts) != 1 else ''})"

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": header
                }
            },
            {"type": "divider"}
        ]

        list_text = ""
        for a in alerts:
            ticker = a['ticker']
            company = a.get('company_name', ticker)
            multiple = a['spike']['multiple']
            pct = a['spike'].get('pct_change', 0)
            headline = a['pr'].headline
            url = a['pr'].url

            pct_str = f"+{pct}%" if pct >= 0 else f"{pct}%"
            line = f"• *{ticker}* ({company}) | {multiple}x Vol | {pct_str} | <{url}|{headline}>\n"
            list_text += line

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": list_text
            }
        })

        try:
            resp = requests.post(self.webhook_url, json={"blocks": blocks})
            resp.raise_for_status()
            logger.info(f"Slack post sent ({run_label}, {len(alerts)} alerts)")
        except Exception as e:
            logger.error(f"Failed to post to Slack: {e}")

    def post_alert(self, *args, **kwargs): pass
    def post_summary(self, *args, **kwargs): pass
