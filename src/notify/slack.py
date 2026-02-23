import requests
import logging
import datetime
import pytz

logger = logging.getLogger(__name__)

HEADER_EMOJI = {
    "Morning": ":sunrise:",
    "Midday":  ":sun_with_face:",
    "Closing": ":bell:",
}

def _fmt_mcap(mcap) -> str:
    """Format market cap as $XM or $XB, or '?' if unknown."""
    if mcap is None:
        return "?"
    if mcap >= 1_000_000_000:
        return f"${mcap/1_000_000_000:.1f}B"
    return f"${mcap/1_000_000:.0f}M"

class SlackNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def post_final_report(self, alerts: list, run_label: str = ""):
        if not self.webhook_url or not alerts:
            return

        emoji = HEADER_EMOJI.get(run_label, ":chart_with_upwards_trend:")
        now_et = datetime.datetime.now(pytz.timezone('US/Eastern'))
        time_str = now_et.strftime("%-I:%M %p ET")
        n = len(alerts)

        header = (
            f"{emoji} *{run_label} Volume Spikes* — {time_str}  "
            f"({n} alert{'s' if n != 1 else ''})"
        )

        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": header}},
            {"type": "divider"},
        ]

        for a in alerts:
            ticker   = a['ticker']
            company  = a.get('company_name', ticker)
            multiple = a['spike']['multiple']
            pct      = a['spike'].get('pct_change', 0)
            vol_today = a['spike'].get('volume_today', 0)
            mcap     = _fmt_mcap(a.get('market_cap'))
            sector   = a.get('sector') or "—"
            headline = a['pr'].headline
            url      = a['pr'].url
            source   = a['pr'].source

            pct_str = f"+{pct:.1f}%" if pct >= 0 else f"{pct:.1f}%"
            vol_str = f"{vol_today:,}"

            text = (
                f"*<{url}|{ticker}>* — {company}\n"
                f"  Volume: *{multiple}x* ({vol_str} shares) | Price: *{pct_str}* | Cap: {mcap} | {sector}\n"
                f"  _{source}:_ {headline}"
            )

            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": text},
            })
            blocks.append({"type": "divider"})

        try:
            resp = requests.post(self.webhook_url, json={"blocks": blocks})
            resp.raise_for_status()
            logger.info(f"Slack post sent ({run_label}, {n} alerts)")
        except Exception as e:
            logger.error(f"Failed to post to Slack: {e}")

    def post_status(self, stats: dict, run_label: str = ""):
        """Post a brief status message when the bot runs but finds no alerts.
        Helps confirm the bot is alive and shows scan stats."""
        if not self.webhook_url:
            return
        now_et = datetime.datetime.now(pytz.timezone('US/Eastern'))
        time_str = now_et.strftime("%-I:%M %p ET")
        candidates = stats.get('pr_candidates', 0)
        scanned = stats.get('scanned', 0)
        text = (
            f":white_check_mark: *{run_label} Scan Complete* — {time_str}\n"
            f"  PR candidates: {candidates} | Scanned: {scanned} | Alerts: 0\n"
            f"  _No PR-driven volume spikes met threshold today._"
        )
        try:
            resp = requests.post(self.webhook_url, json={"text": text})
            resp.raise_for_status()
            logger.info(f"Slack status posted ({run_label}, 0 alerts)")
        except Exception as e:
            logger.error(f"Failed to post Slack status: {e}")

    def post_alert(self, *args, **kwargs): pass
    def post_summary(self, *args, **kwargs): pass
