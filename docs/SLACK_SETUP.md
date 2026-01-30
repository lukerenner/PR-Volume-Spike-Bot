# Slack Configuration Guide

Based on the screenshot you provided, you are currently on the **Basic Information** page of your Slack App. You need to enable and create a Webhook URL.

## 1. Get the Webhook URL (Slack Side)

1.  **Look at the Left Sidebar** on the webpage you screenshotted.
2.  Under the **Features** section, click on **Incoming Webhooks**.
3.  Toggle the switch **Activate Incoming Webhooks** to **On**.
4.  Scroll down to the bottom of the page and click the button **Add New Webhook to Workspace**.
5.  Select the channel where you want the bot to post (e.g., `#alerts` or create a new one).
6.  Click **Allow**.
7.  You will be redirected back to the Incoming Webhooks page. Look for the **Webhook URL** column.
8.  Copy the URL. It should look like `https://hooks.slack.com/services/T-YOUR-WORKSPACE/B-YOUR-APP/TOKEN`.

## 2. Add Secret to GitHub (Repository Side)

1.  Go to your GitHub repository page.
2.  Click on **Settings** (top right tab).
3.  In the left sidebar, click **Secrets and variables** -> **Actions**.
4.  Click the green button **New repository secret**.
5.  **Name**: `SLACK_WEBHOOK_URL`
6.  **Secret**: Paste the URL you copied from Slack (starting with `https://hooks.slack.com/...`).
7.  Click **Add secret**.

Once this is done, the next scheduled run (or manual run) will be able to send messages!
