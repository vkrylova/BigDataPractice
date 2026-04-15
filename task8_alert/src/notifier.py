import time

import requests


class TelegramNotifier:
    """
    Sends alert messages to Telegram with a cooldown to prevent spam.
    """

    def __init__(self, bot_token: str, chat_id: str, cooldown_min=1):
        """
        Initializes the notifier.

        Args:
            bot_token: Telegram bot token.
            chat_id: Target chat ID.
            cooldown_min: Minimum time (in minutes) between alerts with the same ID.
        """

        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

        # State management for spam prevention
        self.cooldown_min = cooldown_min * 60
        # Dictionary to remember when we last sent an alert
        self.last_sent_times = {}

    def send_alert(self, alert_message: str, alert_id: str = "GLOBAL") -> None:
        """
        Sends an alert message to Telegram if cooldown allows.

        Args:
            alert_message: Text of the alert.
            alert_id: Identifier to track cooldown per alert type.

        Returns:
            None.
        """

        current_time = time.time()

        # Check the cooldown timeout
        if alert_id in self.last_sent_times:
            time_since_last_alert = current_time - self.last_sent_times[alert_id]
            if time_since_last_alert < self.cooldown_min:
                return

        payload = {
            "chat_id": self.chat_id,
            "text": f"🚨 <b>CRITICAL SYSTEM ALERT</b> 🚨\n\n{alert_message}",
            "parse_mode": "html",
        }

        try:
            response = requests.post(self.base_url, json=payload, timeout=10)
            response.raise_for_status()  # Raises an exception for HTTP errors
            self.last_sent_times[alert_id] = current_time
        except requests.exceptions.Timeout:
            print(f"Telegram API Timeout: Could not send message for {alert_id}")
        except requests.exceptions.HTTPError as e:
            print(f"Telegram Server Response: {e.response.text}")
