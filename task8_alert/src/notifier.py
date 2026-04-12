import time

import requests


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, cooldown_min=1):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

        # State management for spam prevention
        self.cooldown_min = cooldown_min * 60
        self.last_sent_times = {}  # Dictionary to remember when we last sent an alert

    def send_alert(self, alert_message: str, alert_id: str = "GLOBAL") -> None:
        """alert_id allows us to track cooldowns separately."""
        current_time = time.time()

        # Check the cooldown timeout
        if alert_id in self.last_sent_times:
            time_since_last_alert = current_time - self.last_sent_times[alert_id]
            if time_since_last_alert < self.cooldown_min:
                return

        payload = {
            'chat_id': self.chat_id,
            'text': f"🚨 <b>CRITICAL SYSTEM ALERT</b> 🚨\n\n{alert_message}",
            'parse_mode': 'html',
        }
        try:
            response = requests.post(self.base_url, data=payload, timeout=3)
            response.raise_for_status()  # Raises an exception for HTTP errors
            self.last_sent_times[alert_id] = current_time
        except requests.exceptions.Timeout:
            print(f"Telegram API Timeout: Could not send message for {alert_id}")
        except requests.exceptions.HTTPError as e:
            print(f"Failed to send Telegram alert: {e}")
