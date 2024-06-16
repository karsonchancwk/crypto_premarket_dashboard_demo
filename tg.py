import requests
import pytz

__all__ = (
    'TGSuperGroupBot',
)


class TGSuperGroupBot:
    def __init__(self, token: str, chat_id: str, message_thread_id: int = None):
        self.base_url = 'https://api.telegram.org'
        self._token = token
        self.chat_id = chat_id
        self.message_thread_id = message_thread_id

    def send_message(self, text: str, disable_notification=False):
        params = {
            'chat_id': self.chat_id,
            'message_thread_id': self.message_thread_id,
            'text': text,
            'disable_notification': disable_notification
        }

        res = requests.post(
            f'{self.base_url}/bot{self._token}/sendMessage', data=params)

        return res

    def send_signal(self, strategy: str, signal: int, agg_signal: float):

        for key in signal.keys():

            signal_text = f"""
            Strategy {strategy}
            ==============
            Params: {key}
            timestamp: {signal[key][6].astimezone
                        (pytz.timezone('Asia/Hong_Kong')).strftime('%Y-%m-%d %H:%M:%S (UTC+8)')}
            signal: {signal[key][0]:.1f}
            weighted: {signal[key][5]:.3f}
            z_score: {signal[key][2]:.3f}
            value: {signal[key][3]:.5f}
            agg signal: {agg_signal:.3f}
            ==============
            """

            params = {
                'chat_id': self.chat_id,
                'message_thread_id': self.message_thread_id,
                'text': signal_text,
                'disable_notification': False
            }

            res = requests.post(
                f'{self.base_url}/bot{self._token}/sendMessage', data=params)

        return res
