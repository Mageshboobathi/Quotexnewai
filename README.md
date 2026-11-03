# Quotex Remix AI V3

## Deploy
1. Replace the repository's `app.py` and `requirements.txt` with these files.
2. In Streamlit Community Cloud, set App settings → Secrets:
   `TWELVE_DATA_API_KEY = "your_key"`
3. Reboot the app.

## Free Twelve Data limitations
Basic has 8 API credits per minute (800/day) and up to 8 trial WebSocket symbol subscriptions. It does **not** provide unrestricted all-market WebSocket access. The requested symbols must be in Twelve Data's current trial-symbol list. This app lets you enter up to 8 symbols and shows subscription errors when a symbol is not permitted.

Live 1-minute OHLC candles are constructed from incoming price ticks. Initial history is fetched once for the selected chart symbol. This is market-data analysis only, not a guaranteed prediction or automated trading system.
