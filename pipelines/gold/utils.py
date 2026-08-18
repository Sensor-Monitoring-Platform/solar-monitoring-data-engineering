from datetime import datetime, timezone

def log(message):
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}]: {message}")