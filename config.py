class StatusIndicator:
    RUNNING = "▶️"
    PAUSED = "⏸️"
    STOPPED = "⏹️"

def format_duration(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"