"""
Colorful Terminal UI for Polymarket Konis Bot

Provides rich terminal output with colors, tables, and status displays.
Uses ANSI escape codes for cross-platform color support.
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

# ============ Timezone Configuration ============
# Load from env with defaults
DISPLAY_TZ_OFFSET = int(os.getenv("DISPLAY_TIMEZONE_OFFSET", "0"))
DISPLAY_TZ_NAME = os.getenv("DISPLAY_TIMEZONE_NAME", "UTC")

def get_display_tz():
    """Get timezone for display (from DISPLAY_TIMEZONE_OFFSET env)"""
    return timezone(timedelta(hours=DISPLAY_TZ_OFFSET))

def format_time_display(ts: float, fmt: str = "%H:%M") -> str:
    """Format timestamp in display timezone"""
    if ts <= 0:
        return "--:--"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    display_dt = dt.astimezone(get_display_tz())
    return display_dt.strftime(fmt)

def now_display() -> datetime:
    """Get current time in display timezone"""
    return datetime.now(get_display_tz())


# ============ ANSI Color Codes ============
class Colors:
    """ANSI color codes for terminal output"""
    # Reset
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"

    # Regular colors
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    # Bright colors
    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"

    # Background colors
    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"


# ============ TTY Detection ============
# Check if we're running in an interactive terminal
# FORCE_TERMINAL_UI=1 overrides detection (useful for tmux/screen)
def _is_terminal() -> bool:
    """Check if stdout is a terminal (TTY)"""
    # Allow forcing terminal mode via env var
    force = os.getenv("FORCE_TERMINAL_UI", "").lower()
    if force in ("1", "true", "yes"):
        return True
    if force in ("0", "false", "no"):
        return False
    # Auto-detect
    return sys.stdout.isatty()

IS_TERMINAL = _is_terminal()

# Enable ANSI on Windows
if sys.platform == "win32":
    os.system("")  # Enable ANSI escape sequences


def c(text: str, *colors: str) -> str:
    """Apply colors to text. Returns plain text if not in terminal."""
    if not IS_TERMINAL:
        return str(text)
    return "".join(colors) + str(text) + Colors.RESET


def bold(text: str) -> str:
    return c(text, Colors.BOLD)


def dim(text: str) -> str:
    return c(text, Colors.DIM)


def green(text: str) -> str:
    return c(text, Colors.GREEN)


def red(text: str) -> str:
    return c(text, Colors.RED)


def yellow(text: str) -> str:
    return c(text, Colors.YELLOW)


def cyan(text: str) -> str:
    return c(text, Colors.CYAN)


def blue(text: str) -> str:
    return c(text, Colors.BLUE)


def magenta(text: str) -> str:
    return c(text, Colors.MAGENTA)


def bright_green(text: str) -> str:
    return c(text, Colors.BRIGHT_GREEN)


def bright_red(text: str) -> str:
    return c(text, Colors.BRIGHT_RED)


def bright_yellow(text: str) -> str:
    return c(text, Colors.BRIGHT_YELLOW)


def bright_cyan(text: str) -> str:
    return c(text, Colors.BRIGHT_CYAN)


def bright_blue(text: str) -> str:
    return c(text, Colors.BRIGHT_BLUE)


def bright_white(text: str) -> str:
    return c(text, Colors.BRIGHT_WHITE)


# ============ Symbol Helpers ============
class _Symbols:
    """Unicode symbols for terminal display - ASCII fallback if not in terminal"""
    @staticmethod
    def _get(unicode_char: str, ascii_char: str) -> str:
        return unicode_char if IS_TERMINAL else ascii_char

    @property
    def CHECK(self): return self._get("✓", "[OK]")
    @property
    def CROSS(self): return self._get("✗", "[X]")
    @property
    def ARROW_UP(self): return self._get("▲", "^")
    @property
    def ARROW_DOWN(self): return self._get("▼", "v")
    @property
    def ARROW_RIGHT(self): return self._get("→", "->")
    @property
    def BULLET(self): return self._get("●", "*")
    @property
    def CIRCLE(self): return self._get("○", "o")
    @property
    def SQUARE(self): return self._get("■", "#")
    @property
    def DIAMOND(self): return self._get("◆", "<>")
    @property
    def STAR(self): return self._get("★", "*")
    @property
    def CLOCK(self): return self._get("◷", "[T]")
    @property
    def MONEY(self): return "$"
    @property
    def CHART(self): return self._get("≡", "[~]")
    @property
    def ROCKET(self): return self._get("»", "[!]")
    @property
    def WARNING(self): return self._get("⚠", "[!]")
    @property
    def INFO(self): return self._get("ℹ", "[i]")
    @property
    def FIRE(self): return self._get("*", "[*]")
    @property
    def PROFIT(self): return self._get("$", "[$]")
    @property
    def LOSS(self): return self._get("💸", "[-]")

# Create singleton instance
Symbols = _Symbols()


def pnl_color(value: float, as_percent: bool = False) -> str:
    """Color PnL value based on positive/negative"""
    if as_percent:
        text = f"{value:+.2%}"
    else:
        text = f"${value:+.2f}"

    if value > 0:
        return bright_green(text)
    elif value < 0:
        return bright_red(text)
    else:
        return dim(text)


def direction_color(direction: str) -> str:
    """Color direction (UP/DOWN/YES/NO) - normalizes to YES/NO"""
    d = direction.upper()
    if d in ("UP", "YES"):
        return bright_green(f"{Symbols.ARROW_UP} YES")
    elif d in ("DOWN", "NO"):
        return bright_red(f"{Symbols.ARROW_DOWN} NO")
    else:
        return yellow(d)


def confidence_color(confidence: float) -> str:
    """Color confidence level"""
    pct = f"{confidence:.0%}"
    if confidence >= 0.80:
        return bright_green(pct)
    elif confidence >= 0.70:
        return bright_yellow(pct)
    elif confidence >= 0.60:
        return yellow(pct)
    else:
        return dim(pct)


def status_badge(status: str) -> str:
    """Create colored status badge"""
    s = status.upper()
    if s == "OPEN":
        return c(f" {Symbols.BULLET} OPEN ", Colors.BOLD, Colors.BG_GREEN, Colors.WHITE)
    elif s == "WAITING":
        return c(f" {Symbols.CIRCLE} WAIT ", Colors.BOLD, Colors.BG_YELLOW, Colors.BLACK)
    elif s == "CLOSED":
        return c(f" {Symbols.CHECK} DONE ", Colors.BOLD, Colors.BG_BLUE, Colors.WHITE)
    elif s == "ERROR":
        return c(f" {Symbols.CROSS} ERR  ", Colors.BOLD, Colors.BG_RED, Colors.WHITE)
    else:
        return c(f" {s} ", Colors.DIM)


def format_timestamp(ts: float) -> str:
    """Format timestamp to readable time in display timezone"""
    if ts <= 0:
        return dim("--:--:--")
    return cyan(format_time_display(ts, "%H:%M:%S"))


def format_duration(seconds: float) -> str:
    """Format duration in human readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


# ============ Box Drawing ============
class _Box:
    """Box drawing characters - uses ASCII fallback if not in terminal"""
    @staticmethod
    def _get(unicode_char: str, ascii_char: str) -> str:
        return unicode_char if IS_TERMINAL else ascii_char

    @property
    def H(self): return self._get("─", "-")
    @property
    def V(self): return self._get("│", "|")
    @property
    def TL(self): return self._get("┌", "+")
    @property
    def TR(self): return self._get("┐", "+")
    @property
    def BL(self): return self._get("└", "+")
    @property
    def BR(self): return self._get("┘", "+")
    @property
    def ML(self): return self._get("├", "+")
    @property
    def MR(self): return self._get("┤", "+")
    @property
    def MT(self): return self._get("┬", "+")
    @property
    def MB(self): return self._get("┴", "+")
    @property
    def X(self): return self._get("┼", "+")
    @property
    def DH(self): return self._get("═", "=")
    @property
    def DV(self): return self._get("║", "|")
    @property
    def DTL(self): return self._get("╔", "+")
    @property
    def DTR(self): return self._get("╗", "+")
    @property
    def DBL(self): return self._get("╚", "+")
    @property
    def DBR(self): return self._get("╝", "+")

# Create singleton instance
Box = _Box()


def box_line(width: int, left: str = Box.H, mid: str = Box.H, right: str = Box.H) -> str:
    """Create a box line"""
    return left + mid * (width - 2) + right


def box_top(width: int) -> str:
    return cyan(Box.TL + Box.H * (width - 2) + Box.TR)


def box_bottom(width: int) -> str:
    return cyan(Box.BL + Box.H * (width - 2) + Box.BR)


def box_middle(width: int) -> str:
    return cyan(Box.ML + Box.H * (width - 2) + Box.MR)


def box_row(content: str, width: int) -> str:
    """Create a boxed row with content"""
    content_len = len(strip_ansi(content))
    padding = width - 4 - content_len
    if padding < 0:
        padding = 0
    return cyan(Box.V) + " " + content + " " * padding + " " + cyan(Box.V)


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text"""
    import re
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)


# ============ Dashboard Components ============

def print_header(instance_id: str, width: int = 72):
    """Print colorful header"""
    print()
    print(cyan(Box.DTL + Box.DH * (width - 2) + Box.DTR))
    print(cyan(Box.DV) + " " * (width - 2) + cyan(Box.DV))

    # ASCII Art KONIS
    art = [
        "██╗  ██╗ ██████╗ ███╗   ██╗██╗███████╗",
        "██║ ██╔╝██╔═══██╗████╗  ██║██║██╔════╝",
        "█████╔╝ ██║   ██║██╔██╗ ██║██║███████╗",
        "██╔═██╗ ██║   ██║██║╚██╗██║██║╚════██║",
        "██║  ██╗╚██████╔╝██║ ╚████║██║███████║",
        "╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝╚══════╝",
    ]
    for line in art:
        padding = (width - 2 - len(line)) // 2
        print(cyan(Box.DV) + " " * padding + bright_cyan(line) + " " * (width - 2 - padding - len(line)) + cyan(Box.DV))

    print(cyan(Box.DV) + " " * (width - 2) + cyan(Box.DV))

    subtitle = "Confidence-Based KoNiS Polymarket Trading Bot"
    padding = (width - 2 - len(subtitle)) // 2
    print(cyan(Box.DV) + " " * padding + bold(subtitle) + " " * (width - 2 - padding - len(subtitle)) + cyan(Box.DV))

    print(cyan(Box.DV) + " " * (width - 2) + cyan(Box.DV))

    # Instance ID
    instance_text = f"Instance: {instance_id}"
    padding = (width - 2 - len(instance_text)) // 2
    print(cyan(Box.DV) + " " * padding + dim(instance_text) + " " * (width - 2 - padding - len(instance_text)) + cyan(Box.DV))

    print(cyan(Box.DBL + Box.DH * (width - 2) + Box.DBR))
    print()


def print_header_compact(instance_id: str, width: int = 72):
    """Print compact KONIS header for dashboard refresh - blue style"""
    # KONIS ASCII Art - same as print_header but blue theme
    art = [
        "██╗  ██╗ ██████╗ ███╗   ██╗██╗███████╗",
        "██║ ██╔╝██╔═══██╗████╗  ██║██║██╔════╝",
        "█████╔╝ ██║   ██║██╔██╗ ██║██║███████╗",
        "██╔═██╗ ██║   ██║██║╚██╗██║██║╚════██║",
        "██║  ██╗╚██████╔╝██║ ╚████║██║███████║",
        "╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝╚══════╝",
    ]

    print(blue(Box.DTL + Box.DH * (width - 2) + Box.DTR))
    for line in art:
        padding = (width - 2 - len(line)) // 2
        print(blue(Box.DV) + " " * padding + bright_cyan(line) + " " * (width - 2 - padding - len(line)) + blue(Box.DV))

    # Subtitle
    subtitle = "Confidence-Based Trading Bot"
    padding = (width - 2 - len(subtitle)) // 2
    print(blue(Box.DV) + " " * padding + bold(subtitle) + " " * (width - 2 - padding - len(subtitle)) + blue(Box.DV))

    # Instance ID
    instance_text = f"Instance: {instance_id}"
    padding = (width - 2 - len(instance_text)) // 2
    print(blue(Box.DV) + " " * padding + dim(instance_text) + " " * (width - 2 - padding - len(instance_text)) + blue(Box.DV))

    print(blue(Box.DBL + Box.DH * (width - 2) + Box.DBR))


def print_header_flash(instance_id: str, subtitle: str = "Polymarket Scalping Bot", width: int = 72):
    """Print KONIS FLASH header - blocky pixel-art style for scalping bot"""
    print()
    print(cyan(Box.DTL + Box.DH * (width - 2) + Box.DTR))
    print(cyan(Box.DV) + " " * (width - 2) + cyan(Box.DV))

    # KONIS FLASH ASCII Art - matching screenshot pixel style
    art = [
        "█   █  ███  █   █ █ ███   ████ █      ███  ███ █   █",
        "█  █  █   █ ██  █ █ █    █     █     █   █ █   █   █",
        "███   █   █ █ █ █ █  █   ████  █     █████  █  █████",
        "█  █  █   █ █  ██ █   █  █     █     █   █   █ █   █",
        "█   █  ███  █   █ █ ███  █     █████ █   █ ███ █   █",
    ]
    for line in art:
        padding = (width - 2 - len(line)) // 2
        print(cyan(Box.DV) + " " * padding + bright_cyan(line) + " " * (width - 2 - padding - len(line)) + cyan(Box.DV))

    print(cyan(Box.DV) + " " * (width - 2) + cyan(Box.DV))

    # Subtitle
    padding = (width - 2 - len(subtitle)) // 2
    print(cyan(Box.DV) + " " * padding + bold(subtitle) + " " * (width - 2 - padding - len(subtitle)) + cyan(Box.DV))

    print(cyan(Box.DV) + " " * (width - 2) + cyan(Box.DV))

    # Instance ID
    instance_text = f"Instance: {instance_id}"
    padding = (width - 2 - len(instance_text)) // 2
    print(cyan(Box.DV) + " " * padding + dim(instance_text) + " " * (width - 2 - padding - len(instance_text)) + cyan(Box.DV))

    print(cyan(Box.DBL + Box.DH * (width - 2) + Box.DBR))
    print()


def print_config_section(config: Dict[str, Any], width: int = 72):
    """Print configuration section"""
    print(box_top(width))
    title = " ⚙ CONFIGURATION "
    padding = (width - 2 - len(title)) // 2
    print(cyan(Box.V) + " " * padding + bold(yellow(title)) + " " * (width - 2 - padding - len(title)) + cyan(Box.V))
    print(box_middle(width))

    # Two column layout
    col1 = [
        (f"Trade Unit:", f"${config.get('trade_unit', 0):.2f}"),
        (f"Max Concurrent:", f"{config.get('max_trades', 0)}"),
        (f"Entry Window:", f"{config.get('entry_delay', 0)}m - {config.get('entry_cutoff', 0)}m"),
        (f"Min Confidence:", f"{config.get('min_confidence', 0):.0%}"),
    ]

    col2 = [
        (f"Take Profit:", bright_green(f"+{config.get('pct_profit', 0):.1%}")),
        (f"Stop Loss:", bright_red(f"{config.get('pct_loss', 0):.1%}")),
        (f"Signature:", f"{config.get('sig_type', 'EOA')}"),
        (f"Balance:", bright_green(f"${config.get('balance', 0):.2f}")),
    ]

    for (l1, v1), (l2, v2) in zip(col1, col2):
        left = f"{dim(l1)} {v1}"
        right = f"{dim(l2)} {v2}"
        left_len = len(strip_ansi(left))
        right_len = len(strip_ansi(right))
        mid_padding = width - 6 - left_len - right_len
        print(cyan(Box.V) + "  " + left + " " * mid_padding + right + "  " + cyan(Box.V))

    print(box_bottom(width))
    print()


def print_status_display(
    status: str,
    prediction: Optional[str],
    confidence: float,
    elapsed_minutes: float,
    entry_side: Optional[str],
    position_pnl: Optional[float],
    position_value: Optional[float],
    window_ts: int,
    width: int = 72
):
    """Print real-time status display"""
    print(box_top(width))

    # Status row
    badge = status_badge(status)
    window_time = format_time_display(window_ts, "%H:%M") + f" {DISPLAY_TZ_NAME}" if window_ts > 0 else "--:--"
    elapsed_str = f"{elapsed_minutes:.1f}m / 15m"

    status_row = f"{badge}  {dim('Window:')} {cyan(window_time)}  {dim('Elapsed:')} {yellow(elapsed_str)}"
    print(box_row(status_row, width))

    print(box_middle(width))

    # Prediction row
    if prediction:
        pred_display = direction_color(prediction)
        conf_display = confidence_color(confidence)
        pred_row = f"{dim('Prediction:')} {pred_display}  {dim('Confidence:')} {conf_display}"
    else:
        pred_row = dim("Waiting for prediction signal...")
    print(box_row(pred_row, width))

    # Position row (if open)
    if status == "OPEN" and entry_side:
        side_display = direction_color(entry_side)
        pnl_display = pnl_color(position_pnl or 0, as_percent=True)
        value_display = f"${position_value or 0:.2f}"
        pos_row = f"{dim('Position:')} {side_display}  {dim('PnL:')} {pnl_display}  {dim('Value:')} {bright_cyan(value_display)}"
        print(box_row(pos_row, width))

    print(box_bottom(width))


def print_stats_dashboard(
    total_trades: int,
    winning_trades: int,
    total_pnl: float,
    max_trades: int,  # Now means max concurrent positions (kept for backward compat)
    session_start: float,
    current_balance: float,
    width: int = 72
):
    """Print stats dashboard"""
    current_time = now_display().strftime(f"%H:%M:%S {DISPLAY_TZ_NAME}")

    print()
    print(box_top(width))
    title = f" 📊 SESSION STATS  [{current_time}] "
    padding = (width - 2 - len(title)) // 2
    print(cyan(Box.V) + " " * padding + bold(bright_cyan(title)) + " " * (width - 2 - padding - len(title)) + cyan(Box.V))
    print(box_middle(width))

    # Calculate stats
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
    losing_trades = total_trades - winning_trades
    session_duration = format_duration(time.time() - session_start) if session_start > 0 else "--"

    # Row 1: Trades (no longer show X/max since max is now concurrent, not total)
    trades_display = bright_cyan(str(total_trades))
    row1 = f"  {dim('Trades:')} {trades_display}   {dim('Wins:')} {bright_green(str(winning_trades))}   {dim('Losses:')} {bright_red(str(losing_trades))}   {dim('Win Rate:')} "
    if win_rate >= 50:
        row1 += bright_green(f"{win_rate:.1f}%")
    else:
        row1 += bright_red(f"{win_rate:.1f}%")
    print(box_row(row1, width))

    # Row 2: PnL & Balance
    pnl_display = pnl_color(total_pnl)
    balance_display = bright_green(f"${current_balance:.2f}")
    row2 = f"  {dim('Total PnL:')} {pnl_display}   {dim('Balance:')} {balance_display}   {dim('Duration:')} {cyan(session_duration)}"
    print(box_row(row2, width))

    print(box_bottom(width))


def _get_trade_attr(trade: Any, key: str, default: Any = None) -> Any:
    """Get attribute from trade (works with both dicts and objects)"""
    if isinstance(trade, dict):
        return trade.get(key, default)
    return getattr(trade, key, default)


def print_trade_history(trades: List[Any], width: int = 100):
    """Print trade history table with entry/exit prices, times, PnL % and PnL $"""
    # Get current datetime for header
    now_str = now_display().strftime(f"%d-%b-%y %H:%M {DISPLAY_TZ_NAME}")

    print()
    print(box_top(width))
    title = f" 📜 TRADE HISTORY [{now_str}] "
    padding = (width - 2 - len(title)) // 2
    print(cyan(Box.V) + " " * padding + bold(magenta(title)) + " " * (width - 2 - padding - len(title)) + cyan(Box.V))
    print(box_middle(width))

    # Header with separate Entry and Exit columns + Market
    header = f"  {dim('#')}  {dim('Side')}  {dim('Market')}      {dim('Entry@')}      {dim('Entry$')}  {dim('Exit@')}  {dim('Exit$')}   {dim('PnL %')}    {dim('PnL $')}   {dim('Dur')}   {dim('Reason')}"
    print(box_row(header, width))
    print(box_middle(width))

    # Handle empty trades
    if not trades:
        empty_msg = dim("  No trades in last 24 hours")
        print(box_row(empty_msg, width))
        print(box_bottom(width))
        return

    # Get last 10 trades (already sorted by exit_time descending from mongo)
    recent_trades = trades[:10]

    # Trades - number from 10 to 1 (oldest to newest from top to bottom)
    total = len(recent_trades)
    for i, trade in enumerate(recent_trades):
        row_num = total - i  # 10, 9, 8... for display
        side_val = _get_trade_attr(trade, 'side') or _get_trade_attr(trade, 'outcome') or "?"
        side = direction_color(side_val)

        # Market - extract coin + timeframe (e.g., "btc-updown-15m-1774336500" → "BTC-15M")
        market_slug = _get_trade_attr(trade, 'market_slug', '') or ''
        if market_slug:
            import re as _re
            parts = market_slug.split('-')
            coin = parts[0].upper()[:4]
            timeframe = ""
            for p in parts[1:]:
                if _re.fullmatch(r'\d+[mhd]', p):
                    timeframe = p.upper()
                    break
            market_short = f"{coin}-{timeframe}" if timeframe else coin
        else:
            market_short = "---"

        # Format entry/exit times in display timezone
        entry_time_str = ""
        exit_time_str = ""
        entry_time = _get_trade_attr(trade, 'entry_time')
        exit_time = _get_trade_attr(trade, 'exit_time')
        if entry_time:
            entry_time_str = format_time_display(entry_time, "%d-%b %H:%M")
        if exit_time:
            exit_time_str = format_time_display(exit_time, "%H:%M")

        # Format prices as separate columns
        entry_price = _get_trade_attr(trade, 'entry_price', 0) or 0
        exit_price = _get_trade_attr(trade, 'exit_price', 0) or 0
        entry_price_str = f"{entry_price:.3f}" if entry_price > 0 else "---"
        exit_price_str = f"{exit_price:.3f}" if exit_price > 0 else "---"

        # PnL % (colored) - pnl_percent is already decimal (0.20 = 20%)
        pnl_pct = _get_trade_attr(trade, 'pnl_percent', 0) or 0
        pnl_pct_display = pnl_color(pnl_pct, as_percent=True)

        # PnL $ (colored)
        pnl_cash = _get_trade_attr(trade, 'pnl_cash') or _get_trade_attr(trade, 'pnl_usd', 0) or 0
        pnl_cash_display = pnl_color(pnl_cash, as_percent=False)

        # Duration
        duration = ""
        dur_secs = _get_trade_attr(trade, 'duration_secs')
        if dur_secs:
            duration = format_duration(dur_secs)
        elif entry_time and exit_time:
            duration = format_duration(exit_time - entry_time)

        reason = _get_trade_attr(trade, 'exit_reason', "") or ""
        status = _get_trade_attr(trade, 'status', "")
        # Show full reason (up to 25 chars for detailed exit info)
        reason_display = reason[:25] if reason else ""
        if status == "OPEN" or reason.upper() == "OPEN":
            reason_display = yellow("OPEN")
        elif reason and "profit" in reason.lower():
            reason_display = bright_green(reason_display)
        elif reason and ("loss" in reason.lower() or "stop" in reason.lower()):
            reason_display = bright_red(reason_display)
        elif reason and ("swap" in reason.lower() or "breakeven" in reason.lower()):
            reason_display = bright_yellow(reason_display)
        elif reason and ("cutoff" in reason.lower() or "time" in reason.lower()):
            reason_display = magenta(reason_display)
        elif reason and ("redeem" in reason.lower() or "resolution" in reason.lower()):
            reason_display = cyan(reason_display)
        elif reason and "sold" in reason.lower():
            reason_display = green(reason_display)
        reason = reason_display

        # Pad columns - separate Entry$ and Exit$ columns + Market
        row = f"  {dim(str(row_num).zfill(2))}  {side}  {magenta(market_short.ljust(8))} {cyan(entry_time_str.ljust(11))} {bright_cyan(entry_price_str.ljust(6))} {cyan(exit_time_str.ljust(6))} {bright_yellow(exit_price_str.ljust(6))} {pnl_pct_display}  {pnl_cash_display} {cyan(duration.ljust(5))} {reason}"
        print(box_row(row, width))

    print(box_bottom(width))


def print_entry_signal(prediction: str, confidence: float, open_count: int, max_concurrent: int):
    """Print entry signal notification"""
    print()
    print(bright_yellow("=" * 60))
    print(bright_yellow(f"  {Symbols.ROCKET} ENTRY SIGNAL DETECTED!"))
    print(f"  {dim('Prediction:')} {direction_color(prediction)}  {dim('Confidence:')} {confidence_color(confidence)}")
    print(f"  {dim('Open:')} {bright_cyan(str(open_count))}/{dim(str(max_concurrent))}")
    print(bright_yellow("=" * 60))
    print()


def print_entry_success(side: str, order_id: str, entry_price: float, amount: float):
    """Print entry success notification"""
    print()
    print(bright_green("┌" + "─" * 58 + "┐"))
    print(bright_green(f"│  {Symbols.CHECK} ORDER FILLED") + " " * 42 + bright_green("│"))
    print(bright_green("│") + f"  {dim('Side:')} {direction_color(side)}  {dim('Price:')} {cyan(f'{entry_price:.4f}')}  {dim('Amount:')} {bright_cyan(f'${amount:.2f}')}" + " " * 5 + bright_green("│"))
    print(bright_green("│") + f"  {dim('OrderID:')} {dim(order_id[:32])}..." + " " * 3 + bright_green("│"))
    print(bright_green("└" + "─" * 58 + "┘"))
    print()


def print_exit_success(side: str, pnl_percent: float, pnl_cash: float, reason: str):
    """Print exit success notification"""
    print()
    if pnl_percent >= 0:
        border_color = bright_green
        icon = Symbols.PROFIT
    else:
        border_color = bright_red
        icon = Symbols.LOSS

    print(border_color("┌" + "─" * 58 + "┐"))
    print(border_color(f"│  {icon} POSITION CLOSED") + " " * 39 + border_color("│"))
    print(border_color("│") + f"  {dim('Side:')} {direction_color(side)}  {dim('PnL:')} {pnl_color(pnl_percent, True)} ({pnl_color(pnl_cash)})" + " " * 10 + border_color("│"))
    print(border_color("│") + f"  {dim('Reason:')} {reason[:45]}" + " " * max(0, 45 - len(reason)) + border_color("│"))
    print(border_color("└" + "─" * 58 + "┘"))
    print()


def print_session_summary(
    instance_id: str,
    total_trades: int,
    winning_trades: int,
    total_pnl: float,
    trades: List[Any],
    session_duration: float,
    width: int = 72
):
    """Print final session summary"""
    print()
    print(cyan(Box.DTL + Box.DH * (width - 2) + Box.DTR))
    title = f" 🏁 SESSION COMPLETE - {instance_id} "
    padding = (width - 2 - len(strip_ansi(title))) // 2
    print(cyan(Box.DV) + " " * padding + bold(bright_cyan(title)) + " " * (width - 2 - padding - len(strip_ansi(title))) + cyan(Box.DV))
    print(cyan(Box.DV + " " * (width - 2) + Box.DV))

    # Stats
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0

    stats = [
        (f"Total Trades:", str(total_trades)),
        (f"Winning Trades:", bright_green(str(winning_trades))),
        (f"Losing Trades:", bright_red(str(total_trades - winning_trades))),
        (f"Win Rate:", bright_green(f"{win_rate:.1f}%") if win_rate >= 50 else bright_red(f"{win_rate:.1f}%")),
        (f"Total PnL:", pnl_color(total_pnl)),
        (f"Session Duration:", cyan(format_duration(session_duration))),
    ]

    for label, value in stats:
        row = f"  {dim(label)} {value}"
        print(cyan(Box.DV) + " " + row + " " * (width - 4 - len(strip_ansi(row))) + " " + cyan(Box.DV))

    print(cyan(Box.DV + " " * (width - 2) + Box.DV))
    print(cyan(Box.DBL + Box.DH * (width - 2) + Box.DBR))

    # Trade history
    if trades:
        print_trade_history(trades, width)

    print()


def print_candle_history(candles: List[Any], width: int = 80):
    """
    Print last 4 candle (1 hour) status history.
    Shows each 15m window's decision: ENTERED, SKIPPED, FAILED, etc.
    Now includes hedge position info when applicable.
    """
    if not candles:
        return

    print(box_top(width))
    title = f" {Symbols.CHART} CANDLE HISTORY (Last 1 Hour) "
    padding = (width - 2 - len(strip_ansi(title))) // 2
    print(cyan(Box.V) + " " * padding + bold(magenta(title)) + " " * (width - 2 - padding - len(strip_ansi(title))) + cyan(Box.V))
    print(box_middle(width))

    # Header - updated to include Hedge column
    header = f"  {'Time':<8} {'Status':<10} {'Pred':<6} {'Conf':<6} {'Hedge':<12} {'Reason/PnL':<28}"
    print(box_row(dim(header), width))
    print(box_middle(width))

    # Candles (most recent first)
    for candle in reversed(candles[-4:]):
        # Handle both dict and object
        if hasattr(candle, 'window_time_utc'):
            time_str = candle.window_time_utc[-10:-4] if len(candle.window_time_utc) > 10 else candle.window_time_utc  # Extract HH:MM UTC
            status = candle.status
            prediction = candle.prediction
            max_conf = candle.max_confidence
            reason = candle.skip_reason or candle.exit_reason or ""
            pnl_cash = getattr(candle, 'pnl_cash', 0)
            is_hedged = getattr(candle, 'is_hedged', False)
            hedge_side = getattr(candle, 'hedge_side', None)
            hedge_pnl_cash = getattr(candle, 'hedge_pnl_cash', 0)
        else:
            time_str = str(candle.get('window_time_utc', '--:--'))[-10:-4]
            status = candle.get('status', '?')
            prediction = candle.get('prediction')
            max_conf = candle.get('max_confidence', 0)
            reason = candle.get('skip_reason') or candle.get('exit_reason') or ""
            pnl_cash = candle.get('pnl_cash', 0)
            is_hedged = candle.get('is_hedged', False)
            hedge_side = candle.get('hedge_side')
            hedge_pnl_cash = candle.get('hedge_pnl_cash', 0)

        # Status color
        status_colors = {
            "ENTERED": bright_green,
            "CLOSED": green,
            "SKIPPED": yellow,
            "FAILED": bright_red,
            "WAITING": dim,
        }
        status_color = status_colors.get(status, dim)
        status_display = status_color(status.ljust(10))

        # Prediction display (shorter)
        if prediction in ("UP", "DOWN"):
            pred_display = direction_color(prediction).ljust(6)
        else:
            pred_display = dim(str(prediction or "---").ljust(6))

        # Confidence display (shorter)
        if max_conf > 0:
            conf_display = confidence_color(max_conf).ljust(6)
        else:
            conf_display = dim("---").ljust(6)

        # Hedge display - new column
        if is_hedged and hedge_side:
            hedge_display = f"{magenta(hedge_side)} {pnl_color(hedge_pnl_cash)}"
        else:
            hedge_display = dim("---").ljust(12)

        # Reason (truncate if too long)
        reason_display = reason[:28] if reason else ""
        if status == "CLOSED" and pnl_cash != 0:
            reason_display = f"PnL: {pnl_color(pnl_cash)}"

        row = f"  {cyan(time_str):<8} {status_display} {pred_display} {conf_display} {hedge_display:<12} {reason_display}"
        print(box_row(row, width))

    print(box_bottom(width))


def clear_screen():
    """Clear terminal screen and move cursor to top"""
    print("\033[2J\033[H", end="")


def clear_line():
    """Clear current line"""
    print("\033[2K\033[1G", end="")


def move_cursor_up(lines: int = 1):
    """Move cursor up N lines"""
    print(f"\033[{lines}A", end="")


def move_cursor_down(lines: int = 1):
    """Move cursor down N lines"""
    print(f"\033[{lines}B", end="")


def move_cursor_to(row: int, col: int = 1):
    """Move cursor to specific row/column (1-indexed)"""
    print(f"\033[{row};{col}H", end="")


def clear_from_cursor():
    """Clear from cursor to end of screen"""
    print("\033[J", end="")


def save_cursor():
    """Save cursor position"""
    print("\033[s", end="")


def restore_cursor():
    """Restore cursor position"""
    print("\033[u", end="")


# Need time module for session duration
import time
from collections import deque


# ============ Scalping Bot Dashboard Components ============

class ScalpingDashboard:
    """
    Full-screen dashboard for Scalping Bot.
    Displays: Logo, Status bar, Session Stats, Trade History, Candle History, Session Logs
    """

    def __init__(
        self,
        instance_id: str = "",
        entry_price: float = 0.57,
        exit_price: float = 0.67,
        take_profit_pct: float = 0.10,
        stop_loss_pct: float = -0.50,
        trade_unit: float = 20.0,
        max_logs: int = 50,  # V3.78: Increased from 25 for web dashboard visibility
        max_trades: int = 50,
        max_candles: int = 4
    ):
        self.instance_id = instance_id
        self.entry_price = entry_price
        self.exit_price = exit_price
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.trade_unit = trade_unit

        self.max_logs = max_logs
        self.max_trades = max_trades
        self.max_candles = max_candles

        # Session logs (circular buffer)
        self.logs: deque = deque(maxlen=max_logs)

        # Trade history
        self.trades: List[Dict[str, Any]] = []

        # Candle history (15m windows)
        self.candles: List[Dict[str, Any]] = []

        # Session start time
        self.session_start = time.time()

        # Stats
        self.total_trades = 0
        self.winning_trades = 0
        self.total_pnl = 0.0

        # Track logs section position for partial refresh
        self._logs_start_row = 0
        self._last_full_render = False  # Track if last render was full
        self._cached_width = 160

    def log(self, message: str, level: str = "INFO"):
        """Add log message with timestamp - DEBUG logs are hidden from UI"""
        # Skip DEBUG logs in dashboard to reduce noise
        if level == "DEBUG":
            return

        ts = now_display().strftime("%H:%M:%S")
        level_colors = {
            "INFO": dim,
            "SUCCESS": bright_green,
            "ERROR": bright_red,
            "WARN": bright_yellow,
            "ENTRY": bright_cyan,
            "EXIT": magenta,
            "SKIP": yellow,
        }
        color_fn = level_colors.get(level, dim)
        formatted = f"[{ts}] {color_fn(message)}"
        self.logs.append(formatted)

    def add_trade(self, trade: Dict[str, Any]):
        """Add trade to history (expects pre-sorted data, newest first)"""
        self.trades.append(trade)  # Maintain order from sorted data source
        if len(self.trades) > self.max_trades:
            self.trades = self.trades[-self.max_trades:]

        # Update stats - only count CLOSED/REDEEMED trades for win/loss stats
        status = trade.get('exit_reason', '') or trade.get('status', '')
        is_closed = status not in ('OPEN', '?', '')
        if is_closed:
            pnl = trade.get('pnl_cash', 0)
            self.total_trades += 1
            old_total = self.total_pnl
            self.total_pnl += pnl
            # V3.7 DEBUG: Print to stderr for debugging PnL accumulation
            import sys
            print(f"[UI.add_trade] {status}: pnl_cash={pnl:+.4f} | old={old_total:.2f} + {pnl:.4f} = {self.total_pnl:.2f}", file=sys.stderr)
            if pnl > 0:
                self.winning_trades += 1

    def reset_trades(self):
        """Reset trade history and stats (used before reloading from MongoDB)."""
        self.trades = []
        self.total_trades = 0
        self.winning_trades = 0
        self.total_pnl = 0.0

    def add_candle(self, candle: Dict[str, Any]):
        """Add candle to history"""
        self.candles.append(candle)
        if len(self.candles) > self.max_candles:
            self.candles = self.candles[-self.max_candles:]

    def update_candle(self, window_ts: int, updates: Dict[str, Any], accumulate_pnl: bool = False):
        """Update existing candle by window_ts.

        Args:
            window_ts: Window timestamp to update
            updates: Dict with fields to update
            accumulate_pnl: If True, ADD pnl_cash to existing instead of replacing
        """
        for candle in self.candles:
            if candle.get('window_ts') == window_ts:
                # Handle PnL accumulation for accurate window totals
                if accumulate_pnl and 'pnl_cash' in updates:
                    current_pnl = candle.get('pnl_cash', 0) or 0
                    updates['pnl_cash'] = current_pnl + (updates.get('pnl_cash', 0) or 0)
                candle.update(updates)
                return
        # If not found, add new candle
        updates['window_ts'] = window_ts
        self.add_candle(updates)

    def render(
        self,
        status: str,
        window_ts: int,
        elapsed_minutes: float,
        prediction: Optional[str],
        confidence: float,
        current_balance: float,
        yes_price: float = 0,
        no_price: float = 0,
        entry_range: tuple = (0.60, 0.70),
        position_side: Optional[str] = None,
        position_entry_price: float = 0,
        position_pnl_pct: float = 0,
        position_size: float = 0,
        position_current_price: float = 0,
        tsl_level: int = 0,
        tsl_floor: float = 0,
        position_duration: float = 0,
        positions: List[Dict[str, Any]] = None,  # NEW: List of all positions
        combined_pnl_usd: float = 0.0,  # V2.1: Combined PnL (main + hedge) in USD
        combined_pnl_pct: float = 0.0,  # V2.1: Combined PnL as percentage
        # V3: Session summary stats
        session_total_cost: float = 0.0,
        session_sell_proceeds: float = 0.0,
        session_total_value: float = 0.0,
        session_total_profit: float = 0.0,
        session_avg_entry: float = 0.0,
        session_total_tokens: float = 0.0,
        begin_session_balance: float = 0.0,
        portfolio_value: float = 0.0,
        session_profit: float = 0.0,
        session_profit_pct: float = 0.0,
        width: int = 140,
        logs_only: bool = False,  # NEW: Only refresh logs section
        window_minutes: float = 15.0  # Window duration in minutes (5m or 15m)
    ):
        """Render full dashboard or just logs section

        Args:
            logs_only: If True, only refresh the logs section (keeps top static)
            window_minutes: Market window duration (5.0 for 5m, 15.0 for 15m)
        """
        self._cached_width = width

        # If logs_only mode and we have a valid logs position, just refresh logs
        if logs_only and self._logs_start_row > 0 and self._last_full_render:
            self._render_logs_only(width)
            return

        # Skip terminal output in headless mode (still save snapshot for Redis/web)
        if not getattr(self, 'headless', False):
            # Clear screen for full refresh
            clear_screen()

            # ===== KONIS FLASH LOGO =====
            self._render_logo(width)

            # ===== HEADER BAR =====
            self._render_header(
                status, window_ts, elapsed_minutes, width,
                window_minutes=window_minutes,
                position_side=position_side,
                position_entry_price=position_entry_price,
                position_pnl_pct=position_pnl_pct,
                position_size=position_size,
                position_current_price=position_current_price
            )

            # ===== PREDICTION ROW (only when no positions — otherwise shown per market) =====
            if not positions:
                self._render_prediction(prediction, confidence, entry_range, width)

            # ===== SESSION STATS =====
            self._render_stats(current_balance, width, begin_session_balance=begin_session_balance, portfolio_value=portfolio_value, session_profit=session_profit, session_profit_pct=session_profit_pct)

            # ===== CURRENT PRICE DISPLAY (Big YES/NO) with countdown =====
            remaining_seconds = max(0, (window_minutes - elapsed_minutes) * 60)
            self._render_current_price(yes_price, no_price, width, remaining_seconds)

            # ===== CURRENT POSITIONS (if active) =====
            if positions:
                # Use multi-position render when positions list provided
                self._render_current_positions(
                    positions, width, combined_pnl_usd, combined_pnl_pct,
                    session_total_cost, session_sell_proceeds, session_total_value, session_total_profit,
                    session_avg_entry, session_total_tokens,
                    prediction=prediction, confidence=confidence,
                    entry_range=entry_range,
                )
            elif position_side:
                # Fallback to single position for backward compatibility
                self._render_current_position(
                    position_side=position_side,
                    position_entry_price=position_entry_price,
                    position_size=position_size,
                    position_current_price=position_current_price,
                    position_pnl_pct=position_pnl_pct,
                    tsl_level=tsl_level,
                    tsl_floor=tsl_floor,
                    position_duration=position_duration,
                    width=width
                )

            # ===== TRADE HISTORY =====
            self._render_trade_history(width)

            # Track where logs section starts (count lines printed so far)
            self._logs_start_row = self._count_static_lines(positions, position_side)

            # ===== SESSION LOGS =====
            self._render_logs(width)

            # Move cursor to bottom for status line
            print()

            self._last_full_render = True

        # Save TUI snapshot JSON for remote viewing (python terminal_ui.py)
        self._save_tui_snapshot(
            status=status, window_ts=window_ts, elapsed_minutes=elapsed_minutes,
            prediction=prediction, confidence=confidence, current_balance=current_balance,
            yes_price=yes_price, no_price=no_price, entry_range=entry_range,
            position_side=position_side, position_entry_price=position_entry_price,
            position_pnl_pct=position_pnl_pct, position_size=position_size,
            position_current_price=position_current_price, tsl_level=tsl_level,
            tsl_floor=tsl_floor, position_duration=position_duration,
            positions=positions, combined_pnl_usd=combined_pnl_usd,
            combined_pnl_pct=combined_pnl_pct, session_total_cost=session_total_cost,
            session_sell_proceeds=session_sell_proceeds, session_total_value=session_total_value,
            session_total_profit=session_total_profit, session_avg_entry=session_avg_entry,
            session_total_tokens=session_total_tokens, begin_session_balance=begin_session_balance,
            portfolio_value=portfolio_value, session_profit=session_profit,
            session_profit_pct=session_profit_pct, width=width,
            window_minutes=window_minutes,
        )

    def _save_tui_snapshot(self, **render_kwargs):
        """Save current TUI state to JSON for remote viewing.
        File: logs/tui_snapshot/{instance_id}.json (or latest.json if no instance_id)."""
        if getattr(self, '_viewer_mode', False):
            return
        try:
            snapshot_dir = Path(__file__).parent / "logs" / "tui_snapshot"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            snapshot = {
                "instance_id": self.instance_id,
                "config_tag": getattr(self, 'config_tag', ''),
                "render_kwargs": render_kwargs,
                "trades": list(self.trades),
                "candles": list(self.candles),
                "logs": list(self.logs),
                "total_trades": self.total_trades,
                "winning_trades": self.winning_trades,
                "total_pnl": self.total_pnl,
                "session_start": self.session_start,
                "saved_at": time.time(),
            }
            # Per-instance snapshot file (e.g. V3-LIVE.json)
            fname = f"{self.instance_id}.json" if self.instance_id else "latest.json"
            path = snapshot_dir / fname
            path.write_text(json.dumps(snapshot, default=str), encoding="utf-8")
            # Store last snapshot for Redis publishing
            self._last_snapshot = snapshot
        except Exception:
            pass  # Never crash the bot for snapshot save

    def _count_static_lines(self, positions, position_side) -> int:
        """Count approximate number of lines in static section"""
        lines = 0
        # Logo: 1 (top border) + 6 (art) + 2 (subtitle + bottom) = 9
        lines += 9
        # Header bar: ~3 lines
        lines += 3
        # Prediction row: 2 lines
        lines += 2
        # Stats: 3 lines
        lines += 3
        # Current price display: ~8 lines
        lines += 8
        # Positions section: varies
        if positions:
            lines += 3 + len(positions) * 2  # Header + 2 lines per position roughly
        elif position_side:
            lines += 6
        # Trade history: header + trades + border
        lines += 3 + min(len(self.trades), self.max_trades)
        # Candle history: header + candles + border
        lines += 3 + min(len(self.candles), self.max_candles)
        return lines

    def _render_logs_only(self, width: int):
        """Render only the logs section without redrawing the entire screen"""
        # Move cursor to logs section start
        move_cursor_to(self._logs_start_row, 1)
        # Clear from cursor to end of screen
        clear_from_cursor()
        # Render just logs
        self._render_logs(width)
        print()

    def render_logs_refresh(self, width: int = None):
        """Public method to refresh just the logs section"""
        if width is None:
            width = self._cached_width
        if self._logs_start_row > 0 and self._last_full_render:
            self._render_logs_only(width)
        else:
            # Can't do partial refresh, need full render first
            pass

    def _render_logo(self, width: int):
        """Render KONIS FLASH logo at top - blue style (same as print_header_compact but with FLASH)"""
        # KONIS FLASH ASCII Art - Unicode block style matching print_header_compact
        art = [
            "██╗  ██╗ ██████╗ ███╗   ██╗██╗███████╗  ███████╗██╗      █████╗ ███████╗██╗  ██╗",
            "██║ ██╔╝██╔═══██╗████╗  ██║██║██╔════╝  ██╔════╝██║     ██╔══██╗██╔════╝██║  ██║",
            "█████╔╝ ██║   ██║██╔██╗ ██║██║███████╗  █████╗  ██║     ███████║███████╗███████║",
            "██╔═██╗ ██║   ██║██║╚██╗██║██║╚════██║  ██╔══╝  ██║     ██╔══██║╚════██║██╔══██║",
            "██║  ██╗╚██████╔╝██║ ╚████║██║███████║  ██║     ███████╗██║  ██║███████║██║  ██║",
            "╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝╚══════╝  ╚═╝     ╚══════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝",
        ]

        print(blue(Box.DTL + Box.DH * (width - 2) + Box.DTR))
        for line in art:
            padding = (width - 2 - len(line)) // 2
            print(blue(Box.DV) + " " * padding + bright_cyan(line) + " " * (width - 2 - padding - len(line)) + blue(Box.DV))

        # Subtitle
        subtitle = "Polymarket Scalping Bot"
        padding = (width - 2 - len(subtitle)) // 2
        print(blue(Box.DV) + " " * padding + bold(subtitle) + " " * (width - 2 - padding - len(subtitle)) + blue(Box.DV))

        # Instance ID
        instance_text = f"Instance: {self.instance_id}"
        padding = (width - 2 - len(instance_text)) // 2
        print(blue(Box.DV) + " " * padding + dim(instance_text) + " " * (width - 2 - padding - len(instance_text)) + blue(Box.DV))

        print(blue(Box.DBL + Box.DH * (width - 2) + Box.DBR))

    def _render_header(
        self,
        status: str,
        window_ts: int,
        elapsed_minutes: float,
        width: int,
        position_side: Optional[str] = None,
        position_entry_price: float = 0,
        position_pnl_pct: float = 0,
        position_size: float = 0,
        position_current_price: float = 0,
        window_minutes: float = 15.0
    ):
        """Render header bar with status and position info"""
        print(box_top(width))

        # Status badge
        badge = status_badge(status)

        # Window time
        window_time = format_time_display(window_ts, "%H:%M") + f" {DISPLAY_TZ_NAME}" if window_ts > 0 else "--:--"

        # Elapsed
        wm_label = f"{int(window_minutes)}m" if window_minutes == int(window_minutes) else f"{window_minutes:.1f}m"
        elapsed_str = f"{elapsed_minutes:.1f}m / {wm_label}"

        row = f"{badge}  {dim('Window:')} {cyan(window_time)}  {dim('Elapsed:')} {yellow(elapsed_str)}"
        print(box_row(row, width))

        # Show current position if OPEN
        if status == "OPEN" and position_side:
            print(box_middle(width))
            side_color = bright_green if position_side == "YES" else bright_red
            pnl_pct_display = pnl_color(position_pnl_pct, as_percent=True)
            pnl_cash = position_size * (position_current_price - position_entry_price)
            pnl_cash_display = pnl_color(pnl_cash)

            pos_row = (
                f"{dim('Position:')} {side_color(position_side)}  "
                f"{dim('Entry:')} {cyan(f'${position_entry_price:.4f}')}  "
                f"{dim('Now:')} {bright_cyan(f'${position_current_price:.4f}')}  "
                f"{dim('Size:')} {cyan(f'{position_size:.2f}')}  "
                f"{dim('PnL:')} {pnl_pct_display} ({pnl_cash_display})"
            )
            print(box_row(pos_row, width))

        print(box_bottom(width))

    def _render_prediction(self, prediction: Optional[str], confidence: float, entry_range: tuple, width: int):
        """Render prediction row with entry thresholds"""
        print(box_top(width))

        # Entry range display
        entry_low, entry_high = entry_range
        range_display = f"{dim('Entry Range:')} {yellow(f'{entry_low:.0%}')} - {yellow(f'{entry_high:.0%}')}"

        if prediction:
            pred_display = direction_color(prediction)
            conf_display = confidence_color(confidence)
            row = f"{dim('Prediction:')} {pred_display}  {dim('Confidence:')} {conf_display}  |  {range_display}"
        else:
            row = f"{dim('Waiting for price signal...')}  |  {range_display}"

        print(box_row(row, width))
        print(box_bottom(width))

    def _render_stats(self, current_balance: float, width: int, begin_session_balance: float = 0.0, portfolio_value: float = 0.0, session_profit: float = 0.0, session_profit_pct: float = 0.0):
        """Render session stats panel"""
        current_time = now_display().strftime(f"%H:%M:%S {DISPLAY_TZ_NAME}")

        print()
        print(box_top(width))
        title = f" {Symbols.CHART} SESSION STATS  [{current_time}] "
        padding = (width - 2 - len(strip_ansi(title))) // 2
        print(cyan(Box.V) + " " * padding + bold(bright_cyan(title)) + " " * (width - 2 - padding - len(strip_ansi(title))) + cyan(Box.V))
        print(box_middle(width))

        # Calculate stats
        win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0
        losing_trades = self.total_trades - self.winning_trades
        session_duration = format_duration(time.time() - self.session_start)

        # Row 1: Trade counts
        trades_display = bright_cyan(str(self.total_trades))
        row1 = f"  {dim('Trades:')} {trades_display}   {dim('Wins:')} {bright_green(str(self.winning_trades))}   {dim('Losses:')} {bright_red(str(losing_trades))}   {dim('Win Rate:')} "
        if win_rate >= 50:
            row1 += bright_green(f"{win_rate:.1f}%")
        else:
            row1 += bright_red(f"{win_rate:.1f}%") if self.total_trades > 0 else dim("N/A")
        print(box_row(row1, width))

        # Row 2: PnL & Balance
        pnl_display = pnl_color(self.total_pnl)
        balance_display = bright_green(f"${current_balance:.2f}")
        begin_bal = f"  {dim('Begin:')} {dim(f'${begin_session_balance:.2f}')}" if begin_session_balance > 0 else ""
        portfolio_disp = f"  {dim('Total:')} {bright_yellow(f'${portfolio_value:.2f}')}" if portfolio_value > 0 else ""
        session_pnl_disp = f"  {dim('Session:')} {pnl_color(session_profit)} ({pnl_color(session_profit_pct, as_percent=True)})" if begin_session_balance > 0 else ""
        row2 = f"  {dim('Total PnL:')} {pnl_display}   {dim('Balance:')} {balance_display}{begin_bal}{portfolio_disp}{session_pnl_disp}   {dim('Duration:')} {cyan(session_duration)}"
        print(box_row(row2, width))

        print(box_bottom(width))

    def _render_current_position(
        self,
        position_side: Optional[str],
        position_entry_price: float,
        position_size: float,
        position_current_price: float,
        position_pnl_pct: float,
        tsl_level: int = 0,
        tsl_floor: float = 0,
        position_duration: float = 0,
        width: int = 140
    ):
        """Render current position panel - shows active trade details (single position)"""
        if not position_side:
            return  # No position to display

        # Wrap single position in list and call multi-position render
        positions = [{
            'side': position_side,
            'entry_price': position_entry_price,
            'size': position_size,
            'current_price': position_current_price,
            'pnl_pct': position_pnl_pct,
            'tsl_level': tsl_level,
            'tsl_floor': tsl_floor,
            'duration': position_duration,
        }]
        self._render_current_positions(positions, width)

    def _render_current_positions(
        self, positions: List[Dict[str, Any]], width: int = 140,
        combined_pnl_usd: float = 0.0, combined_pnl_pct: float = 0.0,
        session_total_cost: float = 0.0, session_sell_proceeds: float = 0.0,
        session_total_value: float = 0.0,
        session_total_profit: float = 0.0, session_avg_entry: float = 0.0,
        session_total_tokens: float = 0.0,
        prediction: Optional[str] = None, confidence: float = 0.0,
        entry_range: tuple = (0, 1.0),
    ):
        """Render current positions panel - shows ALL active trade details, GROUPED BY MARKET"""
        if not positions:
            return  # No positions to display

        # Group positions by market slug
        markets_grouped: Dict[str, List[Dict[str, Any]]] = {}
        for p in positions:
            market_slug = p.get('market', 'unknown')
            if market_slug not in markets_grouped:
                markets_grouped[market_slug] = []
            markets_grouped[market_slug].append(p)

        # Render each market as a separate block
        for market_slug, market_positions in markets_grouped.items():
            print()

            # Calculate combined PnL for THIS market
            market_yes_tokens = sum(p.get('size', 0) for p in market_positions if p.get('side') == 'YES')
            market_no_tokens = sum(p.get('size', 0) for p in market_positions if p.get('side') == 'NO')
            market_yes_cost = sum(p.get('size', 0) * p.get('entry_price', 0) for p in market_positions if p.get('side') == 'YES')
            market_no_cost = sum(p.get('size', 0) * p.get('entry_price', 0) for p in market_positions if p.get('side') == 'NO')
            market_yes_value = sum(p.get('size', 0) * p.get('current_price', 0) for p in market_positions if p.get('side') == 'YES')
            market_no_value = sum(p.get('size', 0) * p.get('current_price', 0) for p in market_positions if p.get('side') == 'NO')
            market_total_cost = market_yes_cost + market_no_cost
            market_total_value = market_yes_value + market_no_value
            market_pnl_usd = market_total_value - market_total_cost
            market_pnl_pct = market_pnl_usd / market_total_cost if market_total_cost > 0 else 0

            # Use market PnL for border color
            border_color = bright_green if market_pnl_pct >= 0 else bright_red

            print(border_color(Box.DTL + Box.DH * (width - 2) + Box.DTR))
            title = f" {Symbols.BULLET} {market_slug.upper()} ({len(market_positions)}) "
            padding = (width - 2 - len(strip_ansi(title))) // 2
            print(border_color(Box.DV) + " " * padding + bold(bright_cyan(title)) + " " * (width - 2 - padding - len(strip_ansi(title))) + border_color(Box.DV))
            # Prediction row per market
            entry_low, entry_high = entry_range
            range_display = f"{dim('Entry Range:')} {yellow(f'{entry_low:.0%}')} - {yellow(f'{entry_high:.0%}')}"
            if prediction:
                pred_row = f"  {dim('Prediction:')} {direction_color(prediction)}  {dim('Confidence:')} {confidence_color(confidence)}  |  {range_display}"
            else:
                pred_row = f"  {dim('Waiting for price signal...')}  |  {range_display}"
            print(border_color(Box.DV) + pred_row + " " * max(0, width - 2 - len(strip_ansi(pred_row))) + border_color(Box.DV))
            print(border_color(Box.DV + " " * (width - 2) + Box.DV))

            # Render each position in this market
            for i, pos in enumerate(market_positions):
                position_side = pos.get('side')
                position_entry_price = pos.get('entry_price', 0)
                position_size = pos.get('size', 0)
                position_current_price = pos.get('current_price', 0)
                position_pnl_pct = pos.get('pnl_pct', 0)
                position_duration = pos.get('duration', 0)
                is_hedge = pos.get('is_hedge', False)

                # Side with color + hedge indicator
                side_color = bright_green if position_side == "YES" else bright_red
                hedge_tag = f" {dim('[HEDG]')}" if is_hedge else ""
                side_display = side_color(f"{Symbols.ARROW_UP if position_side == 'YES' else Symbols.ARROW_DOWN} {position_side}") + hedge_tag

                # PnL calculations
                pnl_cash = position_size * (position_current_price - position_entry_price)
                pnl_pct_display = pnl_color(position_pnl_pct, as_percent=True)
                pnl_cash_display = pnl_color(pnl_cash)

                # Duration
                duration_display = format_duration(position_duration) if position_duration > 0 else "0s"

                # Calculate $ value of position
                position_value_usd = position_size * position_current_price

                # Row: Side, Entry, Current, Size (tokens + $), PnL, Duration
                row = f"  {dim('Side:')} {side_display}  {dim('Entry:')} {cyan(f'${position_entry_price:.4f}')}  {dim('Current:')} {bright_cyan(f'${position_current_price:.4f}')}  {dim('Size:')} {cyan(f'{position_size:.2f}')} {dim('(')}${position_value_usd:.2f}{dim(')')}  {dim('PnL:')} {pnl_pct_display} ({pnl_cash_display})  {dim('Dur:')} {cyan(duration_display)}"
                print(border_color(Box.DV) + row + " " * max(0, width - 2 - len(strip_ansi(row))) + border_color(Box.DV))

                # Add separator between positions (except last)
                if i < len(market_positions) - 1:
                    print(border_color(Box.DV) + " " + dim("-" * (width - 4)) + " " + border_color(Box.DV))

            # Show MARKET AVERAGE if multiple positions
            if len(market_positions) > 1:
                print(border_color(Box.DV) + " " + dim("=" * (width - 4)) + " " + border_color(Box.DV))

                total_size = market_yes_tokens + market_no_tokens
                avg_entry = market_total_cost / total_size if total_size > 0 else 0
                avg_current = market_total_value / total_size if total_size > 0 else 0

                # Market average row
                market_short = market_slug.split('-')[0].upper() if market_slug else 'AVG'
                avg_row = f"  {bold(magenta(f'{market_short} AVG'))}  {dim('Entry:')} {bright_yellow(f'${avg_entry:.4f}')}  {dim('Current:')} {bright_cyan(f'${avg_current:.4f}')}  {dim('Size:')} {cyan(f'{total_size:.2f}')} {dim('(')}${market_total_value:.2f}{dim(')')}  {dim('PnL:')} {pnl_color(market_pnl_pct, as_percent=True)} ({pnl_color(market_pnl_usd)})"
                print(border_color(Box.DV) + avg_row + " " * max(0, width - 2 - len(strip_ansi(avg_row))) + border_color(Box.DV))

            # Show COMBINED PnL + Resolve scenarios for this market if has hedge
            has_hedge = any(p.get('is_hedge', False) for p in market_positions)
            if has_hedge:
                print(border_color(Box.DV) + " " + dim("═" * (width - 4)) + " " + border_color(Box.DV))
                # Use session-aware PnL (matches TP logic: total_cost includes sold tokens + fees)
                if session_total_cost > 0:
                    combined_display_pnl_usd = session_total_profit
                    combined_display_pnl_pct = session_total_profit / session_total_cost
                else:
                    combined_display_pnl_usd = market_pnl_usd
                    combined_display_pnl_pct = market_pnl_pct
                combined_row = f"  {bold(bright_yellow('COMBINED (Main + Hedge)'))}  {dim('Net PnL:')} {pnl_color(combined_display_pnl_pct, as_percent=True)} ({pnl_color(combined_display_pnl_usd)})"

                # V3.2: Calculate PnL at resolution for THIS market
                RESOLVE_WIN = 0.97  # Sell before resolution to avoid redeem delays
                RESOLVE_LOSE = 0.00

                if market_total_cost > 0:
                    payout_if_yes = market_yes_tokens * RESOLVE_WIN + market_no_tokens * RESOLVE_LOSE
                    pnl_if_yes_usd = payout_if_yes - market_total_cost
                    pnl_if_yes_pct = pnl_if_yes_usd / market_total_cost

                    payout_if_no = market_no_tokens * RESOLVE_WIN + market_yes_tokens * RESOLVE_LOSE
                    pnl_if_no_usd = payout_if_no - market_total_cost
                    pnl_if_no_pct = pnl_if_no_usd / market_total_cost

                    resolve_row = f"  {dim('Resolve PnL:')} {bright_green('YES wins')} {pnl_color(pnl_if_yes_pct, as_percent=True)} ({pnl_color(pnl_if_yes_usd)})  {dim('|')}  {bright_red('NO wins')} {pnl_color(pnl_if_no_pct, as_percent=True)} ({pnl_color(pnl_if_no_usd)})"
                    combined_row += "  " + resolve_row

                print(border_color(Box.DV) + combined_row + " " * max(0, width - 2 - len(strip_ansi(combined_row))) + border_color(Box.DV))

            print(border_color(Box.DBL + Box.DH * (width - 2) + Box.DBR))

        # V3: SESSION SUMMARY - Total across ALL markets
        if session_total_cost > 0:
            print()
            session_pnl_pct = session_total_profit / session_total_cost if session_total_cost > 0 else 0
            session_border = bright_green if session_pnl_pct >= 0 else bright_red

            print(session_border(Box.DTL + Box.DH * (width - 2) + Box.DTR))
            summary_row = (
                f"  {bold(bright_white('SESSION TOTAL'))}  "
                f"{dim('Cost:')} {bright_yellow(f'${session_total_cost:.2f}')}  "
                f"{dim('Value:')} {bright_cyan(f'${session_total_value:.2f}')}  "
                f"{dim('Profit:')} {pnl_color(session_total_profit)} ({pnl_color(session_pnl_pct, as_percent=True)})  "
                f"{dim('Avg Entry:')} {cyan(f'${session_avg_entry:.4f}')}  "
                f"{dim('Tokens:')} {cyan(f'{session_total_tokens:.0f}')}"
            )
            print(session_border(Box.DV) + summary_row + " " * max(0, width - 2 - len(strip_ansi(summary_row))) + session_border(Box.DV))

            # Resolve scenario lines — show value if YES or NO resolves at $0.97 (sell before resolution)
            yes_tokens_total = sum(p.get('size', 0) for p in positions if p.get('side', '').upper() in ('YES', 'UP'))
            no_tokens_total = sum(p.get('size', 0) for p in positions if p.get('side', '').upper() in ('NO', 'DOWN'))
            if yes_tokens_total > 0 or no_tokens_total > 0:
                yes_resolve_val = yes_tokens_total * 0.97
                no_resolve_val = no_tokens_total * 0.97
                yes_resolve_pnl = yes_resolve_val + session_sell_proceeds - session_total_cost
                no_resolve_pnl = no_resolve_val + session_sell_proceeds - session_total_cost
                yes_pnl_pct = yes_resolve_pnl / session_total_cost if session_total_cost > 0 else 0
                no_pnl_pct = no_resolve_pnl / session_total_cost if session_total_cost > 0 else 0
                resolve_row = (
                    f"  {dim('IF YES Resolves:')} {bright_green(f'${yes_resolve_val:.2f}')} "
                    f"{pnl_color(yes_resolve_pnl)} ({pnl_color(yes_pnl_pct, as_percent=True)})  "
                    f"{dim('│')}  "
                    f"{dim('IF NO Resolves:')} {bright_red(f'${no_resolve_val:.2f}')} "
                    f"{pnl_color(no_resolve_pnl)} ({pnl_color(no_pnl_pct, as_percent=True)})"
                )
                print(session_border(Box.DV) + resolve_row + " " * max(0, width - 2 - len(strip_ansi(resolve_row))) + session_border(Box.DV))

            print(session_border(Box.DBL + Box.DH * (width - 2) + Box.DBR))

    def _render_current_price(self, yes_price: float, no_price: float, width: int, remaining_seconds: float = 0):
        """Render big YES/NO price display - YES in green, NO in red using 5-line font

        Args:
            remaining_seconds: Seconds remaining in current window (for countdown display)
        """
        print()
        print(box_top(width))

        yes_pct = int(yes_price * 100) if 0 < yes_price < 1.0 else 0
        no_pct = int(no_price * 100) if 0 < no_price < 1.0 else 0

        # 5-line ASCII digits (same style as KONIS FLASH banner)
        big_digits = {
            '0': [
                '██████╗ ',
                '██╔══██╗',
                '██║  ██║',
                '██║  ██║',
                '╚█████╔╝',
            ],
            '1': [
                ' ██╗',
                '███║',
                '╚██║',
                ' ██║',
                ' ██║',
            ],
            '2': [
                '██████╗ ',
                '╚════██╗',
                ' █████╔╝',
                '██╔═══╝ ',
                '███████╗',
            ],
            '3': [
                '██████╗ ',
                '╚════██╗',
                ' █████╔╝',
                ' ╚═══██╗',
                '██████╔╝',
            ],
            '4': [
                '██╗  ██╗',
                '██║  ██║',
                '███████║',
                '╚════██║',
                '     ██║',
            ],
            '5': [
                '███████╗',
                '██╔════╝',
                '███████╗',
                '╚════██║',
                '███████║',
            ],
            '6': [
                '██████╗ ',
                '██╔════╝',
                '███████╗',
                '██╔══██║',
                '╚█████╔╝',
            ],
            '7': [
                '███████╗',
                '╚════██║',
                '    ██╔╝',
                '   ██╔╝ ',
                '   ██║  ',
            ],
            '8': [
                '██████╗ ',
                '██╔══██╗',
                '╚█████╔╝',
                '██╔══██╗',
                '╚█████╔╝',
            ],
            '9': [
                '██████╗ ',
                '██╔══██╗',
                '╚██████║',
                ' ╚═══██║',
                ' █████╔╝',
            ],
        }

        def render_big_number(num: int) -> list:
            """Convert 2-digit number to 5-line big digit representation"""
            d1 = str(num // 10)  # tens
            d2 = str(num % 10)   # ones
            lines = []
            for i in range(5):
                line = big_digits[d1][i] + ' ' + big_digits[d2][i]
                lines.append(line)
            return lines

        yes_lines = render_big_number(yes_pct)
        no_lines = render_big_number(no_pct)

        # Build countdown display (MM:SS format) using big digits
        countdown_mins = int(remaining_seconds // 60) if remaining_seconds > 0 else 0
        countdown_secs = int(remaining_seconds % 60) if remaining_seconds > 0 else 0
        countdown_mm = render_big_number(min(countdown_mins, 99))
        countdown_ss = render_big_number(min(countdown_secs, 59))

        # Colon separator for MM:SS (5 lines)
        colon = [
            '   ',
            ' █ ',
            '   ',
            ' █ ',
            '   ',
        ]

        # Labels
        yes_label = "YES "
        no_label = "NO "
        separator = "  │  "
        time_label = "TIME "

        # Calculate block widths
        price_block_raw = yes_label + yes_lines[0] + separator + no_label + no_lines[0]
        price_width = len(price_block_raw)
        countdown_block_raw = time_label + countdown_mm[0] + colon[0] + countdown_ss[0]
        countdown_width = len(countdown_block_raw)

        # 50/50 layout: TIME centered in left half, PRICE centered in right half
        inner_width = width - 2  # Inside box borders
        half_width = inner_width // 2

        # Calculate centering padding for each half
        time_pad = max(0, (half_width - countdown_width) // 2)
        price_pad = max(0, (half_width - price_width) // 2)

        # Render the 5-line price display
        for i in range(5):
            # Countdown part (MM:SS) - centered in left half
            if i == 2:  # Middle line - add TIME label
                countdown_part = bright_cyan(time_label + countdown_mm[i]) + bright_yellow(colon[i]) + bright_cyan(countdown_ss[i])
            else:
                pad_time = " " * len(time_label)
                countdown_part = bright_cyan(pad_time + countdown_mm[i]) + bright_yellow(colon[i]) + bright_cyan(countdown_ss[i])

            # Price part (YES XX | NO XX) - centered in right half
            if i == 2:  # Middle line - add labels
                price_part = bright_green(yes_label + yes_lines[i]) + dim(separator) + bright_red(no_label + no_lines[i])
            else:
                pad_yes = " " * len(yes_label)
                pad_no = " " * len(no_label)
                price_part = bright_green(pad_yes + yes_lines[i]) + dim(separator) + bright_red(pad_no + no_lines[i])

            # Build left half (countdown centered) + right half (price centered)
            left_half = " " * time_pad + countdown_part + " " * (half_width - time_pad - countdown_width)
            right_half = " " * price_pad + price_part
            line = left_half + right_half
            print(box_row(line, width))

        # Progress bar with arrow showing price direction
        bar_width = min(50, half_width - 15)  # Fit in right half
        yes_fill = int((yes_pct / 100) * bar_width)
        no_fill = bar_width - yes_fill

        # Determine arrow direction based on which side is winning
        if yes_pct > 50:
            arrow = bright_green("◀◀◀")
            bar_left = bright_green("█" * yes_fill)
            bar_right = bright_red("░" * no_fill)
        elif no_pct > 50:
            arrow = bright_red("▶▶▶")
            bar_left = bright_green("░" * yes_fill)
            bar_right = bright_red("█" * no_fill)
        else:
            arrow = dim("◆◆◆")
            bar_left = bright_green("█" * yes_fill)
            bar_right = bright_red("█" * no_fill)

        # Build progress bar centered under price block in right half
        progress_bar = f"{bright_green('YES')} [{bar_left}{bar_right}] {bright_red('NO')}  {arrow}"
        bar_content_width = len(strip_ansi(progress_bar))
        bar_pad_in_half = max(0, (half_width - bar_content_width) // 2)
        # Left half empty + bar centered in right half
        bar_line = " " * half_width + " " * bar_pad_in_half + progress_bar
        print(box_row(bar_line, width))

        print(box_bottom(width))

    def _render_trade_history(self, width: int):
        """Render trade history table with separate entry/exit prices"""
        now_str = now_display().strftime(f"%d-%b-%y %H:%M {DISPLAY_TZ_NAME}")

        print()
        print(box_top(width))
        title = f" {Symbols.CHART} TRADE HISTORY [{now_str}] "
        padding = (width - 2 - len(strip_ansi(title))) // 2
        print(cyan(Box.V) + " " * padding + bold(magenta(title)) + " " * (width - 2 - padding - len(strip_ansi(title))) + cyan(Box.V))
        print(box_middle(width))

        # Header with window time, Entry and Exit columns + Market
        header = f"  {dim('#')} {dim('Window')}        {dim('Side')} {dim('Market')}   {dim('Entry$')}  {dim('Exit$')}  {dim('PnL %')}   {dim('PnL $')}  {dim('Dur')}  {dim('Reason')}"
        print(box_row(header, width))
        print(box_middle(width))

        if not self.trades:
            empty_msg = dim("  No trades yet")
            print(box_row(empty_msg, width))
        else:
            total = len(self.trades)
            recent_trades = list(reversed(self.trades))[:self.max_trades]
            for i, trade in enumerate(recent_trades):
                row_num = total - i

                side_val = trade.get('side', '?')
                side = direction_color(side_val)

                # Market - extract coin + timeframe (e.g., "btc-updown-15m-1774336500" → "BTC-15M")
                market_slug = trade.get('market_slug', '')
                if market_slug:
                    import re as _re
                    parts = market_slug.split('-')
                    coin = parts[0].upper()[:4]
                    # Find timeframe part matching pattern like 15m, 1h, 5m (not the timestamp)
                    timeframe = ""
                    for p in parts[1:]:
                        if _re.fullmatch(r'\d+[mhd]', p):
                            timeframe = p.upper()
                            break
                    market_short = f"{coin}-{timeframe}" if timeframe else coin
                else:
                    market_short = "---"

                # Format times
                entry_time = trade.get('entry_time', 0)
                exit_time = trade.get('exit_time', 0)
                entry_time_str = format_time_display(entry_time, "%d-%b %H:%M") if entry_time else ""
                exit_time_str = format_time_display(exit_time, "%H:%M") if exit_time else ""

                # Prices - separate columns
                entry_price = trade.get('entry_price', 0)
                exit_price = trade.get('exit_price', 0)
                entry_price_str = f"{entry_price:.3f}" if entry_price > 0 else "---"
                exit_price_str = f"{exit_price:.3f}" if exit_price > 0 else "---"

                # PnL — pnl_percent is already in % (e.g. 13.8 = 13.8%), divide by 100 for format spec
                pnl_pct = trade.get('pnl_percent', 0)
                pnl_pct_display = pnl_color(pnl_pct / 100, as_percent=True)

                pnl_cash = trade.get('pnl_cash', 0)
                pnl_cash_display = pnl_color(pnl_cash, as_percent=False)

                # Duration
                duration = ""
                if entry_time and exit_time:
                    duration = format_duration(exit_time - entry_time)

                # Reason
                reason = trade.get('exit_reason', '')[:18]
                if "TP" in reason or "profit" in reason.lower():
                    reason = bright_green(reason)
                elif "SL" in reason or "loss" in reason.lower():
                    reason = bright_red(reason)
                else:
                    reason = yellow(reason)

                # Window time from trade data (display in local TZ)
                window_ts = trade.get('window_ts', 0)
                if window_ts:
                    window_str = format_time_display(window_ts, "%d %b - %H:%M").lstrip("0")
                else:
                    window_str = "---"

                row = f"  {dim(str(row_num).zfill(2))} {dim(window_str.ljust(13))} {side} {magenta(market_short.ljust(8))} {bright_cyan(entry_price_str.ljust(6))} {bright_yellow(exit_price_str.ljust(6))} {pnl_pct_display} {pnl_cash_display} {cyan(duration.ljust(5))} {reason}"
                print(box_row(row, width))

        print(box_bottom(width))

    def _render_candle_history(self, width: int):
        """Render candle (15m window) history"""
        print()
        print(box_top(width))
        title = f" {Symbols.CHART} CANDLE HISTORY (Last 1 Hour) "
        padding = (width - 2 - len(strip_ansi(title))) // 2
        print(cyan(Box.V) + " " * padding + bold(magenta(title)) + " " * (width - 2 - padding - len(strip_ansi(title))) + cyan(Box.V))
        print(box_middle(width))

        # Header
        header = f"  {'Time':<8} {'Status':<10} {'Pred':<6} {'Conf':<8} {'Reason/PnL':<30}"
        print(box_row(dim(header), width))
        print(box_middle(width))

        if not self.candles:
            empty_msg = dim("  No candles yet")
            print(box_row(empty_msg, width))
        else:
            for candle in reversed(self.candles[-4:]):
                window_ts = candle.get('window_ts', 0)
                time_str = format_time_display(window_ts, "%H:%M") if window_ts else "--:--"

                status = candle.get('status', 'WAITING')
                status_colors = {
                    "ENTERED": bright_green,
                    "CLOSED": green,
                    "SOLD": green,
                    "SKIPPED": yellow,
                    "FAILED": bright_red,
                    "WAITING": dim,
                }
                status_color = status_colors.get(status, dim)
                status_display = status_color(status.ljust(10))

                # Prediction
                pred = candle.get('prediction', '')
                if pred in ("YES", "NO"):
                    pred_display = direction_color(pred)
                else:
                    pred_display = dim(str(pred or "---").ljust(6))

                # Confidence
                conf = candle.get('confidence', 0)
                if conf > 0:
                    conf_display = confidence_color(conf)
                else:
                    conf_display = dim("---")

                # Reason/PnL
                reason = candle.get('reason', '') or candle.get('exit_reason', '')
                pnl = candle.get('pnl_cash', 0)
                if pnl != 0:
                    reason_display = f"{pnl_color(pnl)}"
                else:
                    reason_display = reason[:30] if reason else dim("---")

                row = f"  {cyan(time_str):<8} {status_display} {pred_display:<6} {conf_display:<8} {reason_display}"
                print(box_row(row, width))

        print(box_bottom(width))

    def _render_logs(self, width: int):
        """Render session logs panel"""
        print()
        title = f" {dim('─── SESSION LOGS ───')} "
        print(title.center(width))

        if not self.logs:
            print(dim("  No logs yet"))
        else:
            for log in list(self.logs)[-self.max_logs:]:
                # Wrap long logs to multiple lines instead of truncating
                visible_len = len(strip_ansi(log))
                if visible_len > width - 2:
                    # Print first line
                    print(log[:width - 2])
                    # Wrap remaining text with indent
                    remaining = log[width - 2:]
                    indent = "          "  # 10 spaces to align with timestamp
                    while remaining:
                        chunk = remaining[:width - len(indent) - 2]
                        print(f"{indent}{chunk}")
                        remaining = remaining[width - len(indent) - 2:]
                else:
                    print(log)

    def print_status_line(
        self,
        balance: float,
        yes_price: float,
        no_price: float,
        entry_range: tuple,
        elapsed: float,
        position_side: Optional[str] = None,
        position_entry_price: float = 0,
        position_pnl_pct: float = 0
    ):
        """Print single-line status (for non-dashboard mode or bottom status)"""
        now_tz = now_display()
        time_str = now_tz.strftime('%H:%M:%S')

        entry_price, exit_price = entry_range
        yes_color = bright_green if yes_price > entry_price else dim
        no_color = bright_red if no_price > entry_price else dim

        status_line = (
            f"\r{dim('[')} {cyan(time_str)} {dim(']')} "
            f"{bright_green(f'${balance:.2f}')} {dim('|')} "
            f"{yes_color(f'YES:{yes_price:.3f}')} {no_color(f'NO:{no_price:.3f}')} {dim('|')} "
            f"{yellow(f'[{entry_price:.2f}-{exit_price:.2f}]')} {dim('|')} "
            f"{cyan(f'{elapsed:.1f}m')}"
        )

        if position_side:
            watch_price = yes_price if position_side == "YES" else no_price
            pnl = position_pnl_pct * 100
            side_color = bright_green if position_side == "YES" else bright_red
            pnl_str = bright_green(f'{pnl:+.1f}%') if pnl > 0 else bright_red(f'{pnl:+.1f}%') if pnl < 0 else dim(f'{pnl:+.1f}%')
            status_line += f" {dim('|')} {side_color(position_side)} {dim(f'{position_entry_price:.3f}')}{dim('->')}{bright_cyan(f'{watch_price:.3f}')} {pnl_str}"

        print(status_line + "    ", end="", flush=True)


def print_scalping_header(
    entry_price: float,
    exit_price: float,
    take_profit_pct: float,
    stop_loss_pct: float,
    trade_unit: float,
    check_interval: int,
    max_retries: int,
    width: int = 140
):
    """Print scalping bot startup header"""
    print()
    print(cyan(Box.DTL + Box.DH * (width - 2) + Box.DTR))

    # Title
    title = "POLYMARKET KONIS SCALPING BOT"
    padding = (width - 2 - len(title)) // 2
    print(cyan(Box.DV) + " " * padding + bold(bright_cyan(title)) + " " * (width - 2 - padding - len(title)) + cyan(Box.DV))

    print(cyan(Box.DV) + " " * (width - 2) + cyan(Box.DV))

    # Config rows
    configs = [
        f"  {dim('Entry:')} {bright_yellow(f'{entry_price:.2f}')}  {dim('TP:')} {bright_green(f'{exit_price:.2f}')} / {bright_green(f'{take_profit_pct:+.0%}')}  {dim('SL:')} {bright_red(f'{stop_loss_pct:+.0%}')}",
        f"  {dim('Trade Unit:')} {bright_green(f'${trade_unit}')}   {dim('Interval:')} {cyan(f'{check_interval}s')}   {dim('Retries:')} {cyan(str(max_retries))}   {dim('TZ:')} {cyan(DISPLAY_TZ_NAME)}",
    ]

    for cfg in configs:
        print(cyan(Box.DV) + cfg + " " * (width - 3 - len(strip_ansi(cfg))) + cyan(Box.DV))

    print(cyan(Box.DV) + " " * (width - 2) + cyan(Box.DV))

    # Entry rules
    rules = [
        f"  {bold('Entry:')} Price in range [{entry_price:.2f}, {exit_price:.2f}] OR rises from <{entry_price:.2f}",
        f"  {bold('TP:')} Price > {exit_price:.2f} OR PnL > {take_profit_pct:+.0%}   {bold('SL:')} PnL < {stop_loss_pct:+.0%}",
    ]

    for rule in rules:
        print(cyan(Box.DV) + rule + " " * (width - 3 - len(strip_ansi(rule))) + cyan(Box.DV))

    print(cyan(Box.DBL + Box.DH * (width - 2) + Box.DBR))
    print()


# ============ Standalone TUI Viewer (python terminal_ui.py) ============

def _run_tui_viewer():
    """Load TUI snapshot JSON and render in a loop (1s refresh).
    Usage: python terminal_ui.py [instance_id]
    If no instance_id given, lists available snapshots and prompts to pick."""
    from collections import deque

    snapshot_dir = Path(__file__).parent / "logs" / "tui_snapshot"
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    # Determine which snapshot to watch
    instance_arg = sys.argv[1] if len(sys.argv) > 1 else None

    if instance_arg:
        snapshot_path = snapshot_dir / f"{instance_arg}.json"
        if not snapshot_path.exists():
            # Try with .json appended
            snapshot_path = snapshot_dir / instance_arg
    else:
        # Auto-pick most recent non-VIEWER snapshot (bot instances only)
        all_snapshots = sorted(snapshot_dir.glob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True)
        snapshots = [f for f in all_snapshots if f.stem.upper() != "VIEWER"]
        if not snapshots:
            snapshots = all_snapshots  # fallback to all if no bot snapshots
        if not snapshots:
            print(f"No TUI snapshots found in {snapshot_dir}")
            print("Start a bot session first — it writes snapshots automatically.")
            sys.exit(1)
        snapshot_path = snapshots[0]

    if not snapshot_path.exists():
        print(f"Snapshot not found: {snapshot_path}")
        sys.exit(1)

    print(f"Watching: {snapshot_path.name}")
    dashboard = ScalpingDashboard(instance_id="VIEWER")
    dashboard._viewer_mode = True  # Disable snapshot saving in viewer
    last_mtime = 0.0
    last_window_ts = 0
    try:
        while True:
            try:
                mtime = snapshot_path.stat().st_mtime
                if mtime == last_mtime:
                    time.sleep(0.5)
                    continue
                last_mtime = mtime

                data = json.loads(snapshot_path.read_text(encoding="utf-8"))
                kwargs = data.get("render_kwargs", {})

                # Detect window change — check if snapshot is from an ended window
                window_ts = kwargs.get("window_ts", 0)
                window_min = kwargs.get("window_minutes", 15)
                window_end = window_ts + window_min * 60 if window_ts else 0
                if window_end and time.time() > window_end + 30:
                    # Window ended >30s ago — snapshot is stale, wait for new session
                    elapsed_since = time.time() - window_end
                    print(f"\r  Waiting for new session... (previous window ended {elapsed_since:.0f}s ago)", end="", flush=True)
                    time.sleep(2)
                    last_mtime = 0  # Re-check file on next iteration
                    continue

                # Track window transitions
                if window_ts and window_ts != last_window_ts:
                    if last_window_ts:
                        print(f"\n  New session detected: window {window_ts}")
                    last_window_ts = window_ts

                # Restore dashboard internal state (trades, candles, logs)
                dashboard.trades = data.get("trades", [])
                dashboard.candles = data.get("candles", [])
                dashboard.logs = deque(data.get("logs", []), maxlen=dashboard.max_logs)

                # Convert entry_range from list back to tuple
                if "entry_range" in kwargs and isinstance(kwargs["entry_range"], list):
                    kwargs["entry_range"] = tuple(kwargs["entry_range"])

                # Remove logs_only if present (always do full render)
                kwargs.pop("logs_only", None)

                dashboard.render(**kwargs)

                # Show staleness indicator
                age = time.time() - data.get("saved_at", 0)
                if age > 10:
                    print(f"\n  Warning: Snapshot is {age:.0f}s old — bot may be stopped")

            except (json.JSONDecodeError, KeyError, FileNotFoundError):
                time.sleep(1)
                continue

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nViewer stopped.")


if __name__ == "__main__":
    _run_tui_viewer()
